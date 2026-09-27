#!/usr/bin/env python3
"""
Phase 1 — Découverte de structure des sites d'agences immobilières.

Pour chaque site :
  1. On essaie de trouver un sitemap.xml (racine + emplacements courants,
     + celui déclaré dans robots.txt). Un sitemap-index est suivi (1 niveau).
  2. Si aucun sitemap exploitable n'est trouvé, on fait un crawl BFS depuis
     la homepage, limité à --max-links pages internes.

Aucune sélection par mot-clé ici : on récupère TOUTES les URLs trouvées.
Le tri (contact / équipe / etc.) se fait en phase 2, sur ce résultat brut.

Résultat : un CSV appendé en continu (résumable — les domaines déjà
présents dans le fichier de sortie sont sautés au redémarrage).

Usage :
    python crawl_phase1.py --input data/agences.csv --output data/phase1_out.csv \
        --start 0 --end 500 --max-links 50 --concurrency 10
"""

import argparse
import asyncio
import csv
import json
import sys
import unicodedata
import xml.etree.ElementTree as ET
from urllib.parse import unquote, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

USER_AGENT = "Mozilla/5.0 (compatible; AgencyStructureBot/1.0; +https://thegreatestdev.com/bot)"
TIMEOUT = httpx.Timeout(15.0, connect=10.0)
SITEMAP_CANDIDATES = ["/sitemap.xml", "/sitemap_index.xml", "/sitemap-index.xml", "/sitemap1.xml"]
MAX_SITEMAP_URLS = 300  # garde-fou : certains sitemaps listent des milliers de fiches biens
SITE_BUDGET_SECONDS = 90  # garde-fou : un site lent ne doit pas monopoliser un slot du sémaphore
NON_HTML_EXTENSIONS = (
    ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico",
    ".zip", ".doc", ".docx", ".xls", ".xlsx", ".mp4", ".mp3", ".css", ".js",
)
# Mots-clés d'URL à faire remonter avant troncature à MAX_SITEMAP_URLS :
# un sitemap immo est souvent dominé par des milliers de fiches biens, qui
# noient les pages "structurelles" (contact, équipe, mentions légales...)
# dont la phase 2 a réellement besoin. Comparés après normalisation
# (accents retirés, tirets/underscores retirés) — voir _normalize_for_match —
# donc "à-propos", "a-propos" et "apropos" sont équivalents ici : pas besoin
# de lister toutes les variantes orthographiques séparément.
PRIORITY_URL_KEYWORDS = (
    "contact", "nouscontacter", "contacteznous", "nousecrire",
    "equipe", "team", "collaborateur", "personnel", "interlocuteur",
    "agence", "apropos", "about", "quisommesnous", "nousconnaitre",
    "notrehistoire", "presentation", "decouvrir", "conseiller",
    "notreagence", "nosagences", "coordonnees", "adresse",
    "mentionslegales", "mentionlegale", "informationslegales", "legal",
    "cgu", "cgv", "plandusite",
)


def _normalize_for_match(text: str) -> str:
    """Minuscule, décodage %XX, accents retirés, séparateurs (-_/. espace) retirés.
    Rend la comparaison de mots-clés insensible à l'orthographe/l'encodage de l'URL."""
    text = unquote(text).lower()
    text = "".join(c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c))
    return "".join(ch for ch in text if ch.isalnum())


def _sitemap_sort_key(url: str) -> int:
    """Pages structurelles d'abord (0), reste ensuite (1) ; sort() étant stable, l'ordre d'origine est conservé dans chaque groupe."""
    norm = _normalize_for_match(url)
    return 0 if any(kw in norm for kw in PRIORITY_URL_KEYWORDS) else 1


# Filet de secours : si le sitemap ne contient AUCUNE page structurelle
# reconnue, on tente une poignée de chemins usuels en direct (vérifiés par
# une requête réelle avant d'être ajoutés — jamais devinés à l'aveugle).
FALLBACK_STRUCTURAL_PATHS = (
    "/contact", "/contact/", "/contactez-nous", "/nous-contacter",
    "/a-propos", "/qui-sommes-nous", "/notre-agence", "/agence",
    "/equipe", "/notre-equipe", "/mentions-legales",
)


async def probe_fallback_structural_pages(client: httpx.AsyncClient, root_url: str) -> list[str]:
    """Vérifie l'existence réelle (statut 200) d'une poignée de chemins usuels."""
    found = []
    for path in FALLBACK_STRUCTURAL_PATHS:
        status, text = await fetch(client, f"{root_url}{path}", retries=0)
        if status == 200 and text:
            found.append(f"{root_url}{path}")
    return found


def normalize_root(url: str) -> str:
    """Ramène n'importe quelle URL à son origine schema://host, sans slash final."""
    parsed = urlparse(url if "://" in url else f"http://{url}")
    return f"{parsed.scheme}://{parsed.netloc}"


async def fetch(client: httpx.AsyncClient, url: str, retries: int = 1) -> tuple[int | None, str | None]:
    for attempt in range(retries + 1):
        try:
            resp = await client.get(url, follow_redirects=True, timeout=TIMEOUT)
            if resp.status_code == 200 and resp.text:
                return resp.status_code, resp.text
            return resp.status_code, None
        except (httpx.HTTPError, httpx.InvalidURL):
            if attempt == retries:
                return None, None
            await asyncio.sleep(1)  # un seul retry, après une courte pause


def parse_sitemap_xml(xml_text: str) -> tuple[list[str], list[str]]:
    """Retourne (urls_de_pages, sous_sitemaps) trouvés dans un fichier sitemap."""
    urls, sub_sitemaps = [], []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return urls, sub_sitemaps

    tag = root.tag.lower()
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    if tag.endswith("sitemapindex"):
        for sm in root.findall("sm:sitemap/sm:loc", ns) or root.findall(".//{*}sitemap/{*}loc"):
            if sm.text:
                sub_sitemaps.append(sm.text.strip())
    else:  # urlset
        for loc in root.findall("sm:url/sm:loc", ns) or root.findall(".//{*}url/{*}loc"):
            if loc.text:
                urls.append(loc.text.strip())
    return urls, sub_sitemaps


async def find_sitemap_locations(client: httpx.AsyncClient, root_url: str) -> list[str]:
    """Cherche des sitemaps déclarés dans robots.txt, sinon retourne les emplacements courants."""
    status, text = await fetch(client, f"{root_url}/robots.txt")
    found = []
    if text:
        for line in text.splitlines():
            if line.strip().lower().startswith("sitemap:"):
                found.append(line.split(":", 1)[1].strip())
    return found or [f"{root_url}{path}" for path in SITEMAP_CANDIDATES]


async def try_sitemap(client: httpx.AsyncClient, root_url: str) -> list[str] | None:
    candidates = await find_sitemap_locations(client, root_url)
    all_urls: list[str] = []
    for sm_url in candidates:
        status, text = await fetch(client, sm_url)
        if not text:
            continue
        urls, sub_sitemaps = parse_sitemap_xml(text)
        all_urls.extend(urls)
        # on suit un seul niveau de sous-sitemaps pour rester raisonnable
        for sub in sub_sitemaps[:10]:
            _, sub_text = await fetch(client, sub)
            if sub_text:
                sub_urls, _ = parse_sitemap_xml(sub_text)
                all_urls.extend(sub_urls)
            if len(all_urls) >= MAX_SITEMAP_URLS:
                break
        if all_urls:
            break  # un sitemap exploitable trouvé, pas besoin de tester les autres emplacements
    if not all_urls:
        return None
    # dédoublonnage en gardant l'ordre
    seen = set()
    deduped = []
    for u in all_urls:
        if u not in seen:
            seen.add(u)
            deduped.append(u)
    # tri stable : pages structurelles (contact, équipe, mentions légales...)
    # en tête, pour ne pas les perdre si le sitemap est dominé par des fiches
    # biens et que la troncature à MAX_SITEMAP_URLS coupe avant de les atteindre
    deduped.sort(key=_sitemap_sort_key)

    # si le sitemap ne référence aucune page structurelle (cas fréquent :
    # CMS qui ne génère le sitemap que pour les fiches biens), on tente un
    # filet de secours en sondant directement quelques chemins usuels
    has_priority = deduped and _sitemap_sort_key(deduped[0]) == 0
    if not has_priority:
        fallback = await probe_fallback_structural_pages(client, root_url)
        existing = set(deduped)
        deduped = [u for u in fallback if u not in existing] + deduped

    return deduped[:MAX_SITEMAP_URLS]


async def crawl_bfs(client: httpx.AsyncClient, root_url: str, max_links: int, max_depth: int = 3) -> list[str]:
    """Crawl BFS same-domain depuis la homepage, sans filtre de mot-clé."""
    host = urlparse(root_url).netloc
    visited: set[str] = set()
    queue: list[tuple[str, int]] = [(root_url, 0)]
    collected: list[str] = []

    while queue and len(collected) < max_links:
        url, depth = queue.pop(0)
        if url in visited or depth > max_depth:
            continue
        visited.add(url)

        status, html = await fetch(client, url)
        if not html:
            continue
        collected.append(url)

        if depth >= max_depth or len(collected) >= max_links:
            continue

        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith(("mailto:", "tel:", "javascript:", "#")):
                continue
            absolute = urljoin(url, href)
            parsed_abs = urlparse(absolute)
            absolute = parsed_abs._replace(fragment="").geturl()  # retire #ancre avant dédup
            if parsed_abs.path.lower().endswith(NON_HTML_EXTENSIONS):
                continue
            if parsed_abs.netloc == host and absolute not in visited:
                queue.append((absolute, depth + 1))

    return collected


async def _process_site_inner(client: httpx.AsyncClient, root_url: str, max_links: int) -> dict:
    # le site répond-il au moins ?
    status, _ = await fetch(client, root_url)
    if status is None:
        return {"status": "unreachable", "method": None, "nb_urls": 0, "urls": []}

    sitemap_urls = await try_sitemap(client, root_url)
    if sitemap_urls:
        return {"status": "ok", "method": "sitemap", "nb_urls": len(sitemap_urls), "urls": sitemap_urls}

    crawled = await crawl_bfs(client, root_url, max_links)
    if crawled:
        return {"status": "ok", "method": "crawl", "nb_urls": len(crawled), "urls": crawled}

    return {"status": "empty", "method": None, "nb_urls": 0, "urls": []}


async def process_site(client: httpx.AsyncClient, site_web: str, max_links: int) -> dict:
    root_url = normalize_root(site_web)
    try:
        return await asyncio.wait_for(
            _process_site_inner(client, root_url, max_links), timeout=SITE_BUDGET_SECONDS
        )
    except asyncio.TimeoutError:
        return {"status": "timeout", "method": None, "nb_urls": 0, "urls": [],
                "error": f"budget de {SITE_BUDGET_SECONDS}s dépassé"}
    except Exception as e:  # garde-fou : un site cassé ne doit jamais arrêter le run
        return {"status": "error", "method": None, "nb_urls": 0, "urls": [], "error": str(e)[:200]}


async def worker(sem: asyncio.Semaphore, client: httpx.AsyncClient, row: dict, max_links: int, writer, lock: asyncio.Lock, out_f):
    async with sem:
        result = await process_site(client, row["site_web"], max_links)
        async with lock:
            writer.writerow({
                "siren": row["siren"],
                "nom": row["nom"],
                "site_web": row["site_web"],
                "status": result["status"],
                "method": result.get("method") or "",
                "nb_urls": result["nb_urls"],
                "urls": json.dumps(result["urls"], ensure_ascii=False),
                "error": result.get("error", ""),
            })
            out_f.flush()
            print(f"[{result['status']:>10}] {row['nom'][:40]:<40} -> {result['nb_urls']} pages ({result.get('method') or '-'})")


def load_already_done(output_path: str) -> set[str]:
    done = set()
    try:
        with open(output_path, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                done.add(r["siren"])
    except FileNotFoundError:
        pass
    return done


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--end", type=int, default=None, help="exclusif")
    parser.add_argument("--max-links", type=int, default=50)
    parser.add_argument("--concurrency", type=int, default=10)
    args = parser.parse_args()

    with open(args.input, newline="", encoding="utf-8-sig") as f:
        all_rows = list(csv.DictReader(f))

    end = args.end if args.end is not None else len(all_rows)
    batch = all_rows[args.start:end]

    already_done = load_already_done(args.output)
    todo = [r for r in batch if r["siren"] not in already_done]
    print(f"Batch [{args.start}:{end}] — {len(batch)} sites, {len(todo)} restants à traiter "
          f"({len(batch) - len(todo)} déjà faits)", file=sys.stderr)

    if not todo:
        return

    file_exists = len(already_done) > 0
    out_f = open(args.output, "a", newline="", encoding="utf-8")
    fieldnames = ["siren", "nom", "site_web", "status", "method", "nb_urls", "urls", "error"]
    writer = csv.DictWriter(out_f, fieldnames=fieldnames)
    if not file_exists:
        writer.writeheader()

    lock = asyncio.Lock()
    sem = asyncio.Semaphore(args.concurrency)
    headers = {"User-Agent": USER_AGENT}

    try:
        async with httpx.AsyncClient(headers=headers) as client:
            tasks = [worker(sem, client, row, args.max_links, writer, lock, out_f) for row in todo]
            await asyncio.gather(*tasks)
    finally:
        out_f.close()


if __name__ == "__main__":
    asyncio.run(main())
