#!/usr/bin/env python3
"""Fusionne tous les CSV de sortie (un par shard) en un seul fichier final."""
import argparse
import csv
import glob

parser = argparse.ArgumentParser()
parser.add_argument("--pattern", required=True, help="glob des fichiers à fusionner, ex. 'artifacts/**/phase1_out_*.csv'")
parser.add_argument("--output", required=True)
args = parser.parse_args()

files = sorted(glob.glob(args.pattern, recursive=True))
print(f"Fusion de {len(files)} fichiers")

fieldnames = ["siren", "nom", "site_web", "status", "method", "nb_urls", "urls", "error"]
seen_sirens = set()

with open(args.output, "w", newline="", encoding="utf-8") as out_f:
    writer = csv.DictWriter(out_f, fieldnames=fieldnames)
    writer.writeheader()
    for path in files:
        with open(path, newline="", encoding="utf-8") as in_f:
            for row in csv.DictReader(in_f):
                if row["siren"] in seen_sirens:
                    continue
                seen_sirens.add(row["siren"])
                writer.writerow(row)

print(f"Total lignes fusionnées : {len(seen_sirens)}")
print(f"Écrit dans {args.output}")
