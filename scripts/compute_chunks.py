#!/usr/bin/env python3
"""Découpe le CSV d'entrée en tranches [start, end) pour une matrix GitHub Actions.

Sortie : JSON sur stdout, ex. {"include": [{"start": 0, "end": 400}, ...]}
"""
import argparse
import csv
import json

parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--chunk-size", type=int, default=400)
args = parser.parse_args()

with open(args.input, newline="", encoding="utf-8-sig") as f:
    total = sum(1 for _ in csv.DictReader(f))

chunks = []
start = 0
while start < total:
    end = min(start + args.chunk_size, total)
    chunks.append({"start": start, "end": end})
    start = end

print(json.dumps({"include": chunks}))
