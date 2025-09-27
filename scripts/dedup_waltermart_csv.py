#!/usr/bin/env python3
import os, sys, csv, re, shutil
from typing import Dict, Tuple, Optional, List

# Argument to accept input and output file paths
if len(sys.argv) < 3:
    print(f"Usage: {sys.argv[0]} <input_csv> <output_csv> [--sep ' | '] [--keep-no-price]")
    sys.exit(1)

# Parse simple optional flags
args = sys.argv[1:]
sep = ";"
keep_no_price = False
if '--sep' in args:
    i = args.index('--sep')
    try:
        sep = args[i+1]
        del args[i:i+2]
    except Exception:
        print("--sep flag requires a value")
        sys.exit(1)
if '--keep-no-price' in args:
    keep_no_price = True
    args.remove('--keep-no-price')

if len(args) < 2:
    print(f"Usage: {sys.argv[0]} <input_csv> <output_csv> [--sep ' | '] [--keep-no-price]")
    sys.exit(1)

INPUT_FILE = args[0]
OUTPUT_FILE = args[1]

ROOT = os.path.dirname(os.path.dirname(__file__))
CSV_PATH = os.path.join(ROOT, 'data', INPUT_FILE)
BAK_PATH = os.path.join(ROOT, 'data', OUTPUT_FILE + '.bak')

_name_ws = re.compile(r"\s+")
_punct = re.compile(r"[^a-z0-9]+")

def norm_name(s: str) -> str:
    s = (s or '').strip().lower()
    s = _name_ws.sub(' ', s)
    s = _punct.sub(' ', s)
    return _name_ws.sub(' ', s).strip()

if not os.path.exists(CSV_PATH):
    print(f"CSV not found: {CSV_PATH}")
    sys.exit(1)

with open(CSV_PATH, newline='', encoding='utf-8') as fh:
    reader = csv.DictReader(fh)
    rows = list(reader)

if not rows:
    print("No rows found; nothing to do.")
    sys.exit(0)

print(f"Loaded {len(rows)} rows from {CSV_PATH}")

# Structures per normalized name
best: Dict[str, Dict[str, str]] = {}          # canonical chosen row
all_ids: Dict[str, List[str]] = {}            # all distinct IDs seen
all_prices: Dict[str, List[float]] = {}       # price history for debugging (optional)
counts: Dict[str, int] = {}                   # number of merged rows


def to_float(x: Optional[str]) -> Optional[float]:
    if x is None or x == '':
        return None
    try:
        return float(x)
    except Exception:
        s = re.sub(r"[^0-9.]+", "", x)
        try:
            return float(s) if s else None
        except Exception:
            return None

for r in rows:
    name = r.get('name') or ''
    key = norm_name(name)
    if not key:
        continue
    price = to_float(r.get('price'))
    if price is None and not keep_no_price:
        continue
    counts[key] = counts.get(key, 0) + 1
    rid = (r.get('id') or '').strip()
    if rid:
        all_ids.setdefault(key, [])
        if rid not in all_ids[key]:
            all_ids[key].append(rid)
    if price is not None:
        all_prices.setdefault(key, []).append(price)
    cur = best.get(key)
    if cur is None:
        best[key] = r
        continue
    # Decide if replace current canonical
    cur_price = to_float(cur.get('price'))
    replace = False
    # Prefer row with price if current lacks
    if cur_price is None and price is not None:
        replace = True
    elif price is not None and cur_price is not None:
        if price < cur_price:
            replace = True
        elif price == cur_price:
            # Prefer row with URL, then longer non-empty source (heuristic stability)
            cur_url = (cur.get('url') or '').strip()
            new_url = (r.get('url') or '').strip()
            if new_url and not cur_url:
                replace = True
            elif new_url and cur_url and len(new_url) > len(cur_url):
                replace = True
    elif price is None and cur_price is None:
        # fallback: keep first
        pass
    if replace:
        best[key] = r

# Backup original
try:
    shutil.copy2(CSV_PATH, BAK_PATH)
except Exception as e:
    print(f"Warning: could not backup original: {e}")

# Determine output fieldnames
base_fields = list(rows[0].keys())
# Ensure our new columns appear (avoid duplicates)
for extra in ["merged_ids","merged_count","canonical_id","min_price","max_price"]:
    if extra not in base_fields:
        base_fields.append(extra)

with open(CSV_PATH, 'w', newline='', encoding='utf-8') as fh:
    w = csv.DictWriter(fh, fieldnames=base_fields)
    w.writeheader()
    for key, row in best.items():
        ids_list = all_ids.get(key, [])
        row_ids_join = sep.join(ids_list)
        # Canonical id: prefer canonical row id, else first aggregated id
        canonical_id = (row.get('id') or '').strip() or (ids_list[0] if ids_list else '')
        prices = all_prices.get(key, [])
        min_price = f"{min(prices):.2f}" if prices else ''
        max_price = f"{max(prices):.2f}" if prices else ''
        out_row = dict(row)  # copy
        out_row['merged_ids'] = row_ids_join
        out_row['merged_count'] = str(counts.get(key, 0))
        out_row['canonical_id'] = canonical_id
        out_row['min_price'] = min_price
        out_row['max_price'] = max_price
        w.writerow(out_row)

print(f"Wrote {len(best)} canonical rows to {CSV_PATH} (backup at {BAK_PATH})")
print("Columns added: merged_ids, merged_count, canonical_id, min_price, max_price")
print(f"Separator used: '{sep}'  keep_no_price={keep_no_price}")
