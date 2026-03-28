#!/usr/bin/env python3
from __future__ import annotations

import csv
import sys
from pathlib import Path

import pandas as pd

master_path = Path('outputs/patent_master_rar_full.csv')
fee_path = Path('outputs/patent_fee_inference_ftp_full.csv')

fee_status = {}
fee_total = 0
fee_explicit = 0
fee_exclude = 0
with fee_path.open(newline='', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for row in reader:
        fee_total += 1
        iid = (row.get('input_id') or '').strip()
        fee_status[iid] = row.get('inferred_fee_status', '')
        if row.get('inferred_fee_status') == 'likely_stopped_payment_due_to_fee_nonpayment':
            fee_explicit += 1
        if row.get('panel_exclusion_recommendation') == 'exclude':
            fee_exclude += 1

usecols = ['input_id', 'city_name', 'year']
chunksize = 1_000_000
master_total = 0
master_matched = 0
master_explicit = 0
master_exclude = 0
city_counts = {}
year_counts = {}

for i, chunk in enumerate(pd.read_csv(master_path, usecols=usecols, chunksize=chunksize, dtype={'input_id': 'string', 'city_name': 'string', 'year': 'string'}), start=1):
    ids = chunk['input_id'].fillna('').str.strip()
    matched = ids.isin(fee_status)
    mcount = int(matched.sum())
    master_total += len(chunk)
    master_matched += mcount
    master_explicit += int(ids[matched].map(lambda x: fee_status.get(x) == 'likely_stopped_payment_due_to_fee_nonpayment').sum())
    master_exclude += int(ids[matched].map(lambda x: fee_status.get(x) in {'likely_stopped_payment_due_to_fee_nonpayment', 'ambiguous'}).sum())
    for city, cnt in chunk.loc[matched, 'city_name'].fillna('').value_counts().items():
        city_counts[city] = city_counts.get(city, 0) + int(cnt)
    for year, cnt in chunk.loc[matched, 'year'].fillna('').value_counts().items():
        year_counts[year] = year_counts.get(year, 0) + int(cnt)
    print(f'chunk {i}: rows={len(chunk)} matched={mcount} total_matched={master_matched}', flush=True)

master_unmatched = master_total - master_matched
print('fee_total', fee_total)
print('fee_explicit', fee_explicit)
print('fee_exclude', fee_exclude)
print('master_total', master_total)
print('master_matched', master_matched)
print('master_explicit', master_explicit)
print('master_exclude', master_exclude)
print('master_unmatched', master_unmatched)
print('top_cities', sorted(city_counts.items(), key=lambda kv: kv[1], reverse=True)[:10])
print('top_years', sorted(year_counts.items(), key=lambda kv: kv[1], reverse=True)[:10])
