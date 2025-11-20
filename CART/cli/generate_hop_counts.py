"""Summarize hop-count CSV outputs across the repository."""

import os
import re
from glob import glob
from typing import Dict, List, Optional

import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))


def collect_counts() -> List[Dict]:
    results: List[Dict] = []

    pattern1 = glob(os.path.join(ROOT, '**', '*hop_chains.csv'), recursive=True)
    for path in pattern1:
        fname = os.path.basename(path)
        m = re.search(r'_(\d+)hop_chains\.csv$', fname)
        if not m:
            continue
        n = int(m.group(1))
        count = None
        try:
            df = pd.read_csv(path)
            count = len(df)
        except Exception:
            try:
                with open(path, 'r') as f:
                    count = sum(1 for _ in f) - 1
            except Exception:
                count = None
        results.append({'source_file': path, 'hop_number': n, 'count': count})

    pattern2 = glob(os.path.join(ROOT, '**', '*all_chains.csv'), recursive=True)
    for path in pattern2:
        try:
            df = pd.read_csv(path, usecols=['chain_length'])
        except Exception:
            df = None
        if df is not None:
            vc = df['chain_length'].value_counts().sort_index()
            for hop_number, cnt in vc.items():
                results.append({'source_file': path, 'hop_number': int(hop_number), 'count': int(cnt)})
        else:
            try:
                df2 = pd.read_csv(path, nrows=5)
                hop_cols = [c for c in df2.columns if re.match(r'hop\d+_ip', c)]
                max_hop = len(hop_cols) - 1
                total = sum(1 for _ in open(path)) - 1
                results.append({'source_file': path, 'hop_number': max_hop, 'count': total})
            except Exception:
                results.append({'source_file': path, 'hop_number': None, 'count': None})

    pattern3 = glob(os.path.join(ROOT, '**', '*.csv'), recursive=True)
    for path in pattern3:
        if any(path.endswith(p) for p in ('hop_chains.csv', 'all_chains.csv')):
            continue
        try:
            df = pd.read_csv(path, nrows=5)
        except Exception:
            continue
        hop_cols = [c for c in df.columns if re.match(r'hop\d+_ip', c)]
        if hop_cols:
            hops = sorted(set(int(re.findall(r'hop(\d+)_ip', c)[0]) for c in hop_cols))
            max_hop = max(hops)
            try:
                total = sum(1 for _ in open(path)) - 1
            except Exception:
                total = None
            results.append({'source_file': path, 'hop_number': max_hop, 'count': total})

    return results


def aggregate_results(results: List[Dict]) -> pd.DataFrame:
    agg = {}
    for r in results:
        hop = r['hop_number']
        cnt = r['count'] if r['count'] is not None else 0
        if hop is None:
            continue
        agg[hop] = agg.get(hop, 0) + cnt
    rows = [{'hop_number': hop, 'total_count': agg[hop]} for hop in sorted(agg)]
    return pd.DataFrame(rows)


def main(argv: Optional[List[str]] = None) -> None:
    results = collect_counts()
    out_df = aggregate_results(results)
    out_csv = os.path.join(ROOT, 'hop_counts_summary.csv')
    out_df.to_csv(out_csv, index=False)

    print('Hop counts summary (hop_number, total_count):')
    print(out_df.to_string(index=False))
    print('\nDetails per source file:')
    for r in results:
        print(f"{r.get('hop_number')!s:>3} | {r.get('count')!s:>10} | {r.get('source_file')}")
    print(f"\nSaved summary to: {out_csv}")


if __name__ == '__main__':
    main()
