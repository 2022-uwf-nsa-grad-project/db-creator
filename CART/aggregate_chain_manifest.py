import json
from pathlib import Path
from collections import Counter

try:
    import polars as pl
except ImportError:
    pl = None


def aggregate_manifest(manifest_path: str, output_summary: str):
    """Read a manifest listing parquet or JSONL batches and produce a JSON summary without concatenating.

    Summary includes:
    - total_chains
    - hop_count_distribution (edges)
    - top_tactics (if tactic-like columns found)
    - per-batch stats
    """
    manifest = Path(manifest_path)
    if not manifest.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")

    if pl is None:
        raise RuntimeError('polars is required for aggregation: pip install polars')

    total_chains = 0
    hop_counter = Counter()
    tactic_counter = Counter()
    batch_stats = []

    # Determine column name patterns on first file
    with open(manifest, 'r') as f:
        lines = [l.strip() for l in f if l.strip()]

    if not lines:
        raise RuntimeError(f"Manifest {manifest_path} is empty; nothing to aggregate")

    first_suffix = Path(lines[0]).suffix.lower()
    json_suffixes = {'.jsonl', '.ndjson', '.json'}
    if first_suffix == '.parquet':
        reader = lambda path: pl.read_parquet(path)
    elif first_suffix in json_suffixes:
        reader = lambda path: pl.read_ndjson(path)
    else:
        raise RuntimeError(f"Unsupported batch format for manifest {manifest_path}: {first_suffix}")

    for p in lines:
        ppath = Path(p)
        if not ppath.exists():
            print(f"  ⚠ Batch file missing: {p}")
            continue

        df = reader(p)
        n_rows = df.shape[0]
        total_chains += n_rows

        # Find hop_ip columns like hop1_ip, hop2_ip, ...
        hop_cols = [c for c in df.columns if c.startswith('hop') and c.endswith('_ip')]
        # Sort by hop index
        hop_cols_sorted = sorted(hop_cols, key=lambda x: int(x.replace('hop', '').replace('_ip', '')))

        # Determine per-row depth by checking last non-null hop_ip
        depths = []
        if hop_cols_sorted:
            for row in df.iter_rows(named=True):
                last = 0
                for i, col in enumerate(hop_cols_sorted, start=1):
                    if row.get(col) is not None:
                        last = i
                # number of nodes = last, number of hops = max(last - 1, 0)
                hops = max(last - 1, 0)
                depths.append(hops)
                hop_counter[hops] += 1
        else:
            # Fallback: if no hop_ip columns, count rows as 1-hop
            hop_counter[1] += n_rows

        # Tactic-like columns: names containing 'tactic' or single-letter t + digit like t1
        tactic_cols = [c for c in df.columns if 'tactic' in c or (len(c) <= 3 and c.startswith('t') and c[1:].isdigit())]
        if tactic_cols:
            for col in tactic_cols:
                vals = df.select(col).drop_nulls().to_series().to_list()
                tactic_counter.update(vals)

        batch_stats.append({
            'path': str(ppath),
            'rows': n_rows,
            'hops_sampled': dict(Counter(depths).most_common(5)) if depths else {},
        })

    summary = {
        'total_chains': total_chains,
        'hop_count_distribution': dict(hop_counter),
        'top_tactics': tactic_counter.most_common(20),
        'batches': batch_stats,
    }

    with open(output_summary, 'w') as out_f:
        json.dump(summary, out_f, indent=2)

    print(f'Aggregate summary written to {output_summary}')
    return summary
