"""Compute hop counts from a cached adjacency dictionary without materializing chains."""

import argparse
import csv
import os
import pickle
import time
from collections import defaultdict
from typing import Dict, List, Optional


def load_adj(cache_file: str) -> Dict[str, List[dict]]:
    with open(cache_file, 'rb') as f:
        adj = pickle.load(f)
    for edges in adj.values():
        edges.sort(key=lambda e: e.get('timestamp', 0))
    return adj


def count_chains_from_node(adj: Dict[str, List[dict]], start_src: str, max_hops: int, timeout: Optional[int] = None) -> List[int]:
    counts = [0] * (max_hops + 1)
    start_edges = adj.get(start_src, [])
    if not start_edges:
        return counts

    start_time = time.time()
    stack = []
    for edge in start_edges:
        dst = edge['dst_ip']
        ts = edge['timestamp']
        visited = {start_src, dst}
        stack.append((dst, ts, visited, 1))

    while stack:
        if timeout and (time.time() - start_time) > timeout:
            raise TimeoutError('Counting exceeded timeout')
        last_ip, last_time, visited, hops = stack.pop()
        counts[hops] += 1
        if hops >= max_hops:
            continue
        for edge in adj.get(last_ip, []):
            if edge['timestamp'] <= last_time:
                continue
            next_ip = edge['dst_ip']
            if next_ip in visited:
                continue
            new_visited = visited | {next_ip}
            stack.append((next_ip, edge['timestamp'], new_visited, hops + 1))
    return counts


def aggregate_counts(adj: Dict[str, List[dict]], max_hops: int, timeout_per_node: Optional[int] = None) -> List[int]:
    total = [0] * (max_hops + 1)
    nodes = list(adj.keys())
    for i, src in enumerate(nodes, 1):
        if i % 1000 == 0:
            print(f'Processed {i}/{len(nodes)} source nodes...')
        try:
            counts = count_chains_from_node(adj, src, max_hops, timeout=timeout_per_node)
        except TimeoutError:
            print(f'  Timeout while processing {src}; skipping remaining extensions for this node')
            continue
        for h in range(1, max_hops + 1):
            total[h] += counts[h]
    return total


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Compute hop counts from cached adjacency pickle file')
    parser.add_argument('--cache', required=True, help='Path to adjacency pickle file')
    parser.add_argument('--max_hops', type=int, default=3, help='Max hops to count')
    parser.add_argument('--output', default='hop_counts.csv', help='CSV output path')
    parser.add_argument('--timeout_per_node', type=int, default=5, help='Seconds timeout per source node')
    return parser


def main(argv: Optional[List[str]] = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if not os.path.exists(args.cache):
        raise SystemExit(f'Cache file not found: {args.cache}')

    print(f'Loading adjacency from {args.cache}...')
    adj = load_adj(args.cache)
    print(f'Adjacency loaded: {len(adj):,} source nodes')

    print(f'Counting chains up to {args.max_hops} hops (per-node timeout {args.timeout_per_node}s)')
    start = time.time()
    totals = aggregate_counts(adj, args.max_hops, timeout_per_node=args.timeout_per_node)
    elapsed = time.time() - start

    print('\nHop counts:')
    for h in range(1, args.max_hops + 1):
        print(f'  {h}-hop chains: {totals[h]:,}')

    with open(args.output, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['hop', 'count'])
        for h in range(1, args.max_hops + 1):
            writer.writerow([h, totals[h]])
    print(f'Wrote hop counts to {args.output} (elapsed {elapsed:.1f}s)')


if __name__ == '__main__':
    main()
