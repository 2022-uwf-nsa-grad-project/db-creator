# Incremental Chain Builder - Standalone Implementation
# This is a working implementation of node-by-node chain traversal
# Much faster than join-based approach - no combinatorial explosions!

import pandas as pd
import polars as pl
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Union, Any
import json
import argparse
import multiprocessing
from functools import partial
import math
from tqdm import tqdm
import random
import time

# Global variable for worker processes to access edge index without pickling overhead
_GLOBAL_EDGE_INDEX = None

def _explore_recursive(
    node: str,
    current_time: int,
    path_records: List[Dict[str, Optional[Union[str, int]]]],
    seen_nodes: set,
    depth: int,
    edge_index: Dict,
    target_hops: List[int],
    max_hop_depth: int
):
    """Recursively build chains from current node, yielding paths lazily."""
    extended = False
    
    if depth < max_hop_depth:
        edges = edge_index.get(node)
        if edges:
            for edge in edges:
                dst = edge['dst_ip']
                ts = edge['timestamp']

                if ts <= current_time or dst in seen_nodes:
                    continue

                new_record = {
                    'ip': dst,
                    'subnet': edge.get('dst_subnet'),
                    'time': ts,
                    'tactic': edge.get('tactic')
                }
                
                yield from _explore_recursive(
                    dst,
                    ts,
                    path_records + [new_record],
                    seen_nodes | {dst},
                    depth + 1,
                    edge_index,
                    target_hops,
                    max_hop_depth
                )
                extended = True
    
    if (depth == max_hop_depth) or (not extended):
        if depth in target_hops:
            yield (depth, path_records)

def _process_chunk(
    chunk_nodes: List[str],
    target_hops: List[int],
    max_hop_depth: int,
    mode: str,
    batch_size: int,
    output_dir: Path,
    output_format: str,
    worker_id: int
) -> Dict[str, Any]:
    """Process a chunk of start nodes in a worker process."""
    global _GLOBAL_EDGE_INDEX
    edge_index = _GLOBAL_EDGE_INDEX
    
    chain_buffers = {h: [] for h in target_hops}
    batch_files_map = {h: [] for h in target_hops}
    total_counts_map = {h: 0 for h in target_hops}
    
    batch_ext = '.parquet' if output_format == 'parquet' else '.jsonl'
    
    MAX_CHAINS_PER_NODE = 200_000
    NODE_TIMEOUT_SECONDS = 300  # 5 minutes per node

    for start_node in chunk_nodes:
        start_time = time.time()
        initial_edges = edge_index.get(start_node, [])
        start_record = {
            'ip': start_node,
            'subnet': initial_edges[0].get('src_subnet') if initial_edges else None,
            'time': None,
            'tactic': None
        }
        
        chain_iterator = _explore_recursive(
            start_node, 0, [start_record], {start_node}, 0,
            edge_index, target_hops, max_hop_depth
        )

        node_chain_count = 0
        for depth, records in chain_iterator:
            # Check limits
            node_chain_count += 1
            if node_chain_count > MAX_CHAINS_PER_NODE:
                # print(f"  [Worker {worker_id}] Node {start_node} hit chain limit ({MAX_CHAINS_PER_NODE})")
                break
            
            if (node_chain_count % 1000 == 0) and (time.time() - start_time > NODE_TIMEOUT_SECONDS):
                # print(f"  [Worker {worker_id}] Node {start_node} timed out ({NODE_TIMEOUT_SECONDS}s)")
                break

            chain_dict = {}
            prev_time = None
            for hop_idx, record in enumerate(records, start=1):
                chain_dict[f'hop{hop_idx}_ip'] = record.get('ip')
                chain_dict[f'hop{hop_idx}_subnet'] = record.get('subnet')
                if hop_idx >= 2:
                    chain_dict[f'hop{hop_idx}_time'] = record.get('time')
                    tactic_key = f'tactic{hop_idx - 1}'
                    chain_dict[tactic_key] = record.get('tactic')
                    if prev_time is not None and record.get('time') is not None:
                        delta = (record['time'] - prev_time) / 3600.0
                        chain_dict[f'hours_to_hop{hop_idx}'] = delta
                prev_time = record.get('time') if record.get('time') is not None else prev_time
            
            chain_buffers[depth].append(chain_dict)
            total_counts_map[depth] += 1
        
            if len(chain_buffers[depth]) >= batch_size:
                batch_num = len(batch_files_map[depth])
                # Include worker_id in filename to avoid collisions
                batch_path = output_dir / f'{mode}_{depth}hop_w{worker_id}_b{batch_num}{batch_ext}'
                
                if output_format == 'parquet':
                    batch_df = pl.from_dicts(chain_buffers[depth])
                    batch_df.write_parquet(batch_path)
                else:
                    with open(batch_path, 'w') as fh:
                        for record in chain_buffers[depth]:
                            fh.write(json.dumps(record))
                            fh.write('\n')
                
                batch_files_map[depth].append(str(batch_path))
                chain_buffers[depth] = []

    # Write final batches
    for depth in target_hops:
        if chain_buffers[depth]:
            batch_num = len(batch_files_map[depth])
            batch_path = output_dir / f'{mode}_{depth}hop_w{worker_id}_b{batch_num}{batch_ext}'
            
            if output_format == 'parquet':
                batch_df = pl.from_dicts(chain_buffers[depth])
                batch_df.write_parquet(batch_path)
            else:
                with open(batch_path, 'w') as fh:
                    for record in chain_buffers[depth]:
                        fh.write(json.dumps(record))
                        fh.write('\n')
            
            batch_files_map[depth].append(str(batch_path))

    return {
        'batch_files_map': batch_files_map,
        'total_counts_map': total_counts_map
    }

def _process_chunk_wrapper(args):
    return _process_chunk(*args)

def build_chains_incremental(
    csv_path: Optional[str],
    num_hops: Union[int, List[int]],
    mode: str = 'label_aware',
    max_edges_per_node: Optional[int] = None,
    log_progress: bool = True,
    progress_log_path: Optional[str] = None,
    batch_size: int = 50000,
    combine_batches: bool = True,
    combine_batches_threshold: Optional[int] = 5_000_000,
    edge_index: Optional[dict] = None,
    start_nodes: Optional[List[str]] = None,
    output_dir: Optional[Union[str, Path]] = None,
    manifest_path: Optional[Union[str, Path]] = None,
    output_format: str = 'parquet',
    workers: int = 1,
) -> Tuple[Optional[pl.DataFrame], int, Optional[Path]]:
    """
    Build n-hop chains using incremental node-by-node traversal.
    
    Args:
        csv_path: Path to CSV with src, dst, ts, is_attack columns
        num_hops: Number of hops (e.g., 3 for 3-hop chains = 4 nodes) OR list of hops
        mode: 'label_aware' (attack edges only) or 'label_agnostic' (all edges)
        max_edges_per_node: Limit outgoing edges per node (None = unlimited)
        combine_batches_threshold: Max chain count to allow in-memory concatenation
        output_format: 'parquet' (default) or 'jsonl' for newline-delimited JSON batches
    
    Returns:
        (chains_df_or_none, total_count, manifest_path_if_streaming)
    """
    canonical_format = (output_format or 'parquet').strip().lower()
    if canonical_format in {'json', 'ndjson'}:
        canonical_format = 'jsonl'
    if canonical_format not in {'parquet', 'jsonl'}:
        raise ValueError("output_format must be 'parquet' or 'jsonl'")
    output_format = canonical_format

    print(f"DEBUG: build_chains_incremental running from {__file__}")
    print(f"DEBUG: batch_size={batch_size}")
    
    # Handle multiple hops
    target_hops = [num_hops] if isinstance(num_hops, int) else sorted(num_hops)
    max_hop_depth = max(target_hops)
    print(f"DEBUG: target_hops={target_hops}, max_hop_depth={max_hop_depth}")

    # Allow caller to provide an in-memory adjacency (edge_index). If not provided,
    # build it from the CSV path.
    if edge_index is not None:
        print(f'\n[{target_hops}-hop {mode}] Using provided adjacency with {len(edge_index):,} source nodes')
        edge_index_local = edge_index
    else:
        if csv_path is None:
            raise ValueError('csv_path is required when edge_index is not provided')

        print(f'\n[{target_hops}-hop {mode}] Loading edges from {Path(csv_path).name}...', end=' ')
        edges_df = pd.read_csv(csv_path)

        # Filter by mode
        if mode == 'label_aware':
            edges_df = edges_df[edges_df['is_attack'] == 1]

        print(f'{len(edges_df):,} edges loaded')

        # Sort and build edge index
        print(f'[{target_hops}-hop] Building edge index...', end=' ', flush=True)
        edges_df = edges_df.sort_values(['src', 'ts'])

        has_src_subnet = 'src_subnet' in edges_df.columns
        has_dst_subnet = 'dst_subnet' in edges_df.columns
        has_tactic = 'tactic' in edges_df.columns

        edge_index_local = {}
        for src, group in edges_df.groupby('src'):
            edges_list = []
            for _, row in group.sort_values('ts').iterrows():
                edges_list.append({
                    'dst_ip': row['dst'],
                    'timestamp': row['ts'],
                    'src_subnet': row['src_subnet'] if has_src_subnet else None,
                    'dst_subnet': row['dst_subnet'] if has_dst_subnet else None,
                    'tactic': row['tactic'] if has_tactic else None,
                })
            if max_edges_per_node:
                edges_list = edges_list[:max_edges_per_node]
            edge_index_local[src] = edges_list

    # Determine start nodes (optionally filtered)
    if start_nodes is not None:
        unique_sources = [s for s in start_nodes if s in edge_index_local]
        print(
            f"{len(unique_sources):,} source nodes after intersecting provided start list "
            f"({len(start_nodes):,} requested)"
        )
    else:
        unique_sources = list(edge_index_local.keys())
        print(f'{len(unique_sources):,} source nodes (full adjacency)')
    
    # Recursive chain exploration
    def explore(
        node: str,
        current_time: int,
        path_records: List[Dict[str, Optional[Union[str, int]]]],
        seen_nodes: set,
        depth: int
    ):
        """Recursively build chains from current node, yielding paths lazily."""
        # OPTIMIZATION: Only yield "maximal" chains or chains at the frontier (max_hop_depth).
        # This avoids saving every intermediate prefix (e.g. A->B, A->B->C) if A->B->C->D exists.
        # We yield if:
        # 1. We reached the maximum requested depth (frontier for future extension)
        # 2. We cannot extend further (dead end / maximal chain)
        
        extended = False
        
        if depth < max_hop_depth:
            edges = edge_index_local.get(node)
            if edges:
                for edge in edges:
                    dst = edge['dst_ip']
                    ts = edge['timestamp']

                    if ts <= current_time or dst in seen_nodes:
                        continue

                    new_record = {
                        'ip': dst,
                        'subnet': edge.get('dst_subnet'),
                        'time': ts,
                        'tactic': edge.get('tactic')
                    }
                    
                    # If we successfully recurse, we have extended this chain
                    # We yield from the recursion
                    yield from explore(
                        dst,
                        ts,
                        path_records + [new_record],
                        seen_nodes | {dst},
                        depth + 1
                    )
                    extended = True
        
        # If we reached max depth OR we couldn't extend (dead end), this is a result to save.
        if (depth == max_hop_depth) or (not extended):
            if depth in target_hops:
                yield (depth, path_records)
    
    # Initialize buffers for each target hop length
    chain_buffers = {h: [] for h in target_hops}
    batch_files_map = {h: [] for h in target_hops}
    total_counts_map = {h: 0 for h in target_hops}
    
    # Optional: Write to temporary parquet files as we go
    temp_dir = Path(output_dir or 'thesis_results/chain_temp')
    temp_dir.mkdir(parents=True, exist_ok=True)
    batch_ext = '.parquet' if output_format == 'parquet' else '.jsonl'
    
    # Optional: JSON progress log
    progress_log = []
    if log_progress and progress_log_path is None:
        progress_log_path = f'thesis_results/{mode}_multihop_progress.json'
    
    # Set global edge index for workers
    global _GLOBAL_EDGE_INDEX
    _GLOBAL_EDGE_INDEX = edge_index_local
    
    if workers > 1:
        print(f"[{target_hops}-hop] Parallel execution with {workers} workers")
        
        # Shuffle sources to distribute load and use small chunks for better load balancing
        random.shuffle(unique_sources)
        chunk_size = 1
        
        chunks = [unique_sources[i:i + chunk_size] for i in range(0, len(unique_sources), chunk_size)]
        
        # Prepare arguments for each chunk
        # (chunk_nodes, target_hops, max_hop_depth, mode, batch_size, output_dir, output_format, worker_id)
        tasks = []
        for i, chunk in enumerate(chunks):
            tasks.append((
                chunk, target_hops, max_hop_depth, mode, batch_size, temp_dir, output_format, i
            ))
            
        with multiprocessing.Pool(workers) as pool:
            results = []
            for res in tqdm(
                pool.imap_unordered(_process_chunk_wrapper, tasks),
                total=len(tasks),
                desc="Processing chunks"
            ):
                results.append(res)
                
        # Aggregate results
        for res in results:
            for h in target_hops:
                batch_files_map[h].extend(res['batch_files_map'][h])
                total_counts_map[h] += res['total_counts_map'][h]
                
    else:
        # Serial execution
        print(f"[{target_hops}-hop] Serial execution")
        res = _process_chunk(
            unique_sources, target_hops, max_hop_depth, mode, batch_size, temp_dir, output_format, 0
        )
        batch_files_map = res['batch_files_map']
        total_counts_map = res['total_counts_map']

    total_all = sum(total_counts_map.values())
    print(f'[Multi-hop] ✓ Found {total_all:,} total chains across {len(target_hops)} depths')
    
    # Save final progress log to JSON
    if log_progress and progress_log:
        with open(progress_log_path, 'w') as f:
            json.dump({
                'mode': mode,
                'target_hops': target_hops,
                'max_edges_per_node': max_edges_per_node,
                'total_nodes_processed': len(unique_sources),
                'total_chains_found': total_counts_map,
                'progress_snapshots': progress_log
            }, f, indent=2)
        print(f'[Multi-hop] Progress log saved to {progress_log_path}')
    
    manifest_map = {}
    for depth in target_hops:
        if batch_files_map[depth]:
            manifest_p = temp_dir / f'{mode}_{depth}hop_batches.manifest'
            with open(manifest_p, 'w') as mf:
                for p in batch_files_map[depth]:
                    mf.write(str(p) + '\n')
            manifest_map[depth] = manifest_p
            print(f'[{depth}-hop] Batch manifest: {manifest_p}')

    # For backward compatibility, return the max depth result
    max_depth = max(target_hops)
    return None, total_counts_map[max_depth], manifest_map

def extend_chains_from_disk(
    input_manifest: Union[str, Path],
    output_dir: Union[str, Path],
    edge_index: Dict,
    current_hop: int,
    mode: str = 'label_aware',
    batch_size: int = 50000,
    output_format: str = 'parquet'
) -> Tuple[int, Path]:
    """
    Extend existing N-hop chains to (N+1)-hop chains by reading from disk.
    This avoids re-running the full DFS from scratch.
    """
    print(f"DEBUG: Extending {current_hop}-hop chains to {current_hop + 1}-hop chains...")
    
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    batch_ext = '.parquet' if output_format == 'parquet' else '.jsonl'
    
    # Read input manifest
    with open(input_manifest, 'r') as f:
        input_files = [line.strip() for line in f if line.strip()]
    
    if not input_files:
        print("  ⚠ No input files found in manifest.")
        return 0, None

    chain_buffer = []
    batch_files = []
    total_count = 0
    
    # Process each input batch
    for input_file in input_files:
        # Read batch
        if str(input_file).endswith('.parquet'):
            df = pl.read_parquet(input_file)
            chains = df.to_dicts()
        else:
            chains = []
            with open(input_file, 'r') as f:
                for line in f:
                    chains.append(json.loads(line))
        
        # Extend each chain
        for chain in chains:
            # Get tail node
            tail_ip_key = f'hop{current_hop + 1}_ip'
            tail_time_key = f'hop{current_hop + 1}_time'
            
            # If chain is shorter than expected (shouldn't happen if logic is correct), skip
            if tail_ip_key not in chain:
                continue
                
            tail_ip = chain[tail_ip_key]
            tail_time = chain.get(tail_time_key) # Might be None for hop 1 (start node) but here we are at hop >= 2
            
            # For hop 1 (start node), time is None. For hop > 1, time is set.
            # Wait, hop numbering:
            # hop1_ip (start)
            # hop2_ip (1st jump)
            # ...
            # If current_hop=1 (1-hop chain = 2 nodes), tail is hop2_ip.
            # Wait, "N-hop chain" usually means N edges, N+1 nodes.
            # My code uses: 
            # hop1_ip (start)
            # hop2_ip (end of edge 1)
            # So a "1-hop chain" has hop1_ip and hop2_ip.
            # If input is "N-hop chains", it has hop(N+1)_ip.
            
            # Let's verify the parameter `current_hop`.
            # If we are extending FROM `current_hop` TO `current_hop + 1`.
            # The input chains have `current_hop` edges.
            # So they have nodes up to `hop(current_hop + 1)_ip`.
            
            # Example: Extend 2-hop chains (A->B->C) to 3-hop (A->B->C->D).
            # Input has hop1, hop2, hop3.
            # Tail is hop3.
            
            tail_node_idx = current_hop + 1
            tail_ip = chain[f'hop{tail_node_idx}_ip']
            tail_time = chain.get(f'hop{tail_node_idx}_time')
            
            # If tail_time is None (start node), treat as 0 for comparison? 
            # No, start node (hop1) has time=None.
            # But edges have timestamps.
            # If we are extending a 0-hop chain (just a node), tail_time is None.
            # But `extend_chains_from_disk` is likely called for N >= 1.
            
            # Get neighbors
            neighbors = edge_index.get(tail_ip, [])
            
            for edge in neighbors:
                dst = edge['dst_ip']
                ts = edge['timestamp']
                
                # Temporal constraint: next hop must be after previous hop
                if tail_time is not None and ts <= tail_time:
                    continue
                
                # Cycle check: dst must not be in current chain
                # Collect all IPs in chain
                existing_ips = {chain[f'hop{i}_ip'] for i in range(1, tail_node_idx + 1)}
                if dst in existing_ips:
                    continue
                
                # Create new chain
                new_chain = chain.copy()
                next_hop_idx = tail_node_idx + 1
                new_chain[f'hop{next_hop_idx}_ip'] = dst
                new_chain[f'hop{next_hop_idx}_subnet'] = edge.get('dst_subnet')
                new_chain[f'hop{next_hop_idx}_time'] = ts
                new_chain[f'tactic{next_hop_idx - 1}'] = edge.get('tactic')
                
                if tail_time is not None:
                    delta = (ts - tail_time) / 3600.0
                    new_chain[f'hours_to_hop{next_hop_idx}'] = delta
                
                chain_buffer.append(new_chain)
                total_count += 1
                
                if len(chain_buffer) >= batch_size:
                    batch_num = len(batch_files)
                    batch_path = output_dir / f'{mode}_{current_hop + 1}hop_batch{batch_num}{batch_ext}'
                    if output_format == 'parquet':
                        pl.from_dicts(chain_buffer).write_parquet(batch_path)
                    else:
                        with open(batch_path, 'w') as fh:
                            for rec in chain_buffer:
                                fh.write(json.dumps(rec) + '\n')
                    batch_files.append(batch_path)
                    chain_buffer = []
                    print(f'  → Wrote extended batch {batch_num} ({batch_size:,} chains)')

    # Write final batch
    if chain_buffer:
        batch_num = len(batch_files)
        batch_path = output_dir / f'{mode}_{current_hop + 1}hop_batch{batch_num}{batch_ext}'
        if output_format == 'parquet':
            pl.from_dicts(chain_buffer).write_parquet(batch_path)
        else:
            with open(batch_path, 'w') as fh:
                for rec in chain_buffer:
                    fh.write(json.dumps(rec) + '\n')
        batch_files.append(batch_path)
        print(f'  → Wrote final extended batch {batch_num} ({len(chain_buffer):,} chains)')
    
    # Write manifest
    manifest_path = output_dir / f'{mode}_{current_hop + 1}hop_batches.manifest'
    with open(manifest_path, 'w') as mf:
        for p in batch_files:
            mf.write(str(p) + '\n')
            
    return total_count, manifest_path



# Example usage:
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Incremental n-hop chain builder (streaming batches)')
    parser.add_argument('--csv', '-c', default='thesis_results/connects_edges.csv', help='Path to connects CSV')
    parser.add_argument('--hops', '-k', type=int, default=3, help='Number of hops to build')
    parser.add_argument('--mode', choices=['label_aware', 'label_agnostic'], default='label_aware')
    parser.add_argument('--max-edges-per-node', type=int, default=None)
    parser.add_argument('--batch-size', type=int, default=50000)
    parser.add_argument('--no-combine', dest='combine', action='store_false', help='Do not concatenate batch files at the end (keep batches on disk)')
    parser.add_argument('--progress-log', default=None, help='Path to JSON progress log (optional)')
    parser.add_argument('--start-nodes-file', default=None, help='Optional file with start nodes (one per line) to limit processing')
    parser.add_argument('--output-dir', default='thesis_results/chain_temp', help='Directory for streaming batch files')
    parser.add_argument('--manifest-path', default=None, help='Explicit manifest path when --no-combine is set')
    parser.add_argument('--output-format', choices=['parquet', 'jsonl'], default='parquet', help='Batch serialization format (default: parquet)')
    args = parser.parse_args()

    start_nodes = None
    if args.start_nodes_file:
        with open(args.start_nodes_file, 'r') as f:
            start_nodes = [l.strip() for l in f if l.strip()]

    result_df, count, manifest = build_chains_incremental(
        csv_path=args.csv,
        num_hops=args.hops,
        mode=args.mode,
        max_edges_per_node=args.max_edges_per_node,
        batch_size=args.batch_size,
        progress_log_path=args.progress_log,
        combine_batches=args.combine,
        start_nodes=start_nodes,
        output_dir=args.output_dir,
        manifest_path=args.manifest_path,
        output_format=args.output_format
    )

    if result_df is not None:
        print(f'\nResult: {len(result_df):,} unique chains, {count:,} total')
        print(result_df.head())
    else:
        print(f'\nResult: {count:,} total chains written in batches (see {args.output_dir})')
        if manifest:
            print(f'  Manifest: {manifest}')
