# Incremental Chain Builder - Standalone Implementation
# This is a working implementation of node-by-node chain traversal
# Much faster than join-based approach - no combinatorial explosions!

import pandas as pd
import polars as pl
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import json

def build_chains_incremental(
    csv_path: str,
    num_hops: int,
    mode: str = 'label_aware',
    max_edges_per_node: Optional[int] = None,
    log_progress: bool = True,
    progress_log_path: Optional[str] = None,
) -> Tuple[pl.DataFrame, int]:
    """
    Build n-hop chains using incremental node-by-node traversal.
    
    Args:
        csv_path: Path to CSV with src, dst, ts, is_attack columns
        num_hops: Number of hops (e.g., 3 for 3-hop chains = 4 nodes)
        mode: 'label_aware' (attack edges only) or 'label_agnostic' (all edges)
        max_edges_per_node: Limit outgoing edges per node (None = unlimited)
    
    Returns:
        (chains_df, total_count)
    """
    print(f'\n[{num_hops}-hop {mode}] Loading edges from {Path(csv_path).name}...', end=' ')
    edges_df = pd.read_csv(csv_path, usecols=['src', 'dst', 'ts', 'is_attack'])
    
    # Filter by mode
    if mode == 'label_aware':
        edges_df = edges_df[edges_df['is_attack'] == 1]
    
    print(f'{len(edges_df):,} edges loaded')
    
    # Sort and build edge index
    print(f'[{num_hops}-hop] Building edge index...', end=' ', flush=True)
    edges_df = edges_df.sort_values(['src', 'ts'])
    
    edge_index = {}
    for src, group in edges_df.groupby('src'):
        edges_list = list(zip(group['dst'].values, group['ts'].values))
        if max_edges_per_node:
            edges_list = edges_list[:max_edges_per_node]
        edge_index[src] = edges_list
    
    unique_sources = list(edge_index.keys())
    print(f'{len(unique_sources):,} source nodes')
    
    # Recursive chain exploration
    def explore(node: str, current_time: int, path: List[str], timestamps: List[int], depth: int) -> List[Tuple[List[str], List[int]]]:
        """Recursively build chains from current node."""
        if depth == num_hops:
            return [(path, timestamps)]  # Complete chain
        
        if node not in edge_index:
            return []  # Dead end
        
        # Get valid next hops
        outgoing = edge_index[node]
        valid_edges = [(dst, ts) for dst, ts in outgoing if ts > current_time and dst not in path]
        
        if not valid_edges:
            return []
        
        # Explore each continuation
        chains = []
        for next_node, next_time in valid_edges:
            sub_chains = explore(
                next_node,
                next_time,
                path + [next_node],
                timestamps + [next_time],
                depth + 1
            )
            chains.extend(sub_chains)
        
        return chains
    
    # Build chains from all starting nodes with incremental output
    print(f'[{num_hops}-hop] Exploring from {len(unique_sources):,} starting nodes...')
    
    all_chains = []
    total_count = 0
    batch_size = 50000  # Write to disk every 50K chains
    
    # Optional: Write to temporary parquet files as we go
    temp_dir = Path('thesis_results/chain_temp')
    temp_dir.mkdir(parents=True, exist_ok=True)
    batch_files = []
    
    # Optional: JSON progress log
    progress_log = []
    if log_progress and progress_log_path is None:
        progress_log_path = f'thesis_results/{mode}_{num_hops}hop_progress.json'
    
    for idx, start_node in enumerate(unique_sources):
        if idx > 0 and idx % 1000 == 0:
            print(f'  Progress: {idx:,}/{len(unique_sources):,} nodes, {total_count:,} chains found', flush=True)
            
            # Log progress to JSON
            if log_progress:
                progress_log.append({
                    'nodes_processed': idx,
                    'total_nodes': len(unique_sources),
                    'chains_found': total_count,
                    'batches_written': len(batch_files),
                    'percent_complete': round(100 * idx / len(unique_sources), 2)
                })
        
        # Build all chains from this starting node
        node_chains = explore(start_node, 0, [start_node], [0], 0)
        
        # Convert to dict format
        for path, timestamps in node_chains:
            chain_dict = {}
            for hop_idx, (node, ts) in enumerate(zip(path, timestamps), start=1):
                chain_dict[f'hop{hop_idx}_ip'] = node
                chain_dict[f't{hop_idx}'] = int(ts)
            
            # Add time deltas
            for hop_idx in range(2, len(path) + 1):
                delta = (chain_dict[f't{hop_idx}'] - chain_dict[f't{hop_idx-1}']) / 3600.0
                chain_dict[f'hours_to_hop{hop_idx}'] = delta
            
            all_chains.append(chain_dict)
            total_count += 1
        
        # Write batch to disk when buffer is full
        if len(all_chains) >= batch_size:
            batch_num = len(batch_files)
            batch_path = temp_dir / f'{mode}_{num_hops}hop_batch{batch_num}.parquet'
            batch_df = pl.from_dicts(all_chains)
            batch_df.write_parquet(batch_path)
            batch_files.append(batch_path)
            print(f'  → Wrote batch {batch_num} ({len(all_chains):,} chains) to disk')
            all_chains = []  # Clear memory
    
    # Write final batch
    if all_chains:
        batch_num = len(batch_files)
        batch_path = temp_dir / f'{mode}_{num_hops}hop_batch{batch_num}.parquet'
        batch_df = pl.from_dicts(all_chains)
        batch_df.write_parquet(batch_path)
        batch_files.append(batch_path)
        print(f'  → Wrote final batch {batch_num} ({len(all_chains):,} chains) to disk')
    
    print(f'[{num_hops}-hop] ✓ Found {total_count:,} total chains')
    
    # Save final progress log to JSON
    if log_progress and progress_log:
        with open(progress_log_path, 'w') as f:
            json.dump({
                'mode': mode,
                'num_hops': num_hops,
                'max_edges_per_node': max_edges_per_node,
                'total_nodes_processed': len(unique_sources),
                'total_chains_found': total_count,
                'batches_written': len(batch_files),
                'progress_snapshots': progress_log
            }, f, indent=2)
        print(f'[{num_hops}-hop] Progress log saved to {progress_log_path}')
    
    # Combine all batch files
    if batch_files:
        print(f'[{num_hops}-hop] Combining {len(batch_files)} batch files...', end=' ', flush=True)
        result_df = pl.concat([pl.read_parquet(f) for f in batch_files])
        
        # Clean up temp files
        for batch_file in batch_files:
            batch_file.unlink()
        print('✓')
    else:
        result_df = pl.DataFrame()
    
    return result_df, total_count


# Example usage:
if __name__ == '__main__':
    # Test with your data
    csv_path = 'thesis_results/connects_edges.csv'
    
    # Build 3-hop chains
    chains_df, count = build_chains_incremental(
        csv_path,
        num_hops=3,
        mode='label_aware',
        max_edges_per_node=50  # Or None for unlimited
    )
    
    print(f'\nResult: {len(chains_df):,} unique chains, {count:,} total')
    print(chains_df.head())
