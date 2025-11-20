#!/usr/bin/env python3
"""
Find Maximum Chain Depth in Attack Graph

This script explores the cached adjacency list to find the longest attack chain
without generating all possible chains.
"""

import sys
import os
import pickle
import time
import logging
from multiprocessing import Pool, cpu_count
from collections import Counter
from tqdm import tqdm

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def explore_max_depth(args):
    """Explore from a single node to find max depth reachable."""
    try:
        node, adj_list = args
        local_max = 0
        nodes_explored = 0
        start_time = time.time()

        def dfs(current_node, current_time, seen_nodes, depth):
            nonlocal local_max, nodes_explored
            local_max = max(local_max, depth)
            nodes_explored += 1

            # Safety limits to prevent infinite exploration
            if depth >= 30:  # Reasonable limit for attack chains
                return
            if nodes_explored > 50000:  # Prevent excessive exploration
                return
            if time.time() - start_time > 60:  # 1 minute timeout per node
                logger.warning(f"Node {node} exploration timed out")
                return

            edges = adj_list.get(current_node)
            if not edges:
                return

            for edge in edges:
                dst = edge['dst_ip']
                ts = edge['timestamp']

                # Temporal constraint: next hop must be after current
                if ts <= current_time:
                    continue

                # Cycle prevention: don't revisit nodes
                if dst in seen_nodes:
                    continue

                dfs(dst, ts, seen_nodes | {dst}, depth + 1)

        dfs(node, 0, {node}, 0)
        elapsed = time.time() - start_time
        logger.debug(f"Node {node}: max_depth={local_max}, explored={nodes_explored}, time={elapsed:.2f}s")
        return local_max, nodes_explored

    except Exception as e:
        logger.error(f"Error exploring from node {node}: {e}")
        return 0, 0

def main():
    try:
        # Load cached adjacency list
        cache_file = './.chain_cache/edges_9765c3409d1e5f8537d8e5e175f9472a.pkl'

        if not os.path.exists(cache_file):
            logger.error("Cache file not found. Run the pipeline first to generate cache.")
            return 1

        logger.info('Loading cached adjacency list...')
        with open(cache_file, 'rb') as f:
            adj_list = pickle.load(f)
        logger.info(f'Loaded adjacency list with {len(adj_list)} source nodes')

        if not adj_list:
            logger.error("Adjacency list is empty")
            return 1

        logger.info('Exploring graph to find maximum chain depth...')
        start_time = time.time()

        # Prepare arguments for parallel processing
        nodes_list = list(adj_list.keys())
        args_list = [(node, adj_list) for node in nodes_list]

        # Use multiprocessing with progress bar
        results = []
        total_nodes_explored = 0

        num_processes = min(2, len(nodes_list))  # Use fewer processes to be safer
        logger.info(f'Using {num_processes} processes for parallel exploration')

        with Pool(processes=num_processes) as pool:
            for result in tqdm(
                pool.imap_unordered(explore_max_depth, args_list),
                total=len(nodes_list),
                desc='Processing nodes',
                unit='node'
            ):
                max_depth, nodes_exp = result
                results.append(max_depth)
                total_nodes_explored += nodes_exp

        if not results:
            logger.error("No results obtained from exploration")
            return 1

        global_max = max(results)
        elapsed = time.time() - start_time

        logger.info(f'Completed in {elapsed:.1f} seconds')
        logger.info(f'Total nodes explored: {total_nodes_explored:,}')
        logger.info(f'Maximum chain depth found: {global_max} hops')

        # Show distribution
        depth_counts = Counter(results)
        logger.info('Depth distribution (chains by length):')
        for depth in sorted(depth_counts.keys()):
            count = depth_counts[depth]
            percentage = (count / len(results)) * 100
            logger.info(f'  {depth:2d} hops: {count:3d} nodes ({percentage:5.1f}%)')

        # Summary statistics
        depths = sorted(results)
        logger.info('Summary statistics:')
        logger.info(f'  Mean depth: {sum(depths)/len(depths):.1f} hops')
        logger.info(f'  Median depth: {depths[len(depths)//2]} hops')
        logger.info(f'  95th percentile: {depths[int(len(depths)*0.95)]} hops')

        return 0

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1

if __name__ == '__main__':
    sys.exit(main())