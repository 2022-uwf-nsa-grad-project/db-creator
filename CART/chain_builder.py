"""
Chain Builder - Build temporal attack chains from CSV data

This module provides a class-based interface for building multi-hop
temporal chains from Zeek network telemetry data without requiring Neo4j.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from tqdm.auto import tqdm
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import json
from typing import List, Dict, Optional, Tuple


class ChainBuilder:
    """
    Build temporal attack chains from CSV network telemetry data.
    
    This class loads network connection data, builds a graph dictionary
    for efficient lookups, and constructs multi-hop temporal chains using
    parallel processing.
    """
    
    def __init__(
        self,
        csv_path: str = 'zeek_import_data.csv',
        use_labels: bool = True,
        limit_nodes: Optional[int] = None,
        output_dir: str = 'chain_output',
        use_parallel: bool = True,
        n_workers: Optional[int] = None,
        batch_size: int = 10,
        max_total_chains: int = 100_000,
        max_extensions_per_chain: int = 10
    ):
        """
        Initialize ChainBuilder.
        
        Args:
            csv_path: Path to CSV file with network connection data
            use_labels: If True, filter to attack edges only (label_binary=True)
            limit_nodes: If set, limit to top N source nodes by degree (None = all nodes)
            output_dir: Directory for output files
            use_parallel: Enable parallel processing
            n_workers: Number of worker processes (None = auto-detect)
            batch_size: Number of nodes per batch for parallel processing
            max_total_chains: Max chains per batch to prevent memory exhaustion
            max_extensions_per_chain: Max branching factor per chain
        """
        self.csv_path = csv_path
        self.use_labels = use_labels
        self.limit_nodes = limit_nodes
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Parallel processing config
        self.use_parallel = use_parallel
        self.n_workers = n_workers or min(8, multiprocessing.cpu_count())
        self.batch_size = batch_size
        
        # Safety limits
        self.max_total_chains = max_total_chains
        self.max_extensions_per_chain = max_extensions_per_chain
        
        # Data storage
        self.df = None
        self.edges = None
        self.graph_dict = None
        self.top_sources = None
        self.all_chains = None
        
    def load_data(self) -> pd.DataFrame:
        """
        Load edges from CSV file.
        
        Returns:
            DataFrame with edges
        """
        print(f"Loading edges from {self.csv_path}...")
        self.df = pd.read_csv(self.csv_path)
        
        # Filter to attack edges if requested
        if self.use_labels:
            self.df = self.df[self.df['label_binary'] == True].copy()
            print(f"✓ Filtered to {len(self.df):,} attack edges")
        else:
            print(f"✓ Loaded {len(self.df):,} total edges")
        
        # Compute subnets (assuming /24)
        def ip_to_subnet(ip):
            parts = ip.split('.')
            return f"{parts[0]}.{parts[1]}.{parts[2]}.0/24"
        
        self.df['src_subnet'] = self.df['src_ip_zeek'].apply(ip_to_subnet)
        self.df['dst_subnet'] = self.df['dest_ip_zeek'].apply(ip_to_subnet)
        
        # Rename columns to match expected format
        self.df = self.df.rename(columns={
            'src_ip_zeek': 'src_ip',
            'dest_ip_zeek': 'dst_ip',
            'ts': 'timestamp',
            'label_tactic': 'tactic'
        })
        
        # Sort by timestamp for temporal ordering
        self.df = self.df.sort_values('timestamp')
        
        print(f"✓ Prepared {len(self.df):,} edges with subnet metadata")
        return self.df
    
    def select_top_sources(self) -> List[str]:
        """
        Select top N source nodes by connection count.
        
        Returns:
            List of source IP addresses
        """
        if self.df is None:
            raise ValueError("Must call load_data() first")
        
        src_counts = self.df['src_ip'].value_counts()
        
        if self.limit_nodes:
            print(f"Selecting top {self.limit_nodes} source nodes by connection count...")
            self.top_sources = src_counts.head(self.limit_nodes).index.tolist()
            print(f"✓ Selected {len(self.top_sources)} source nodes")
            print(f"  Top source: {self.top_sources[0]} with {src_counts.iloc[0]:,} connections")
        else:
            print(f"Using all {len(src_counts)} source nodes...")
            self.top_sources = src_counts.index.tolist()
            print(f"✓ Selected all {len(self.top_sources)} source nodes")
        
        return self.top_sources
    
    def prepare_edges(self) -> List[Dict]:
        """
        Prepare edges subset for selected source nodes.
        
        Returns:
            List of edge dictionaries
        """
        if self.top_sources is None:
            raise ValueError("Must call select_top_sources() first")
        
        # Filter edges to only those FROM top sources
        edges_subset = self.df[self.df['src_ip'].isin(self.top_sources)].copy()
        
        # Convert to list of dicts for graph building
        self.edges = edges_subset[['src_ip', 'src_subnet', 'dst_ip', 'dst_subnet', 'timestamp', 'label_binary', 'tactic']].to_dict('records')
        
        # Rename label_binary to is_attack
        for edge in self.edges:
            edge['is_attack'] = 1 if edge['label_binary'] else 0
            del edge['label_binary']
            if pd.isna(edge['tactic']):
                edge['tactic'] = ''
        
        total_edges = len(self.edges)
        total_nodes = len(set(e['src_ip'] for e in self.edges) | set(e['dst_ip'] for e in self.edges))
        print(f"✓ Subset contains {total_edges:,} edges involving {total_nodes:,} unique nodes")
        
        return self.edges
    
    def build_graph_dict(self) -> Dict:
        """
        Build graph dictionary: {src_ip: {dst_ip: [edge_data_list]}}
        
        Returns:
            Graph dictionary
        """
        if self.edges is None:
            raise ValueError("Must call prepare_edges() first")
        
        print("Building graph dictionary...")
        self.graph_dict = {}
        
        for edge in tqdm(self.edges, desc="Processing edges"):
            src = edge['src_ip']
            dst = edge['dst_ip']
            
            if src not in self.graph_dict:
                self.graph_dict[src] = {}
            if dst not in self.graph_dict[src]:
                self.graph_dict[src][dst] = []
            
            # Store connection details
            self.graph_dict[src][dst].append({
                'timestamp': edge['timestamp'],
                'src_subnet': edge['src_subnet'],
                'dst_subnet': edge['dst_subnet'],
                'is_attack': edge['is_attack'],
                'tactic': edge['tactic']
            })
        
        # Sort connections by timestamp for efficient temporal filtering
        print("Sorting connections by timestamp...")
        for src in self.graph_dict:
            for dst in self.graph_dict[src]:
                self.graph_dict[src][dst].sort(key=lambda x: x['timestamp'])
        
        # Save for inspection
        graph_file = self.output_dir / 'graph_dict.json'
        print(f"Saving graph dictionary to {graph_file}...")
        with open(graph_file, 'w') as f:
            # Convert to serializable format
            serializable = {
                src: {
                    dst: [
                        {
                            'timestamp': conn['timestamp'],
                            'src_subnet': conn['src_subnet'],
                            'dst_subnet': conn['dst_subnet'],
                            'is_attack': conn['is_attack'],
                            'tactic': conn['tactic']
                        }
                        for conn in connections
                    ]
                    for dst, connections in dsts.items()
                }
                for src, dsts in self.graph_dict.items()
            }
            json.dump(serializable, f, indent=2)
        
        print(f"✓ Graph dictionary built: {len(self.graph_dict):,} source nodes")
        return self.graph_dict
    
    @staticmethod
    def _build_chains_for_nodes(node_batch, graph_dict, max_extensions=10, max_total=100_000):
        """
        Worker function: Build all possible temporal chains for a batch of source nodes.
        
        Args:
            node_batch: List of source IPs to build chains from
            graph_dict: {src_ip: {dst_ip: [connection_dicts]}}
            max_extensions: Max branches per chain to prevent explosion
            max_total: Max total chains per batch to prevent memory exhaustion
        
        Returns:
            List of chains where each chain is a flat list:
            [ip0, subnet0, ip1, subnet1, time1, tactic1, ip2, subnet2, time2, tactic2, ...]
        """
        all_chains = []
        
        # Initialize 1-hop chains for nodes in this batch
        for src_ip in node_batch:
            if src_ip not in graph_dict:
                continue
            
            for dst_ip, connections in graph_dict[src_ip].items():
                for conn in connections:
                    # Chain format: [ip0, subnet0, ip1, subnet1, time1, tactic1, ...]
                    chain = [
                        src_ip,
                        conn['src_subnet'],
                        dst_ip,
                        conn['dst_subnet'],
                        conn['timestamp'],
                        conn['tactic']
                    ]
                    all_chains.append(chain)
        
        # Keep extending chains until no more valid extensions
        iteration = 1
        while True:
            if len(all_chains) >= max_total:
                break
                
            next_chains = []
            chains_extended = 0
            
            for chain in all_chains:
                # Extract last hop info
                n_hops = (len(chain) - 2) // 4
                last_ip_idx = 2 + (n_hops - 1) * 4
                last_ip = chain[last_ip_idx]
                last_time_idx = last_ip_idx + 2
                last_time = chain[last_time_idx]
                
                # Check if this IP has outgoing connections
                if last_ip not in graph_dict:
                    next_chains.append(chain)
                    continue
                
                # Find valid extensions
                extensions_found = 0
                had_valid_extension = False
                
                for next_ip, connections in graph_dict[last_ip].items():
                    # Cycle prevention: check if next_ip already appears in chain
                    ip_positions = [0] + [2 + 4*k for k in range(n_hops)]
                    if next_ip in [chain[pos] for pos in ip_positions]:
                        continue
                    
                    # Find connections after last_time
                    valid_conns = [c for c in connections if c['timestamp'] > last_time]
                    
                    if not valid_conns:
                        continue
                    
                    # Limit branching
                    conns_to_use = valid_conns[:max_extensions]
                    
                    for conn in conns_to_use:
                        new_chain = chain.copy()
                        new_chain.extend([
                            next_ip,
                            conn['dst_subnet'],
                            conn['timestamp'],
                            conn['tactic']
                        ])
                        next_chains.append(new_chain)
                        extensions_found += 1
                        had_valid_extension = True
                        
                        if extensions_found >= max_extensions:
                            break
                    
                    if extensions_found >= max_extensions:
                        break
                
                if had_valid_extension:
                    chains_extended += 1
                else:
                    next_chains.append(chain)
                
                if len(next_chains) >= max_total:
                    break
            
            if chains_extended == 0:
                break
            
            all_chains = next_chains
            iteration += 1
        
        return all_chains
    
    def build_chains(self) -> List[List]:
        """
        Build chains (parallel or sequential).
        
        Returns:
            List of all chains
        """
        if self.graph_dict is None:
            raise ValueError("Must call build_graph_dict() first")
        
        print(f"\nBuilding chains for {len(self.top_sources)} source nodes...")
        
        if self.use_parallel:
            # Split nodes into batches
            node_batches = [self.top_sources[i:i + self.batch_size] 
                          for i in range(0, len(self.top_sources), self.batch_size)]
            print(f"Processing {len(node_batches)} batches in parallel with {self.n_workers} workers...")
            
            self.all_chains = []
            
            with ProcessPoolExecutor(max_workers=self.n_workers) as executor:
                # Submit all batches
                future_to_batch = {
                    executor.submit(
                        self._build_chains_for_nodes,
                        batch,
                        self.graph_dict,
                        self.max_extensions_per_chain,
                        self.max_total_chains
                    ): i for i, batch in enumerate(node_batches)
                }
                
                # Collect results with progress bar
                with tqdm(total=len(node_batches), desc="Processing batches") as pbar:
                    for future in as_completed(future_to_batch):
                        batch_idx = future_to_batch[future]
                        try:
                            batch_chains = future.result()
                            self.all_chains.extend(batch_chains)
                            pbar.set_postfix({"total_chains": len(self.all_chains)})
                        except Exception as e:
                            print(f"\n⚠ Batch {batch_idx} failed: {e}")
                        pbar.update(1)
            
            print(f"✓ Parallel processing complete: {len(self.all_chains):,} total chains")
        else:
            # Sequential processing
            print("Building chains sequentially...")
            self.all_chains = self._build_chains_for_nodes(
                self.top_sources,
                self.graph_dict,
                self.max_extensions_per_chain,
                self.max_total_chains
            )
            print(f"✓ Sequential processing complete: {len(self.all_chains):,} total chains")
        
        return self.all_chains
    
    def analyze_and_save(self) -> pd.DataFrame:
        """
        Save all chains to a single CSV file with chain length metadata.
        
        Returns:
            DataFrame with all chains
        """
        if self.all_chains is None:
            raise ValueError("Must call build_chains() first")
        
        print("\nAnalyzing chains...")
        
        # Determine max chain length
        max_hops = max((len(chain) - 2) // 4 for chain in self.all_chains)
        print(f"  Maximum chain depth: {max_hops} hops")
        
        # Count chains by depth
        depth_counts = defaultdict(int)
        for chain in self.all_chains:
            n_hops = (len(chain) - 2) // 4
            depth_counts[n_hops] += 1
        
        print(f"\nChain depth distribution:")
        for depth in sorted(depth_counts.keys()):
            print(f"  {depth}-hop: {depth_counts[depth]:,} chains")
        
        # Build column names for maximum depth
        columns = ['chain_length', 'hop0_ip', 'hop0_subnet']
        for i in range(1, max_hops + 1):
            columns.extend([f'hop{i}_ip', f'hop{i}_subnet', f'hop{i}_time', f'hop{i}_tactic'])
        
        # Convert all chains to rows (pad shorter chains with None)
        print("\nConverting to DataFrame...")
        rows = []
        for chain in tqdm(self.all_chains, desc="Processing chains"):
            n_hops = (len(chain) - 2) // 4
            row = [n_hops] + chain  # Add chain length as first column
            
            # Pad with None if shorter than max_hops
            expected_length = 2 + max_hops * 4  # hop0_ip, hop0_subnet, then 4 fields per hop
            while len(row) < len(columns):
                row.append(None)
            
            rows.append(row)
        
        df = pd.DataFrame(rows, columns=columns)
        
        # Save complete chains
        output_file = self.output_dir / 'all_chains.csv'
        print(f"\nSaving all chains to {output_file}...")
        df.to_csv(output_file, index=False)
        print(f"✓ Saved {len(df):,} chains (depths {min(depth_counts.keys())}-{max(depth_counts.keys())} hops)")
        
        # Save summary statistics
        summary_file = self.output_dir / 'chain_summary.json'
        summary = {
            'total_chains': len(self.all_chains),
            'max_depth': max_hops,
            'depth_distribution': dict(depth_counts),
            'source_nodes': len(self.top_sources),
            'use_labels': self.use_labels
        }
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"✓ Saved summary to {summary_file}")
        
        print(f"\n✓ All results saved to {self.output_dir}/")
        return df
    
    def print_summary(self, df: pd.DataFrame):
        """
        Print summary statistics for chains.
        
        Args:
            df: DataFrame with all chains
        """
        print("\n" + "="*60)
        print("SUMMARY STATISTICS")
        print("="*60)
        
        # Overall stats
        print(f"\nTotal chains: {len(df):,}")
        print(f"Chain depth range: {df['chain_length'].min()}-{df['chain_length'].max()} hops")
        
        # Depth distribution
        print(f"\nChain depth distribution:")
        depth_dist = df['chain_length'].value_counts().sort_index()
        for depth, count in depth_dist.items():
            pct = 100 * count / len(df)
            print(f"  {depth}-hop: {count:,} ({pct:.1f}%)")
        
        # Unique IPs per hop
        max_depth = df['chain_length'].max()
        print(f"\nUnique IPs per hop position:")
        for hop in range(max_depth + 1):
            ip_col = f'hop{hop}_ip'
            if ip_col in df.columns:
                unique_count = df[ip_col].nunique()
                print(f"  Hop {hop}: {unique_count:,} unique IPs")
        
        # Analyze tactics across all chains
        print(f"\nTop tactics (all hops):")
        all_tactics = []
        for col in df.columns:
            if col.endswith('_tactic'):
                all_tactics.extend(df[col].dropna().tolist())
        
        if all_tactics:
            tactic_counts = pd.Series(all_tactics).value_counts()
            for tactic, count in tactic_counts.head(10).items():
                if tactic:
                    pct = 100 * count / len(all_tactics)
                    print(f"    {tactic}: {count:,} ({pct:.1f}%)")
        
        print("\n" + "="*60)
    
    def compute_timing_statistics(self, df: pd.DataFrame) -> Dict:
        """
        Compute comprehensive timing statistics for adversary dwell time analysis.
        
        Args:
            df: DataFrame with all chains
            
        Returns:
            Dictionary with timing statistics per hop transition
        """
        print("\n" + "="*60)
        print("COMPUTING TIMING STATISTICS")
        print("="*60)
        
        hop_time_cols = [col for col in df.columns if col.endswith('_time')]
        timing_stats = {}
        all_times = []
        
        for i in range(1, len(hop_time_cols)):
            prev_col = hop_time_cols[i-1]
            curr_col = hop_time_cols[i]
            time_diff = (df[curr_col] - df[prev_col]) / 3600.0  # Convert to hours
            valid_diffs = time_diff.dropna()
            
            if len(valid_diffs) > 0:
                all_times.extend(valid_diffs.tolist())
                stats = {
                    'mean_hours': float(valid_diffs.mean()),
                    'median_hours': float(valid_diffs.median()),
                    'std_hours': float(valid_diffs.std()),
                    'min_hours': float(valid_diffs.min()),
                    'max_hours': float(valid_diffs.max()),
                    'q25_hours': float(valid_diffs.quantile(0.25)),
                    'q75_hours': float(valid_diffs.quantile(0.75)),
                    'count': int(len(valid_diffs))
                }
                timing_stats[f'hop_{i-1}_to_{i}'] = stats
                
                print(f"\nHop {i-1} → {i} timing:")
                print(f"  Count: {stats['count']:,} transitions")
                print(f"  Mean: {stats['mean_hours']:.2f} hours")
                print(f"  Median: {stats['median_hours']:.2f} hours")
                print(f"  Std Dev: {stats['std_hours']:.2f} hours")
                print(f"  Range: [{stats['min_hours']:.2f}, {stats['max_hours']:.2f}] hours")
                print(f"  IQR: [{stats['q25_hours']:.2f}, {stats['q75_hours']:.2f}] hours")
        
        # Overall multi-hop cadence
        if all_times:
            overall_stats = {
                'mean_hours': float(np.mean(all_times)),
                'median_hours': float(np.median(all_times)),
                'std_hours': float(np.std(all_times)),
                'total_transitions': len(all_times)
            }
            timing_stats['overall'] = overall_stats
            
            print(f"\n{'='*60}")
            print("OVERALL MULTI-HOP CADENCE")
            print(f"{'='*60}")
            print(f"  Total transitions analyzed: {overall_stats['total_transitions']:,}")
            print(f"  Mean dwell time: {overall_stats['mean_hours']:.2f} hours")
            print(f"  Median dwell time: {overall_stats['median_hours']:.2f} hours")
            print(f"  Std deviation: {overall_stats['std_hours']:.2f} hours")
        
        return timing_stats
    
    def analyze_tactic_sequences(self, df: pd.DataFrame) -> Dict:
        """
        Analyze attack tactic sequences for label-aware chains.
        
        Args:
            df: DataFrame with all chains
            
        Returns:
            Dictionary with tactic sequence analysis
        """
        print("\n" + "="*60)
        print("ANALYZING ATTACK TACTIC SEQUENCES")
        print("="*60)
        
        tactic_cols = [col for col in df.columns if col.endswith('_tactic')]
        
        if not tactic_cols:
            print("  No tactic columns found (label-agnostic mode)")
            return {}
        
        # Create sequence strings
        def make_sequence(row):
            tactics = []
            chain_len = int(row['chain_length'])
            for i in range(chain_len):
                col = f'hop{i}_tactic' if i > 0 else None
                if col and col in tactic_cols:
                    tactic = str(row[col]) if pd.notna(row[col]) else 'Unknown'
                    tactics.append(tactic)
            return ' → '.join(tactics) if tactics else 'No tactics'
        
        df['tactic_sequence'] = df.apply(make_sequence, axis=1)
        top_sequences = df['tactic_sequence'].value_counts().head(20)
        
        sequence_analysis = {
            'total_chains': len(df),
            'unique_sequences': len(df['tactic_sequence'].unique()),
            'top_sequences': []
        }
        
        print(f"\n  Total chains: {len(df):,}")
        print(f"  Unique tactic sequences: {len(df['tactic_sequence'].unique()):,}")
        print(f"\n  Top 20 Attack Tactic Sequences:")
        
        for rank, (seq, count) in enumerate(top_sequences.items(), 1):
            pct = 100 * count / len(df)
            sequence_analysis['top_sequences'].append({
                'rank': rank,
                'sequence': seq,
                'count': int(count),
                'percentage': float(pct)
            })
            print(f"    {rank:2d}. {seq}")
            print(f"        Count: {count:,} ({pct:.1f}%)")
        
        return sequence_analysis
    
    def analyze_subnet_participation(self, df: pd.DataFrame) -> Dict:
        """
        Analyze subnet participation patterns in attack chains.
        
        Args:
            df: DataFrame with all chains
            
        Returns:
            Dictionary with subnet participation metrics
        """
        print("\n" + "="*60)
        print("ANALYZING SUBNET PARTICIPATION")
        print("="*60)
        
        subnet_cols = [col for col in df.columns if col.endswith('_subnet')]
        all_subnets = []
        
        for col in subnet_cols:
            all_subnets.extend(df[col].dropna().tolist())
        
        subnet_freq = pd.Series(all_subnets).value_counts()
        
        participation_analysis = {
            'unique_subnets': len(subnet_freq),
            'total_appearances': len(all_subnets),
            'top_subnets': []
        }
        
        print(f"\n  Unique subnets in chains: {len(subnet_freq):,}")
        print(f"  Total subnet appearances: {len(all_subnets):,}")
        print(f"  Average appearances per subnet: {len(all_subnets) / len(subnet_freq):.2f}")
        print(f"\n  Top 20 Most Frequent Subnets:")
        
        for rank, (subnet, count) in enumerate(subnet_freq.head(20).items(), 1):
            pct = 100 * count / len(all_subnets)
            participation_analysis['top_subnets'].append({
                'rank': rank,
                'subnet': subnet,
                'count': int(count),
                'percentage': float(pct)
            })
            print(f"    {rank:2d}. {subnet}: {count:,} appearances ({pct:.1f}%)")
        
        return participation_analysis
    
    def analyze_hop_distribution(self, df: pd.DataFrame) -> Dict:
        """
        Analyze distribution of chains across hop depths.
        
        Args:
            df: DataFrame with all chains
            
        Returns:
            Dictionary with hop distribution analysis
        """
        print("\n" + "="*60)
        print("ANALYZING HOP DISTRIBUTION")
        print("="*60)
        
        depth_dist = df['chain_length'].value_counts().sort_index()
        
        hop_analysis = {
            'total_chains': len(df),
            'min_depth': int(depth_dist.index.min()),
            'max_depth': int(depth_dist.index.max()),
            'mean_depth': float(df['chain_length'].mean()),
            'median_depth': float(df['chain_length'].median()),
            'distribution': []
        }
        
        print(f"\n  Total chains: {len(df):,}")
        print(f"  Depth range: {hop_analysis['min_depth']}-{hop_analysis['max_depth']} hops")
        print(f"  Mean depth: {hop_analysis['mean_depth']:.2f} hops")
        print(f"  Median depth: {hop_analysis['median_depth']:.1f} hops")
        print(f"\n  Chain Depth Distribution:")
        
        for depth, count in depth_dist.items():
            pct = 100 * count / len(df)
            hop_analysis['distribution'].append({
                'depth': int(depth),
                'count': int(count),
                'percentage': float(pct)
            })
            print(f"    {depth:2d}-hop: {count:6,} chains ({pct:5.1f}%)")
        
        return hop_analysis
    
    def generate_comprehensive_summary(self, df: pd.DataFrame, mode: str = 'label_aware') -> Dict:
        """
        Generate comprehensive summary with all analysis components.
        
        Args:
            df: DataFrame with all chains
            mode: Analysis mode ('label_aware' or 'label_agnostic')
            
        Returns:
            Dictionary with complete analysis summary
        """
        print("\n" + "="*80)
        print(f"GENERATING COMPREHENSIVE ANALYSIS SUMMARY - {mode.upper()}")
        print("="*80)
        
        summary = {
            'mode': mode,
            'timestamp': pd.Timestamp.now().isoformat(),
            'total_chains': len(df),
            'source_nodes': len(self.top_sources) if self.top_sources else 0
        }
        
        # Hop distribution
        summary['hop_distribution'] = self.analyze_hop_distribution(df)
        
        # Timing statistics
        summary['timing_statistics'] = self.compute_timing_statistics(df)
        
        # Subnet participation
        summary['subnet_participation'] = self.analyze_subnet_participation(df)
        
        # Tactic sequences (label-aware only)
        if mode == 'label_aware':
            summary['tactic_sequences'] = self.analyze_tactic_sequences(df)
        
        # Save comprehensive summary
        summary_file = self.output_dir / f'{mode}_comprehensive_analysis.json'
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"\n✓ Comprehensive analysis saved to {summary_file}")
        
        return summary
    
    def export_thesis_logs(self, summary: Dict, output_path: str = None) -> str:
        """
        Export analysis results in markdown format for thesis integration.
        
        Args:
            summary: Comprehensive analysis summary dictionary
            output_path: Optional path for output file
            
        Returns:
            Markdown formatted string
        """
        if output_path is None:
            output_path = self.output_dir / f"{summary['mode']}_thesis_logs.md"
        
        lines = []
        lines.append(f"# Multi-Hop Chain Analysis Results - {summary['mode'].replace('_', ' ').title()}")
        lines.append(f"\nGenerated: {summary['timestamp']}")
        lines.append(f"\n## Overview")
        lines.append(f"- Total chains extracted: {summary['total_chains']:,}")
        lines.append(f"- Source nodes analyzed: {summary['source_nodes']:,}")
        
        # Hop distribution
        if 'hop_distribution' in summary:
            hop = summary['hop_distribution']
            lines.append(f"\n## Chain Depth Distribution")
            lines.append(f"- Depth range: {hop['min_depth']}-{hop['max_depth']} hops")
            lines.append(f"- Mean depth: {hop['mean_depth']:.2f} hops")
            lines.append(f"- Median depth: {hop['median_depth']:.1f} hops")
            lines.append(f"\n| Depth | Chains | Percentage |")
            lines.append(f"|-------|--------|------------|")
            for item in hop['distribution']:
                lines.append(f"| {item['depth']}-hop | {item['count']:,} | {item['percentage']:.1f}% |")
        
        # Timing statistics
        if 'timing_statistics' in summary and 'overall' in summary['timing_statistics']:
            timing = summary['timing_statistics']
            overall = timing['overall']
            lines.append(f"\n## Adversary Dwell Time Analysis")
            lines.append(f"- **Overall multi-hop cadence: ~{overall['mean_hours']:.0f} hours**")
            lines.append(f"- Median dwell time: {overall['median_hours']:.1f} hours")
            lines.append(f"- Total transitions analyzed: {overall['total_transitions']:,}")
            lines.append(f"\n### Hop-by-Hop Timing")
            lines.append(f"\n| Transition | Count | Mean | Median | Std Dev | Min | Max |")
            lines.append(f"|------------|-------|------|--------|---------|-----|-----|")
            for key, stats in timing.items():
                if key != 'overall':
                    lines.append(
                        f"| {key.replace('_', ' ').title()} | {stats['count']:,} | "
                        f"{stats['mean_hours']:.1f}h | {stats['median_hours']:.1f}h | "
                        f"{stats['std_hours']:.1f}h | {stats['min_hours']:.1f}h | {stats['max_hours']:.1f}h |"
                    )
        
        # Subnet participation
        if 'subnet_participation' in summary:
            subnet = summary['subnet_participation']
            lines.append(f"\n## Subnet Participation")
            lines.append(f"- Unique subnets: {subnet['unique_subnets']:,}")
            lines.append(f"- Total appearances: {subnet['total_appearances']:,}")
            lines.append(f"- Average per subnet: {subnet['total_appearances'] / subnet['unique_subnets']:.2f}")
            lines.append(f"\n### Top 10 Most Active Subnets")
            lines.append(f"\n| Rank | Subnet | Appearances | Percentage |")
            lines.append(f"|------|--------|-------------|------------|")
            for item in subnet['top_subnets'][:10]:
                lines.append(f"| {item['rank']} | {item['subnet']} | {item['count']:,} | {item['percentage']:.1f}% |")
        
        # Tactic sequences
        if 'tactic_sequences' in summary:
            tactics = summary['tactic_sequences']
            lines.append(f"\n## Attack Tactic Sequences (MITRE ATT&CK)")
            lines.append(f"- Total chains: {tactics['total_chains']:,}")
            lines.append(f"- Unique sequences: {tactics['unique_sequences']:,}")
            lines.append(f"\n### Top 10 Most Common Sequences")
            lines.append(f"\n| Rank | Tactic Sequence | Count | Percentage |")
            lines.append(f"|------|----------------|-------|------------|")
            for item in tactics['top_sequences'][:10]:
                lines.append(f"| {item['rank']} | {item['sequence']} | {item['count']:,} | {item['percentage']:.1f}% |")
        
        markdown_content = '\n'.join(lines)
        
        # Save to file
        with open(output_path, 'w') as f:
            f.write(markdown_content)
        
        print(f"\n✓ Thesis logs exported to {output_path}")
        return markdown_content
    
    def run_full_pipeline(self) -> pd.DataFrame:
        """
        Run the complete chain building pipeline.
        
        Returns:
            DataFrame with all chains
        """
        # Step 1: Load data
        self.load_data()
        
        # Step 2: Select top sources
        self.select_top_sources()
        
        # Step 3: Prepare edges
        self.prepare_edges()
        
        # Step 4: Build graph dictionary
        self.build_graph_dict()
        
        # Step 5: Build chains
        self.build_chains()
        
        # Step 6: Analyze and save
        df = self.analyze_and_save()
        
        # Step 7: Print summary
        self.print_summary(df)
        
        return df


