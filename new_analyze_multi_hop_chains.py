# New version of analyze_multi_hop_chains for CART/analyzers.py
# Replace the existing method (lines 1805-2116) with this implementation

def analyze_multi_hop_chains(self, use_labels: bool, output_prefix: str, n_hops: int = None, use_cache: bool = True):
    """
    Analyze multi-hop attack chains by building complete chains to natural termination.
    
    Args:
        use_labels: Whether to use attack labels (filter to attacks only)
        output_prefix: Prefix for output files
        n_hops: DEPRECATED - Kept for compatibility but ignored. Chains are built to natural termination.
        use_cache: Whether to use cached graph dictionary
    
    Builds all possible temporal chains for each source node until:
    - No more valid temporal extensions exist (timestamp > previous)
    - Cycle detection prevents revisiting nodes
    - Safety limits prevent explosion
    
    Saves:
    - {output_prefix}_all_chains.csv: All chains with chain_length column
    - {output_prefix}_chain_summary.json: Distribution and statistics
    """
    if n_hops is not None:
        print(f"⚠ Warning: n_hops parameter is deprecated. Building complete chains to natural termination.")
    
    print(f"\n--- Complete Chain Analysis (Node-by-Node to Natural Termination) ---")
    
    if pl is None:
        print("  ⚠ Polars not installed. Run: pip install polars")
        return
    
    import json
    from collections import defaultdict
    from tqdm.auto import tqdm
    
    output_file_chains = f'{output_prefix}_all_chains.csv'
    output_file_summary = f'{output_prefix}_chain_summary.json'
    
    # Create cache directory
    cache_dir = os.path.join(os.path.dirname(output_prefix) or '.', '.chain_cache')
    os.makedirs(cache_dir, exist_ok=True)
    
    # Generate cache key based on parameters
    cache_params = f"labels={use_labels}"
    cache_key = hashlib.md5(cache_params.encode()).hexdigest()
    graph_cache_file = os.path.join(cache_dir, f'graph_dict_{cache_key}.json')
    
    # Step 1: Build/load graph dictionary
    if use_cache and os.path.exists(graph_cache_file):
        print(f"  Step 1: Loading cached graph from {graph_cache_file}")
        with open(graph_cache_file, 'r') as f:
            graph_dict = json.load(f)
        
        total_nodes = len(graph_dict)
        total_edges = sum(len(neighbors) for neighbors in graph_dict.values())
        print(f"  ✓ Loaded graph with {total_nodes:,} nodes and {total_edges:,} unique edges")
    else:
        print("  Step 1: Building graph dictionary from Neo4j...")
        
        with self.driver.session(database=self.database) as session:
            # Query all edges
            query = """
            MATCH (a:IP)-[r:CONNECTS]->(b:IP)
            """ + ("WHERE r.is_attack = 1" if use_labels else "") + """
            RETURN 
                a.address as src_ip,
                b.address as dst_ip,
                a.subnet as src_subnet,
                b.subnet as dst_subnet,
                r.timestamp as timestamp,
                r.is_attack as is_attack,
                CASE WHEN r.is_attack = 1 THEN r.tactic ELSE null END as tactic
            ORDER BY r.timestamp
            """
            
            result = session.run(query)
            edges = list(tqdm(result, desc="    Fetching edges"))
        
        print(f"  ✓ Retrieved {len(edges):,} edges from Neo4j")
        print(f"    Building graph structure: {{src: {{dst: [connections]}}}}...")
        
        # Build nested dictionary
        graph_dict = {}
        for record in tqdm(edges, desc="    Processing edges"):
            src = record['src_ip']
            dst = record['dst_ip']
            
            if src not in graph_dict:
                graph_dict[src] = {}
            if dst not in graph_dict[src]:
                graph_dict[src][dst] = []
            
            graph_dict[src][dst].append({
                'timestamp': record['timestamp'],
                'src_subnet': record['src_subnet'],
                'dst_subnet': record['dst_subnet'],
                'is_attack': record['is_attack'],
                'tactic': record['tactic']
            })
        
        # Sort connections by timestamp
        print(f"    Sorting connections by timestamp...")
        for src in tqdm(graph_dict, desc="    Finalizing"):
            for dst in graph_dict[src]:
                graph_dict[src][dst].sort(key=lambda x: x['timestamp'])
        
        total_nodes = len(graph_dict)
        total_edges = sum(len(neighbors) for neighbors in graph_dict.values())
        print(f"  ✓ Graph contains {total_nodes:,} nodes and {total_edges:,} unique edges")
        
        # Cache as JSON
        if use_cache:
            print(f"    Caching graph to {graph_cache_file}...")
            with open(graph_cache_file, 'w') as f:
                json.dump(graph_dict, f)
            print("  ✓ Graph cached")
    
    # Step 2: Build complete chains using the worker function approach
    print(f"  Step 2: Building complete chains to natural termination...")
    print(f"    Using pure Python dictionary lookups (no database queries)")
    
    MAX_TOTAL_CHAINS = 1_000_000  # Safety limit per source node
    MAX_EXTENSIONS_PER_CHAIN = 10  # Branching limit
    
    # Get all source nodes
    all_source_nodes = list(graph_dict.keys())
    print(f"    Processing {len(all_source_nodes):,} source nodes...")
    
    # Build chains for all nodes
    all_chains = []
    for src_ip in tqdm(all_source_nodes, desc="    Building chains"):
        if src_ip not in graph_dict:
            continue
        
        # Initialize 1-hop chains from this source
        node_chains = []
        for dst_ip, connections in graph_dict[src_ip].items():
            for conn in connections:
                chain = [
                    src_ip,
                    conn['src_subnet'],
                    dst_ip,
                    conn['dst_subnet'],
                    conn['timestamp'],
                    conn['tactic']
                ]
                node_chains.append(chain)
        
        # Keep extending until no more valid extensions
        iteration = 0
        while node_chains:
            if len(all_chains) + len(node_chains) >= MAX_TOTAL_CHAINS:
                all_chains.extend(node_chains[:MAX_TOTAL_CHAINS - len(all_chains)])
                break
            
            next_chains = []
            had_extensions = False
            
            for chain in node_chains:
                # Extract last hop info
                n_hops = (len(chain) - 2) // 4
                last_ip_idx = 2 + (n_hops - 1) * 4
                last_ip = chain[last_ip_idx]
                last_time_idx = last_ip_idx + 2
                last_time = chain[last_time_idx]
                
                # Check for outgoing connections
                if last_ip not in graph_dict:
                    next_chains.append(chain)
                    continue
                
                # Find valid extensions
                extensions_found = 0
                had_valid_extension = False
                
                for next_ip, connections in graph_dict[last_ip].items():
                    # Cycle prevention
                    ip_positions = [0] + [2 + 4*k for k in range(n_hops)]
                    if next_ip in [chain[pos] for pos in ip_positions]:
                        continue
                    
                    # Temporal constraint
                    valid_conns = [c for c in connections if c['timestamp'] > last_time]
                    if not valid_conns:
                        continue
                    
                    # Limit branching
                    conns_to_use = valid_conns[:MAX_EXTENSIONS_PER_CHAIN]
                    
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
                        had_extensions = True
                        
                        if extensions_found >= MAX_EXTENSIONS_PER_CHAIN:
                            break
                    
                    if extensions_found >= MAX_EXTENSIONS_PER_CHAIN:
                        break
                
                if not had_valid_extension:
                    next_chains.append(chain)
                
                if len(next_chains) >= MAX_TOTAL_CHAINS:
                    break
            
            if not had_extensions:
                # No chains were extended, we're done
                all_chains.extend(next_chains)
                break
            
            node_chains = next_chains
            iteration += 1
    
    print(f"  ✓ Built {len(all_chains):,} complete chains")
    
    # Step 3: Analyze chains by depth
    print("\n  Step 3: Analyzing chain depth distribution...")
    
    depth_counts = defaultdict(int)
    max_depth = 0
    for chain in all_chains:
        n_hops = (len(chain) - 2) // 4
        depth_counts[n_hops] += 1
        max_depth = max(max_depth, n_hops)
    
    print(f"\n  Chain depth distribution:")
    for depth in sorted(depth_counts.keys()):
        pct = 100 * depth_counts[depth] / len(all_chains)
        print(f"    {depth}-hop: {depth_counts[depth]:,} chains ({pct:.1f}%)")
    
    # Step 4: Convert to DataFrame with chain_length column
    print(f"\n  Step 4: Converting to DataFrame...")
    
    # Build column names for max depth
    columns = ['chain_length', 'hop0_ip', 'hop0_subnet']
    for i in range(1, max_depth + 1):
        columns.extend([f'hop{i}_ip', f'hop{i}_subnet', f'hop{i}_time', f'hop{i}_tactic'])
    
    # Convert chains to rows (pad shorter chains)
    rows = []
    for chain in tqdm(all_chains, desc="    Processing chains"):
        n_hops = (len(chain) - 2) // 4
        row = [n_hops] + chain
        
        # Pad with None
        while len(row) < len(columns):
            row.append(None)
        
        rows.append(row)
    
    import pandas as pd
    df = pd.DataFrame(rows, columns=columns)
    
    # Step 5: Save results
    print(f"\n  Step 5: Saving results...")
    df.to_csv(output_file_chains, index=False)
    print(f"    ✓ Saved {len(df):,} chains to {output_file_chains}")
    
    # Save summary
    summary = {
        'total_chains': len(all_chains),
        'max_depth': max_depth,
        'depth_distribution': dict(depth_counts),
        'source_nodes': len(all_source_nodes),
        'use_labels': use_labels
    }
    with open(output_file_summary, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"    ✓ Saved summary to {output_file_summary}")
    
    # Step 6: Print summary statistics
    print(f"\n  Summary Statistics:")
    print(f"    Total chains: {len(all_chains):,}")
    print(f"    Depth range: {min(depth_counts.keys())}-{max(depth_counts.keys())} hops")
    print(f"    Most common depth: {max(depth_counts, key=depth_counts.get)}-hop ({depth_counts[max(depth_counts, key=depth_counts.get)]:,} chains)")
    
    # Analyze tactics if label-aware
    if use_labels:
        print(f"\n  Top Attack Tactics (all chains):")
        all_tactics = []
        for col in df.columns:
            if col.endswith('_tactic'):
                all_tactics.extend(df[col].dropna().tolist())
        
        if all_tactics:
            import pandas as pd
            tactic_counts = pd.Series(all_tactics).value_counts()
            for tactic, count in tactic_counts.head(10).items():
                if tactic:
                    pct = 100 * count / len(all_tactics)
                    print(f"    {tactic}: {count:,} ({pct:.1f}%)")
    
    print(f"\n  ✓ Analysis complete!")
    print(f"    All chains: {output_file_chains}")
    print(f"    Summary: {output_file_summary}")
    print(f"  💾 Graph dictionary cached at {graph_cache_file}")
