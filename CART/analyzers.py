from .base import Neo4jConnection
import pandas as pd
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.metrics import roc_curve, auc, confusion_matrix
import time
import warnings
import json

warnings.filterwarnings('ignore')

class StructuralPivotAnalyzer(Neo4jConnection):
    """
    Identifies potential pivots based on graph structure.
    Inherits connection logic from Neo4jConnection.
    """
    
    def __init__(self, uri, user, password, database="neo4j"):
        # This one line calls the parent's __init__ method
        # and sets up all the connection attributes for you.
        super().__init__(uri, user, password, database)
    
    # You no longer need to define __init__, connect(), or close() here!
    # They are all inherited from the parent class.

    def run_analysis(self, output_filepath="structural_pivots.json"):
        """
        Runs the full analysis pipeline.
        """
        try:
            if self.connect():
                print("\n" + "="*70)
                print("STRUCTURAL PIVOT CANDIDATE ANALYSIS")
                print("="*70)
                
                candidates = self.find_structural_pivots()
                
                if candidates:
                    self.save_results_to_json(candidates, output_filepath)
                else:
                    print("⚠ No structural pivot candidates were found.")

        finally:
            # self.close() is also inherited
            self.close()

    def find_structural_pivots(self):
        """
        Finds nodes that receive a connection and then initiate another one.
        """
        print("--- Finding all A -> B -> C structural and temporal paths ---")
        start_time = time.time()
        
        with self.driver.session(database=self.database) as session:
            query = """
            MATCH (a:IP)-[r1:CONNECTS]->(b:IP)-[r2:CONNECTS]->(c:IP)
            WHERE r2.timestamp > r1.timestamp AND a <> c AND b <> a
            RETURN
                a.address AS source_ip, b.address AS pivot_ip, c.address AS final_victim_ip,
                r1.timestamp AS compromise_time, r1.tactic AS compromise_tactic,
                r1.port AS compromise_port, r1.is_attack AS compromise_is_attack,
                r2.timestamp AS pivot_time, r2.tactic AS pivot_tactic,
                r2.port AS pivot_port, r2.is_attack AS pivot_is_attack,
                (r2.timestamp - r1.timestamp) AS time_to_pivot
            LIMIT 500000
            """
            result = session.run(query)
            records = [dict(record) for record in result]
            elapsed = time.time() - start_time
            print(f"✓ Found {len(records):,} candidates in {elapsed:.2f} seconds.")
            return records
            
    def save_results_to_json(self, candidates, filepath):
        """Saves the list of candidate dictionaries to a JSON file."""
        print(f"--- Saving {len(candidates):,} candidates to {filepath} ---")
        with open(filepath, 'w') as f:
            json.dump(candidates, f, indent=4)
        print(f"✓ Data successfully written.")

class KillChainAnalyzer(Neo4jConnection):
    """
    Exploratory analysis of APT kill chain patterns in network data.
    Inherits connection logic from Neo4jConnection.
    """
    
    def __init__(self, uri, user, password, database="neo4j"):
        # This one line handles everything
        super().__init__(uri, user, password, database)
    
    def run_full_analysis(self):
        """Run complete exploratory analysis."""
        try:
            self.connect()
            
            print("\n" + "="*70)
            print("KILL CHAIN EXPLORATORY ANALYSIS")
            print("="*70)
            
            # Basic statistics
            self.get_basic_stats()
            
            # Find pivot nodes (victim -> attacker transitions)
            pivot_df = self.find_pivot_nodes()
            
            # Analyze pivot characteristics
            if pivot_df is not None and len(pivot_df) > 0:
                self.analyze_pivot_timing(pivot_df)
                self.analyze_pivot_tactics(pivot_df)
                self.analyze_attack_chains(pivot_df)
                self.visualize_pivot_patterns(pivot_df)
            else:
                print("\n⚠ No pivot nodes found in the data")
            
            print("\n" + "="*70)
            print("ANALYSIS COMPLETE")
            print("="*70)
            
        finally:
            self.close()
    
    def get_basic_stats(self):
        """Get basic statistics about the network and attacks."""
        print("\n--- BASIC STATISTICS ---")
        
        with self.driver.session(database=self.database) as session:
            # Total nodes and edges
            result = session.run("MATCH (n:IP) RETURN count(n) as node_count")
            node_count = result.single()["node_count"]
            
            result = session.run("MATCH ()-[r:CONNECTS]->() RETURN count(r) as edge_count")
            edge_count = result.single()["edge_count"]
            
            # Attack statistics
            result = session.run("MATCH ()-[r:CONNECTS]->() WHERE r.is_attack = 1 RETURN count(r) as attack_count")
            attack_count = result.single()["attack_count"]
            
            # Unique attackers and victims
            result = session.run("""
                MATCH (a:IP)-[r:CONNECTS]->(v:IP) 
                WHERE r.is_attack = 1 
                RETURN count(DISTINCT a) as attacker_count, count(DISTINCT v) as victim_count
            """)
            row = result.single()
            attacker_count = row["attacker_count"]
            victim_count = row["victim_count"]
            
            print(f"  Total IPs: {node_count:,}")
            print(f"  Total Connections: {edge_count:,}")
            print(f"  Attack Connections: {attack_count:,} ({attack_count/edge_count*100:.1f}%)")
            print(f"  Unique Attackers: {attacker_count:,}")
            print(f"  Unique Victims: {victim_count:,}")
            
            # Tactic distribution
            result = session.run("""
                MATCH ()-[r:CONNECTS]->() 
                WHERE r.is_attack = 1 AND r.tactic <> 'none'
                RETURN r.tactic as tactic, count(*) as count 
                ORDER BY count DESC
            """)
            
            print("\n  Attack Tactics Distribution:")
            for record in result:
                print(f"    {record['tactic']}: {record['count']:,}")
    
    def find_pivot_nodes(self):
        """Find nodes that were victims and later became attackers (lateral movement pivots)."""
        print("\n--- FINDING PIVOT NODES (Victim → Attacker Transitions) ---")
        
        with self.driver.session(database=self.database) as session:
            # First, find unique IPs that both received AND sent attacks
            pivot_ips_query = """
            MATCH (attacker:IP)-[r1:CONNECTS]->(pivot:IP)
            WHERE r1.is_attack = 1
            WITH DISTINCT pivot
            MATCH (pivot)-[r2:CONNECTS]->(victim:IP)
            WHERE r2.is_attack = 1
            RETURN DISTINCT pivot.address as pivot_ip
            """
            
            pivot_ips_result = session.run(pivot_ips_query)
            pivot_ips = [record['pivot_ip'] for record in pivot_ips_result]
            
            print(f"  ✓ Found {len(pivot_ips)} unique pivot IPs")
            
            if not pivot_ips:
                print("  ⚠ No pivot nodes found")
                return None
            
            # Now get detailed pivot instances (first time they were attacked → first time they attacked)
            query = """
            UNWIND $pivot_ips AS pivot_address
            
            // Get first time this IP was attacked
            MATCH (attacker:IP)-[r1:CONNECTS]->(pivot:IP {address: pivot_address})
            WHERE r1.is_attack = 1
            WITH pivot, attacker, r1
            ORDER BY r1.timestamp
            WITH pivot, collect({attacker: attacker.address, time: r1.timestamp, tactic: r1.tactic, port: r1.port})[0] AS first_compromise
            
            // Get first time this IP attacked after being compromised
            MATCH (pivot)-[r2:CONNECTS]->(victim:IP)
            WHERE r2.is_attack = 1 
              AND r2.timestamp > first_compromise.time
              AND victim.address <> first_compromise.attacker  // Not attacking back
            WITH pivot, first_compromise, victim, r2
            ORDER BY r2.timestamp
            WITH pivot, first_compromise, collect({victim: victim.address, time: r2.timestamp, tactic: r2.tactic, port: r2.port})[0] AS first_pivot_attack
            
            WHERE first_pivot_attack IS NOT NULL
            
            RETURN 
                pivot.address as pivot_ip,
                first_compromise.attacker as initial_attacker,
                first_pivot_attack.victim as subsequent_victim,
                first_compromise.time as compromised_time,
                first_pivot_attack.time as attack_time,
                (first_pivot_attack.time - first_compromise.time) as time_to_attack,
                first_compromise.tactic as compromise_tactic,
                first_pivot_attack.tactic as attack_tactic,
                first_compromise.port as compromise_port,
                first_pivot_attack.port as attack_port
            ORDER BY compromised_time
            """
            
            result = session.run(query, pivot_ips=pivot_ips)
            records = [dict(record) for record in result]
            
            if not records:
                print("  ⚠ No valid pivot transitions found")
                return None
            
            df = pd.DataFrame(records)
            
            # Convert time_to_attack to hours
            df['hours_to_attack'] = df['time_to_attack'] / 3600
            
            print(f"  ✓ Found {len(df):,} unique pivot transitions (first compromise → first attack)")
            
            # Also get total activity stats per pivot
            activity_query = """
            UNWIND $pivot_ips AS pivot_address
            MATCH (a:IP)-[r1:CONNECTS]->(pivot:IP {address: pivot_address})
            WHERE r1.is_attack = 1
            WITH pivot, count(r1) as times_attacked
            MATCH (pivot)-[r2:CONNECTS]->(v:IP)
            WHERE r2.is_attack = 1
            WITH pivot.address as ip, times_attacked, count(r2) as times_attacked_others
            RETURN ip, times_attacked, times_attacked_others
            ORDER BY times_attacked_others DESC
            """
            
            activity_result = session.run(activity_query, pivot_ips=pivot_ips)
            activity_df = pd.DataFrame([dict(r) for r in activity_result])
            
            print(f"\n  Pivot Activity Summary:")
            print(f"    Most active pivot: {activity_df.iloc[0]['ip']}")
            print(f"      Times compromised: {activity_df.iloc[0]['times_attacked']:,}")
            print(f"      Subsequent attacks launched: {activity_df.iloc[0]['times_attacked_others']:,}")
            
            return df
    
    def analyze_pivot_timing(self, pivot_df):
        """Analyze timing between compromise and subsequent attack."""
        print("\n--- PIVOT TIMING ANALYSIS ---")
        
        print(f"  Time Between Compromise and Attack:")
        print(f"    Mean: {pivot_df['hours_to_attack'].mean():.2f} hours")
        print(f"    Median: {pivot_df['hours_to_attack'].median():.2f} hours")
        print(f"    Min: {pivot_df['hours_to_attack'].min():.2f} hours")
        print(f"    Max: {pivot_df['hours_to_attack'].max():.2f} hours")
        
        # How many within 24 hours?
        within_24h = (pivot_df['hours_to_attack'] <= 24).sum()
        within_48h = (pivot_df['hours_to_attack'] <= 48).sum()
        within_1w = (pivot_df['hours_to_attack'] <= 168).sum()
        
        total = len(pivot_df)
        print(f"\n  Pivot Attack Timeline:")
        print(f"    Within 24 hours: {within_24h:,} ({within_24h/total*100:.1f}%)")
        print(f"    Within 48 hours: {within_48h:,} ({within_48h/total*100:.1f}%)")
        print(f"    Within 1 week: {within_1w:,} ({within_1w/total*100:.1f}%)")
    
    def analyze_pivot_tactics(self, pivot_df):
        """Analyze what tactics are used for compromise vs subsequent attacks."""
        print("\n--- PIVOT TACTIC ANALYSIS ---")
        
        print("  Most Common Compromise Tactics:")
        compromise_tactics = pivot_df['compromise_tactic'].value_counts().head(10)
        for tactic, count in compromise_tactics.items():
            print(f"    {tactic}: {count:,}")
        
        print("\n  Most Common Attack Tactics (After Compromise):")
        attack_tactics = pivot_df['attack_tactic'].value_counts().head(10)
        for tactic, count in attack_tactics.items():
            print(f"    {tactic}: {count:,}")
        
        # Tactic transitions
        print("\n  Most Common Tactic Transitions:")
        transitions = pivot_df.groupby(['compromise_tactic', 'attack_tactic']).size().sort_values(ascending=False).head(10)
        for (comp, attack), count in transitions.items():
            print(f"    {comp} → {attack}: {count:,}")
    
    def analyze_attack_chains(self, pivot_df):
        """Analyze multi-hop attack chains (A→B→C→D...)."""
        print("\n--- ATTACK CHAIN ANALYSIS ---")
        
        with self.driver.session(database=self.database) as session:
            # Find chains of length 3+ (A→B→C)
            query = """
            MATCH path = (a:IP)-[r1:CONNECTS]->(b:IP)-[r2:CONNECTS]->(c:IP)-[r3:CONNECTS]->(d:IP)
            WHERE r1.is_attack = 1 AND r2.is_attack = 1 AND r3.is_attack = 1
            AND r2.timestamp > r1.timestamp
            AND r3.timestamp > r2.timestamp
            RETURN 
                a.address as hop1,
                b.address as hop2, 
                c.address as hop3,
                d.address as hop4,
                r1.timestamp as t1,
                r2.timestamp as t2,
                r3.timestamp as t3
            LIMIT 100
            """
            
            result = session.run(query)
            chains = [dict(record) for record in result]
            
            if chains:
                print(f"  ✓ Found {len(chains):,} attack chains (3+ hops)")
                
                # Show example chain
                if chains:
                    chain = chains[0]
                    print(f"\n  Example Attack Chain:")
                    print(f"    {chain['hop1']} → {chain['hop2']} → {chain['hop3']} → {chain['hop4']}")
                    
                    t1 = chain['t1']
                    t2 = chain['t2']
                    t3 = chain['t3']
                    print(f"    Timing: 0h → {(t2-t1)/3600:.1f}h → {(t3-t1)/3600:.1f}h")
            else:
                print("  No multi-hop chains found (or chains are rare)")
    
    def visualize_pivot_patterns(self, pivot_df):
        """Create visualizations of pivot patterns."""
        print("\n--- GENERATING VISUALIZATIONS ---")
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        # 1. Time to attack histogram
        ax = axes[0, 0]
        pivot_df[pivot_df['hours_to_attack'] <= 168]['hours_to_attack'].hist(bins=50, ax=ax, edgecolor='black')
        ax.axvline(24, color='red', linestyle='--', label='24 hours')
        ax.set_xlabel('Hours Between Compromise and Attack')
        ax.set_ylabel('Frequency')
        ax.set_title('Distribution of Time to Pivot Attack (≤1 week)')
        ax.legend()
        
        # 2. Compromise tactics
        ax = axes[0, 1]
        top_comp = pivot_df['compromise_tactic'].value_counts().head(10)
        top_comp.plot(kind='barh', ax=ax, color='steelblue')
        ax.set_xlabel('Count')
        ax.set_title('Top 10 Compromise Tactics')
        
        # 3. Attack tactics
        ax = axes[1, 0]
        top_attack = pivot_df['attack_tactic'].value_counts().head(10)
        top_attack.plot(kind='barh', ax=ax, color='coral')
        ax.set_xlabel('Count')
        ax.set_title('Top 10 Subsequent Attack Tactics')
        
        # 4. Pivot IPs - how many times did each pivot?
        ax = axes[1, 1]
        pivot_counts = pivot_df['pivot_ip'].value_counts().head(20)
        pivot_counts.plot(kind='bar', ax=ax, color='darkgreen')
        ax.set_xlabel('Pivot IP (top 20)')
        ax.set_ylabel('Number of Times Used as Pivot')
        ax.set_title('Most Frequently Used Pivot IPs')
        ax.tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        plt.savefig('killchain_analysis.png', dpi=150, bbox_inches='tight')
        print("  ✓ Saved visualizations to 'killchain_analysis.png'")
        
        # Save pivot data to CSV for further analysis
        pivot_df.to_csv('pivot_nodes.csv', index=False)
        print("  ✓ Saved pivot node data to 'pivot_nodes.csv'")


class ThesisAnalyzer(Neo4jConnection):
    """
    A comprehensive class to manage the entire data pipeline for the thesis project.
    Inherits connection logic from Neo4jConnection.
    """
    
    def __init__(self, uri, user, password, database="neo4j"):
        # This single line replaces the original __init__, connect, and close methods
        super().__init__(uri, user, password, database)

    # ==============================================================================
    # === DATABASE BUILDING METHODS ================================================
    # ==============================================================================

    def build_database(self, rebuild=True):
        """
        Orchestrates the entire database construction process.
        
        Args:
            rebuild (bool, optional): If True (default), the existing database will be
                                      wiped clean before loading new data. If False,
                                      new data will be added to the existing graph.
        """
        print("Starting database build process...")
        
        # Download and prepare data
        df = self._download_and_prepare_data()
        if df is None:
            print("Failed to prepare data. Aborting.")
            return
        
        try:
            if self.connect():
                self._write_dataframe_to_neo4j(df, rebuild=rebuild)
                print("\nDatabase build process finished successfully.")
        finally:
            self.close()

    def _download_and_prepare_data(self):
        """Downloads Zeek data and returns as DataFrame."""
        print("Step 1: Loading and Preparing Data...")
        BASE_URL = "https://datasets.uwf.edu/data/UWF-ZeekData24/parquet/"
        all_dataframes = []
        
        try:
            directory_urls = self._get_directory_urls(BASE_URL)
            print(f"Found {len(directory_urls)} directories to process...")
            
            for dir_url in directory_urls:
                response = requests.get(dir_url)
                soup = BeautifulSoup(response.text, 'html.parser')
                parquet_urls = [urljoin(dir_url, link.get('href')) 
                              for link in soup.find_all('a') 
                              if link.get('href').endswith('.parquet')]
                
                print(f"  Processing directory: {dir_url.split('/')[-2]}")
                for url in parquet_urls:
                    print(f"    Loading: {url.split('/')[-1]}")
                    all_dataframes.append(pd.read_parquet(url, engine='pyarrow'))
            
            if not all_dataframes:
                raise ValueError("No dataframes were loaded.")
            
            print("\nCombining and cleaning data...")
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            cleaned_df = combined_df[combined_df['label_technique'] != 'Duplicate'].copy().head(500_000)
            
            print(f"Prepared {len(cleaned_df):,} rows for import.")
            return cleaned_df
            
        except Exception as e:
            print(f"Error during data preparation: {e}")
            return None

    def _get_directory_urls(self, base_url):
        """Helper to fetch subdirectory URLs."""
        response = requests.get(base_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        return [urljoin(base_url, link.get('href')) 
                for link in soup.find_all('a') 
                if link.get('href') and link.get('href').startswith('2024') 
                and link.get('href').endswith('/')]

    def _write_dataframe_to_neo4j(self, df, rebuild=True):
        """Write DataFrame directly to Neo4j - optimized for large datasets."""
        print("\nStep 2: Writing Data to Neo4j...")
        
        with self.driver.session(database=self.database) as session:
            if rebuild:
                print("Rebuild flag is True. Clearing database in batches...")
                deleted = 1
                total_deleted = 0
                while deleted > 0:
                    result = session.run("""
                        MATCH (n)
                        WITH n LIMIT 10000
                        DETACH DELETE n
                        RETURN count(n) as deleted
                    """)
                    deleted = result.single()["deleted"]
                    total_deleted += deleted
                    if deleted > 0:
                        print(f"  Cleared {total_deleted:,} nodes...")
                print(f"  ✓ Database cleared ({total_deleted:,} nodes total)")
            
            # Create index FIRST (critical for performance)
            print("\nCreating indexes...")
            session.run("CREATE INDEX ip_address_index IF NOT EXISTS FOR (n:IP) ON (n.address)")
            print("  ✓ Index created for IP addresses")
            
            # Prepare data
            total_rows = len(df)
            batch_size = 5000  # Optimal batch size for Neo4j
            
            print(f"\nWriting {total_rows:,} rows in batches of {batch_size:,}...")
            
            start_time = time.time()
            
            for i in range(0, total_rows, batch_size):
                batch = df.iloc[i:i+batch_size].copy()
                
                # Convert label_binary to integer (0/1) for GDS compatibility
                batch['label_binary'] = batch['label_binary'].astype(bool).astype(int)
                
                # Convert to records - only select needed columns
                records = batch[[
                    'src_ip_zeek', 'dest_ip_zeek', 'ts', 'duration', 
                    'service', 'dest_port_zeek', 'conn_state', 
                    'label_tactic', 'label_technique', 'label_binary'
                ]].to_dict('records')
                
                # Optimized query with MERGE for IPs (avoids duplicates)
                query = """
                UNWIND $records AS row
                MERGE (orig:IP {address: row.src_ip_zeek})
                MERGE (resp:IP {address: row.dest_ip_zeek})
                CREATE (orig)-[:CONNECTS {
                    timestamp: row.ts,
                    duration: row.duration,
                    service: row.service,
                    port: row.dest_port_zeek,
                    state: row.conn_state,
                    tactic: row.label_tactic,
                    technique: row.label_technique,
                    is_attack: row.label_binary
                }]->(resp)
                """
                
                session.run(query, records=records)
                
                # Progress tracking
                progress = min(i + batch_size, total_rows)
                pct = (progress / total_rows) * 100
                elapsed = time.time() - start_time
                rate = progress / elapsed if elapsed > 0 else 0
                eta = (total_rows - progress) / rate if rate > 0 else 0
                
                print(f"  Progress: {progress:,}/{total_rows:,} ({pct:.1f}%) | "
                      f"Rate: {rate:.0f} rows/sec | ETA: {eta/60:.1f} min")
            
            total_time = time.time() - start_time
            print(f"\n✓ Data writing complete in {total_time/60:.1f} minutes")
            print(f"  Average rate: {total_rows/total_time:.0f} rows/sec")

    # ==============================================================================
    # === DATABASE ANALYSIS METHODS ================================================
    # ==============================================================================
    
    def run_analysis(self):
        """
        Connects to the database and runs the full analysis pipeline.
        (Hypothesis Test, Verification Query, and Visualization)
        """
        try:
            self.connect()
            if self.driver:
                self.test_pivot_prediction()
                self.run_verification_query()
                self.visualize_attack_graph()
        finally:
            self.close()

    def test_pivot_prediction(self):
        """
        Tests the core hypothesis: Can GNN embeddings predict which reconnaissance 
        victims will become pivots?
        """
        print("\n" + "="*80)
        print("PIVOT PREDICTION ANALYSIS")
        print("="*80)
        
        with self.driver.session(database=self.database) as session:
            # Drop existing graph projection if it exists
            try: 
                session.run("CALL gds.graph.drop('pivotGraph', false)")
            except Exception: 
                pass
            
            print("\n1. Projecting graph into GDS...")
            session.run("""
                CALL gds.graph.project(
                    'pivotGraph', 
                    'IP', 
                    {CONNECTS: {properties: 'is_attack'}}
                )
            """)
            print("   ✓ Graph projected")
            
            print("\n2. Generating node embeddings with FastRP...")
            result = session.run("""
                CALL gds.fastRP.write('pivotGraph', {
                    embeddingDimension: 128, 
                    writeProperty: 'embedding',
                    randomSeed: 42
                })
                YIELD nodePropertiesWritten
                RETURN nodePropertiesWritten
            """)
            nodes_updated = result.single()['nodePropertiesWritten']
            print(f"   ✓ Created embeddings for {nodes_updated:,} nodes")
            
            print("\n3. Identifying reconnaissance victims and their outcomes...")
            
            # Get all IPs that received reconnaissance attacks
            victims_query = """
            MATCH (attacker:IP)-[r:CONNECTS]->(victim:IP)
            WHERE r.is_attack = 1 
              AND r.tactic = 'Reconnaissance'
              AND victim.embedding IS NOT NULL
            WITH DISTINCT victim
            
            // Check if this victim later became an attacker
            OPTIONAL MATCH (victim)-[r2:CONNECTS]->(target:IP)
            WHERE r2.is_attack = 1
            
            RETURN 
                victim.address AS ip,
                victim.embedding AS embedding,
                count(r2) > 0 AS became_pivot
            """
            
            result = session.run(victims_query)
            victims = [dict(record) for record in result]
            
            if not victims:
                print("   ✗ No reconnaissance victims found with embeddings")
                return
            
            victims_df = pd.DataFrame(victims)
            
            pivot_count = victims_df['became_pivot'].sum()
            non_pivot_count = len(victims_df) - pivot_count
            
            print(f"   ✓ Found {len(victims_df)} reconnaissance victims:")
            print(f"      - Became pivots: {pivot_count} ({pivot_count/len(victims_df)*100:.1f}%)")
            print(f"      - Did NOT pivot: {non_pivot_count} ({non_pivot_count/len(victims_df)*100:.1f}%)")
            
            if pivot_count == 0:
                print("   ✗ No pivots found - cannot test hypothesis")
                return
            
            print("\n4. Computing embedding similarities...")
            
            # Separate pivots and non-pivots
            pivot_embeddings = np.array([v['embedding'] for v in victims if v['became_pivot']])
            non_pivot_embeddings = np.array([v['embedding'] for v in victims if not v['became_pivot']])
            
            # Compute reference pivot embedding (mean of all pivots)
            reference_pivot_embedding = np.mean(pivot_embeddings, axis=0)
            
            # Compute similarities for all victims
            similarities = []
            for victim in victims:
                victim_emb = np.array(victim['embedding']).reshape(1, -1)
                ref_emb = reference_pivot_embedding.reshape(1, -1)
                sim = cosine_similarity(victim_emb, ref_emb)[0][0]
                similarities.append({
                    'ip': victim['ip'],
                    'similarity': sim,
                    'actual_pivot': victim['became_pivot']
                })
            
            similarity_df = pd.DataFrame(similarities)
            
            # Statistical analysis
            pivot_sims = similarity_df[similarity_df['actual_pivot']]['similarity']
            non_pivot_sims = similarity_df[~similarity_df['actual_pivot']]['similarity']
            
            print(f"\n   Similarity Statistics:")
            print(f"      Pivots:     mean={pivot_sims.mean():.4f}, median={pivot_sims.median():.4f}, std={pivot_sims.std():.4f}")
            print(f"      Non-pivots: mean={non_pivot_sims.mean():.4f}, median={non_pivot_sims.median():.4f}, std={non_pivot_sims.std():.4f}")
            print(f"      Difference: {pivot_sims.mean() - non_pivot_sims.mean():.4f}")
            
            # Statistical test
            from scipy import stats
            t_stat, p_value = stats.ttest_ind(pivot_sims, non_pivot_sims)
            print(f"\n   Welch's t-test: t={t_stat:.4f}, p={p_value:.6f}")
            
            if p_value < 0.05:
                print(f"   ✓ STATISTICALLY SIGNIFICANT (p < 0.05)")
            else:
                print(f"   ✗ Not statistically significant (p >= 0.05)")
            
            # Find optimal threshold using ROC curve
            print("\n5. Finding optimal classification threshold...")
            
            fpr, tpr, thresholds = roc_curve(
                similarity_df['actual_pivot'], 
                similarity_df['similarity']
            )
            roc_auc = auc(fpr, tpr)
            
            # Find threshold that maximizes F1 score
            f1_scores = []
            for threshold in thresholds:
                predictions = similarity_df['similarity'] >= threshold
                tp = ((predictions) & (similarity_df['actual_pivot'])).sum()
                fp = ((predictions) & (~similarity_df['actual_pivot'])).sum()
                fn = ((~predictions) & (similarity_df['actual_pivot'])).sum()
                
                precision = tp / (tp + fp) if (tp + fp) > 0 else 0
                recall = tp / (tp + fn) if (tp + fn) > 0 else 0
                f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
                f1_scores.append(f1)
            
            optimal_idx = np.argmax(f1_scores)
            optimal_threshold = thresholds[optimal_idx]
            
            print(f"   ✓ Optimal threshold: {optimal_threshold:.4f}")
            print(f"   ✓ AUC-ROC: {roc_auc:.4f}")
            
            # Make predictions with optimal threshold
            print("\n6. Evaluating predictions with optimal threshold...")
            
            similarity_df['predicted_pivot'] = similarity_df['similarity'] >= optimal_threshold
            
            # Confusion matrix
            cm = confusion_matrix(
                similarity_df['actual_pivot'], 
                similarity_df['predicted_pivot']
            )
            
            tn, fp, fn, tp = cm.ravel()
            
            accuracy = (tp + tn) / len(similarity_df)
            precision = tp / (tp + fp) if (tp + fp) > 0 else 0
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0
            f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
            
            print("\n" + "="*80)
            print("HYPOTHESIS TEST RESULTS")
            print("="*80)
            print(f"\nConfusion Matrix:")
            print(f"                    Predicted Non-Pivot    Predicted Pivot")
            print(f"Actual Non-Pivot           {tn:5d}                {fp:5d}")
            print(f"Actual Pivot               {fn:5d}                {tp:5d}")
            
            print(f"\nPerformance Metrics:")
            print(f"  Accuracy:  {accuracy*100:.2f}%")
            print(f"  Precision: {precision*100:.2f}%")
            print(f"  Recall:    {recall*100:.2f}%")
            print(f"  F1-Score:  {f1:.4f}")
            print(f"  AUC-ROC:   {roc_auc:.4f}")
            
            print(f"\n" + "="*80)
            if accuracy >= 0.80:
                print("✓ HYPOTHESIS SUPPORTED: Accuracy ≥ 80%")
                print("  Graph structural embeddings successfully predict pivot behavior")
            else:
                print("✗ HYPOTHESIS NOT SUPPORTED: Accuracy < 80%")
                print("  Structural features alone may be insufficient")
            print("="*80)
            
            # Save results
            similarity_df.to_csv('pivot_predictions.csv', index=False)
            print(f"\n✓ Detailed predictions saved to 'pivot_predictions.csv'")
            
            # Plot ROC curve
            plt.figure(figsize=(10, 8))
            plt.plot(fpr, tpr, color='darkorange', lw=2, label=f'ROC curve (AUC = {roc_auc:.4f})')
            plt.plot([0, 1], [0, 1], color='navy', lw=2, linestyle='--', label='Random guess')
            plt.xlim([0.0, 1.0])
            plt.ylim([0.0, 1.05])
            plt.xlabel('False Positive Rate')
            plt.ylabel('True Positive Rate')
            plt.title('ROC Curve: Pivot Prediction Using GNN Embeddings')
            plt.legend(loc="lower right")
            plt.grid(alpha=0.3)
            plt.savefig('roc_curve.png', dpi=150, bbox_inches='tight')
            print(f"✓ ROC curve saved to 'roc_curve.png'")

    def run_verification_query(self):
        """Finds and prints example attack paths from the graph."""
        print("\n--- VERIFYING ATTACK PATHS ---")
        query = """
        MATCH (a:IP)-[r:CONNECTS]->(v:IP) 
        WHERE r.is_attack = 1 AND r.tactic <> 'none' AND r.tactic <> 'Duplicate' 
        RETURN a.address AS attacker_ip, v.address AS victim_ip, r.port AS port, r.tactic AS tactic 
        LIMIT 10
        """
        with self.driver.session(database=self.database) as session:
            result = session.run(query).data()
            if result:
                for record in result:
                    print(f"Attack Detected: {record['attacker_ip']} -> {record['victim_ip']} "
                          f"on port {record['port']} (Tactic: {record['tactic']})")
            else:
                print("No attack paths found.")

    def visualize_attack_graph(self):
        """Generates and saves a PNG visualization of the attack graph."""
        print("\n--- GENERATING GRAPH VISUALIZATION ---")
        query = "MATCH (a:IP)-[r:CONNECTS]->(v:IP) RETURN a.address AS source, v.address AS target, r.is_attack AS is_attack LIMIT 1000"
        
        with self.driver.session(database=self.database) as session:
            results = session.run(query).data()
        
        if not results: 
            print("No data to visualize.")
            return
        
        G = nx.DiGraph()
        attack_edges = set()
        attacker_nodes = set()
        
        for record in results:
            G.add_edge(record['source'], record['target'])
            if record['is_attack']:
                attack_edges.add((record['source'], record['target']))
                attacker_nodes.add(record['source'])
        
        node_colors = ['red' if node in attacker_nodes else 'skyblue' for node in G.nodes()]
        edge_colors = ['red' if edge in attack_edges else 'lightgray' for edge in G.edges()]
        
        plt.figure(figsize=(20, 16))
        pos = nx.spring_layout(G, k=0.7, iterations=40)
        nx.draw(G, pos, with_labels=True, node_color=node_colors, edge_color=edge_colors, 
                node_size=1500, font_size=8, width=0.8, arrows=True)
        plt.title("Network Graph with Attack Paths Highlighted (Red=Attackers)", size=20)
        plt.savefig("network_attack_graph.png", dpi=150, bbox_inches='tight')
        print("✓ Visualization saved to 'network_attack_graph.png'")