import pandas as pd
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from neo4j import GraphDatabase
from sklearn.metrics.pairwise import cosine_similarity
import os
import time
import warnings

warnings.filterwarnings('ignore')

class ThesisAnalyzer:
    """
    A comprehensive class to manage the entire data pipeline for the thesis project.
    
    This class handles:
    1. Downloading and preparing the real-world Zeek data.
    2. Building a Neo4j graph database by writing directly via Python driver.
    3. Running GDS analysis and other queries on the constructed database.
    
    Designed to be imported and used from a Jupyter Notebook.
    """
    
    def __init__(self, uri, user, password, database="neo4j"):
        """
        Initializes the analyzer with database credentials.
        
        Args:
            uri (str): The Bolt URI for the Neo4j instance.
            user (str): The username for the Neo4j instance.
            password (str): The password for the Neo4j instance.
            database (str): The database name (default: "neo4j" for Community Edition).
        """
        self.uri = uri
        self.user = user
        self.password = password
        self.database = database
        self.driver = None

    def connect(self):
        """Establishes a connection to the Neo4j database."""
        if self.driver is None:
            try:
                self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
                self.driver.verify_connectivity()
                print("--- Successfully connected to the Neo4j database. ---")
            except Exception as e:
                print(f"Failed to connect to Neo4j: {e}")
                self.driver = None

    def close(self):
        """Closes the connection to the Neo4j database."""
        if self.driver is not None:
            self.driver.close()
            self.driver = None
            print("\n--- Neo4j connection closed. ---")

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
            self.connect()
            if self.driver:
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
            cleaned_df = combined_df[combined_df['label_technique'] != 'Duplicate'].copy().head(100_000)
            
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
                self.test_thesis_hypothesis()
                self.run_verification_query()
                self.visualize_attack_graph()
        finally:
            self.close()

    def test_thesis_hypothesis(self):
        """Runs the GDS analysis to test the core hypothesis."""
        print("\n--- Running Thesis Hypothesis Test ---")
        with self.driver.session(database=self.database) as session:
            try: 
                session.run("CALL gds.graph.drop('thesisGraph', false)")
            except Exception: 
                pass
            
            print("1. Projecting graph into GDS...")
            session.run("CALL gds.graph.project('thesisGraph', 'IP', {CONNECTS: {properties: 'is_attack'}})")
            
            print("2. Generating node embeddings with FastRP and writing back to nodes...")
            result = session.run("""
                CALL gds.fastRP.write('thesisGraph', {
                    embeddingDimension: 128, 
                    writeProperty: 'embedding'
                })
                YIELD nodePropertiesWritten
                RETURN nodePropertiesWritten
            """)
            nodes_updated = result.single()['nodePropertiesWritten']
            print(f"   ✓ Created embeddings for {nodes_updated:,} nodes")
            
            print("3. Fetching embeddings for correlation analysis...")
            early_stage_tactics = ['Reconnaissance', 'Initial Access']
            
            # Get early-stage attacker embedding
            early_query = """
            MATCH (a:IP)-[r:CONNECTS]->() 
            WHERE r.tactic IN $tactics AND a.embedding IS NOT NULL
            RETURN a.embedding AS embedding 
            LIMIT 1
            """
            early_result = session.run(early_query, tactics=early_stage_tactics).single()
            
            # Get late-stage attacker embedding  
            late_query = """
            MATCH (a:IP)-[r:CONNECTS]->() 
            WHERE r.is_attack = 1 
            AND NOT r.tactic IN $early_tactics 
            AND r.tactic <> 'none' 
            AND r.tactic <> 'Duplicate'
            AND a.embedding IS NOT NULL
            RETURN a.embedding AS embedding 
            LIMIT 1
            """
            late_result = session.run(late_query, early_tactics=early_stage_tactics).single()
            
            if not early_result or not late_result:
                print("Could not find embeddings for both early and late-stage attacks.")
                return

            early_embedding = np.array(early_result['embedding']).reshape(1, -1)
            late_embedding = np.array(late_result['embedding']).reshape(1, -1)
            similarity_score = cosine_similarity(early_embedding, late_embedding)[0][0]
            
            print("\n--- HYPOTHESIS TEST RESULTS ---")
            print(f"Cosine Similarity between an Early and a Late-Stage Attacker Node: {similarity_score:.4f}")
            if similarity_score > 0.85:
                print("Result: High correlation found. The hypothesis is strongly supported.")
            else:
                print("Result: Low correlation found. The hypothesis is not supported.")

    def run_verification_query(self):
        """Finds and prints example attack paths from the graph."""
        print("\n--- Verifying Attack Paths ---")
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
        print("\n--- Generating Graph Visualization ---")
        query = "MATCH (a:IP)-[r:CONNECTS]->(v:IP) RETURN a.address AS source, v.address AS target, r.is_attack AS is_attack"
        
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
        plt.title("Real Zeek Data Network Graph with Attack Paths Highlighted", size=20)
        plt.savefig("real_attack_graph.png")
        print("Visualization saved to real_attack_graph.png")