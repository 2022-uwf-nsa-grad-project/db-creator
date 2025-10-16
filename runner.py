import pandas as pd
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from neo4j import GraphDatabase
from sklearn.metrics.pairwise import cosine_similarity
import warnings
import os
import subprocess

warnings.filterwarnings('ignore')

# ==============================================================================
# === ANALYSIS & VISUALIZATION FUNCTIONS (Standalone) ==========================
# ==============================================================================

def test_thesis_hypothesis(driver):
    """Runs GDS analysis using the 'tactic' property now in the graph."""
    print("\n--- Running Thesis Hypothesis Test ---")
    with driver.session() as session:
        try:
            session.run("CALL gds.graph.drop('thesisGraph', false)")
        except Exception:
            pass # Graph may not exist on the first run, which is fine.
            
        print("1. Projecting graph into GDS...")
        # **THE FIX**: We remove 'tactic' from the properties list. 
        # 'is_attack' is a boolean, which GDS correctly treats as a number (0 or 1).
        session.run("""
            CALL gds.graph.project(
                'thesisGraph', 
                'IP', 
                {CONNECTS: {properties: 'is_attack'}}
            )
        """)
        
        print("2. Generating node embeddings with FastRP...")
        session.run("CALL gds.fastRP.mutate('thesisGraph', {embeddingDimension: 128, mutateProperty: 'embedding'})")

        # The rest of the function remains the same, as it queries the main database, not GDS.
        print("3. Fetching embeddings for correlation analysis...")
        early_stage_tactics = ['Reconnaissance', 'Initial Access']
        
        early_query = """
        MATCH (a:IP)-[r:CONNECTS]->() WHERE r.tactic IN $tactics
        WITH a LIMIT 1
        CALL gds.util.asNode(id(a)) YIELD embedding
        RETURN embedding
        """
        early_result = session.run(early_query, tactics=early_stage_tactics).single()

        late_query = """
        MATCH (a:IP)-[r:CONNECTS]->() WHERE r.is_attack = true AND NOT r.tactic IN $early_tactics
        WITH a LIMIT 1
        CALL gds.util.asNode(id(a)) YIELD embedding
        RETURN embedding
        """
        late_result = session.run(late_query, early_tactics=early_stage_tactics + ['none', 'Duplicate']).single()
        
        if not early_result or not late_result:
            print("Could not find embeddings for both early and late-stage attacks. Aborting test.")
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
        session.run("CALL gds.graph.drop('thesisGraph', false)")

def run_verification_query(driver):
    """Finds and prints attack paths using the 'is_attack' property."""
    print("\n--- Verifying Attack Paths ---")
    query = """
    MATCH (a:IP)-[r:CONNECTS {is_attack: TRUE}]->(v:IP) 
    WHERE r.tactic <> 'none' AND r.tactic <> 'Duplicate'
    RETURN a.address AS attacker_ip, v.address AS victim_ip, r.port AS port, r.tactic AS tactic
    LIMIT 10
    """
    with driver.session() as session:
        result = session.run(query).data()
        if result:
            for record in result:
                print(f"Attack Detected: {record['attacker_ip']} -> {record['victim_ip']} on port {record['port']} (Tactic: {record['tactic']})")
        else:
            print("Verification Failed: No late-stage attack path found in the graph.")

def visualize_attack_graph(driver):
    """Fetches the graph and highlights nodes involved in attacks."""
    print("\n--- Generating Graph Visualization ---")
    query = "MATCH (a:IP)-[r:CONNECTS]->(v:IP) RETURN a.address AS source, v.address AS target, r.is_attack AS is_attack"
    with driver.session() as session:
        results = session.run(query).data()
    if not results:
        print("No data to visualize."); return
    G = nx.DiGraph()
    attack_edges = set(); attacker_nodes = set(); victim_nodes = set()
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


# ==============================================================================
# === MAIN KILL CHAIN ANALYZER CLASS ===========================================
# ==============================================================================

class KillChainAnalyzer:
    """Orchestrates the entire process of data loading, graph building, and analysis."""
    
    def __init__(self, uri, user, password, container_name):
        self.uri = uri
        self.user = user
        self.password = password
        self.container_name = container_name
        self.driver = None
        self.csv_filename = "zeek_import_data.csv"
        self.csv_filepath = os.path.join(os.getcwd(), self.csv_filename)

    def _get_directory_urls(self, base_url):
        try:
            response = requests.get(base_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            return [urljoin(base_url, link.get('href')) for link in soup.find_all('a') if link.get('href') and link.get('href').startswith('2024') and link.get('href').endswith('/')]
        except requests.exceptions.RequestException as e:
            print(f"Error fetching base URL: {e}")
            return []

    def _prepare_data_for_import(self):
        """Loads data from UWF, cleans it, and saves it to a local CSV file."""
        print("--- Step 1: Loading and Preparing Data ---")
        BASE_URL = "https://datasets.uwf.edu/data/UWF-ZeekData24/parquet/"
        all_dataframes = []
        directory_urls = self._get_directory_urls(BASE_URL)
        for dir_url in directory_urls:
            try:
                response = requests.get(dir_url)
                soup = BeautifulSoup(response.text, 'html.parser')
                parquet_urls = [urljoin(dir_url, link.get('href')) for link in soup.find_all('a') if link.get('href').endswith('.parquet')]
                for url in parquet_urls:
                    all_dataframes.append(pd.read_parquet(url, engine='pyarrow'))
            except Exception as e:
                print(f"Could not process directory {dir_url}: {e}")

        if not all_dataframes:
            print("Data loading failed. Aborting.")
            return False
            
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        cleaned_df = combined_df[combined_df['label_technique'] != 'Duplicate'].copy()
        
        print(f"\nSaving {len(cleaned_df)} rows to local file: {self.csv_filepath}")
        columns_to_export = ['src_ip_zeek', 'dest_ip_zeek', 'ts', 'duration', 'service', 
                             'dest_port_zeek', 'conn_state', 'label_tactic', 'label_technique', 'label_binary']
        cleaned_df[columns_to_export].to_csv(self.csv_filepath, index=False)
        print("--- Data preparation complete! ---")
        return True

    def _copy_csv_to_docker(self):
            """Creates the /var/lib/neo4j/import dir and copies the CSV into it."""
            print("\n--- Step 2: Copying CSV to Docker Container ---")
            
            # **CORRECTED PATH**: The standard, writable location inside the container.
            container_import_dir = "/var/lib/neo4j/import"
            destination = f"{self.container_name}:{container_import_dir}/{self.csv_filename}"
            
            try:
                # Step 2a: Ensure the standard import directory exists.
                print(f"Ensuring {container_import_dir} directory exists in '{self.container_name}'...")
                subprocess.run(
                    ['docker', 'exec', self.container_name, 'mkdir', '-p', container_import_dir],
                    check=True, capture_output=True
                )

                # Step 2b: Copy the file to the correct location.
                print(f"Executing: docker cp {self.csv_filepath} {destination}")
                subprocess.run(['docker', 'cp', self.csv_filepath, destination], check=True, capture_output=True)
                
                print("--- File copied successfully! ---")
                return True
                
            except subprocess.CalledProcessError as e:
                print("\nERROR: A Docker command failed.")
                print("Please ensure that:")
                print(f"1. Docker is running.")
                print(f"2. The container named '{self.container_name}' is running.")
                print(f"3. You have permissions to run Docker commands.")
                print(f"\nCommand Stderr: {e.stderr.decode().strip()}")
                return False
            
    def _build_graph_from_csv(self):
            """Builds the Neo4j graph using an index for massive performance gains."""
            print("\n--- Step 3: Building Graph with LOAD CSV ---")
            with self.driver.session() as session:
                # Step 3a: Clear the database as before.
                session.run("MATCH (n) DETACH DELETE n")
                print("Database cleared.")

                # **THE FIX: Create an index on IP addresses BEFORE loading data.**
                print("Creating index for IP addresses... This will make the import much faster.")
                session.run("CREATE INDEX ip_address_index IF NOT EXISTS FOR (n:IP) ON (n.address)")

                # Step 3b: Run the LOAD CSV command, which will now use the index.
                # We can even try increasing the batch size again now that it's more memory-efficient.
                load_query = f"""
                LOAD CSV WITH HEADERS FROM 'file:///{self.csv_filename}' AS row
                CALL (row) {{
                    MERGE (orig:IP {{address: row.src_ip_zeek}})
                    MERGE (resp:IP {{address: row.dest_ip_zeek}})
                    CREATE (orig)-[:CONNECTS {{
                        timestamp: toFloat(row.ts), duration: toFloat(row.duration), service: row.service,
                        port: toInteger(row.dest_port_zeek), state: row.conn_state, tactic: row.label_tactic,
                        technique: row.label_technique, is_attack: toBoolean(row.label_binary)
                    }}]->(resp)
                }} IN TRANSACTIONS OF 50000 ROWS
                """

                print("Executing LOAD CSV query. This will be much faster now...")
                session.run(load_query)
                print("--- Graph construction complete! ---")

    def run_full_process(self):
        """Executes the entire data pipeline and analysis."""
        # Step 1: Prepare data
        if not self._prepare_data_for_import():
            return
        
        # Step 2: Copy file to container
        if not self._copy_csv_to_docker():
            return

        # Step 3 & 4: Connect, build graph, and run analysis
        try:
            self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
            self.driver.verify_connectivity()
            
            self._build_graph_from_csv()
            
            print("\n--- Step 4: Running Analysis Pipeline ---")
            test_thesis_hypothesis(self.driver)
            run_verification_query(self.driver)
            visualize_attack_graph(self.driver)

        except Exception as e:
            print(f"\nAn error occurred during Neo4j operations: {e}")
        finally:
            if self.driver:
                self.driver.close()
                print("\nNeo4j driver closed.")

# ==============================================================================
# === MAIN EXECUTION BLOCK =====================================================
# ==============================================================================

if __name__ == "__main__":
    # --- CONFIGURATION ---
    NEO4J_URI = "bolt://localhost:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "ubuntuubuntu"  # <-- IMPORTANT: SET YOUR NEO4J PASSWORD
    CONTAINER_NAME = "neo4j_thesis_server" 

    # --- EXECUTION ---
    analyzer = KillChainAnalyzer(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, CONTAINER_NAME)
    analyzer.run_full_process()