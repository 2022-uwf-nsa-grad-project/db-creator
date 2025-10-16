# thesis_analyzer.py
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
import subprocess
import warnings

warnings.filterwarnings('ignore')

class ThesisAnalyzer:
    """
    A comprehensive class to manage the entire data pipeline for the thesis project.
    
    This class handles:
    1. Downloading and preparing the real-world Zeek data.
    2. Building a Neo4j graph database within a specified Docker container.
    3. Running GDS analysis and other queries on the constructed database.
    
    Designed to be imported and used from a Jupyter Notebook.
    """
    
    def __init__(self, uri, user, password, container_name):
        """
        Initializes the analyzer with database and Docker credentials.
        
        Args:
            uri (str): The Bolt URI for the Neo4j instance.
            user (str): The username for the Neo4j instance.
            password (str): The password for the Neo4j instance.
            container_name (str): The name of the running Docker container.
        """
        self.uri = uri
        self.user = user
        self.password = password
        self.container_name = container_name
        self.driver = None
        self.csv_filename = "zeek_import_data.csv"
        self.csv_filepath = os.path.join(os.getcwd(), self.csv_filename)

    def connect(self):
        """Establishes a connection to the Neo4j database."""
        if self.driver is None or not self.driver.verify_connectivity():
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
        if not self._prepare_data_for_import(): return
        if not self._copy_csv_to_docker(): return
        
        try:
            self.connect()
            if self.driver:
                self._build_graph_from_csv(rebuild=rebuild)
                print("\nDatabase build process finished successfully.")
        finally:
            self.close()

    def _prepare_data_for_import(self):
        """[Private] Downloads, cleans, and saves data to a local CSV."""
        print("Step 1: Loading and Preparing Data...")
        BASE_URL = "https://datasets.uwf.edu/data/UWF-ZeekData24/parquet/"
        all_dataframes = []
        try:
            directory_urls = self._get_directory_urls(BASE_URL)
            for dir_url in directory_urls:
                response = requests.get(dir_url)
                soup = BeautifulSoup(response.text, 'html.parser')
                parquet_urls = [urljoin(dir_url, link.get('href')) for link in soup.find_all('a') if link.get('href').endswith('.parquet')]
                for url in parquet_urls:
                    all_dataframes.append(pd.read_parquet(url, engine='pyarrow'))
            
            if not all_dataframes: raise ValueError("No dataframes were loaded.")
            
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            cleaned_df = combined_df[combined_df['label_technique'] != 'Duplicate'].copy()
            
            print(f"Saving {len(cleaned_df)} rows to local file: {self.csv_filepath}")
            columns_to_export = ['src_ip_zeek', 'dest_ip_zeek', 'ts', 'duration', 'service', 
                                 'dest_port_zeek', 'conn_state', 'label_tactic', 'label_technique', 'label_binary']
            cleaned_df[columns_to_export].to_csv(self.csv_filepath, index=False)
            print("Data preparation complete.")
            return True
        except Exception as e:
            print(f"Error during data preparation: {e}")
            return False

    def _get_directory_urls(self, base_url):
        """[Private] Helper to fetch subdirectory URLs."""
        response = requests.get(base_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        return [urljoin(base_url, link.get('href')) for link in soup.find_all('a') if link.get('href') and link.get('href').startswith('2024') and link.get('href').endswith('/')]

    def _copy_csv_to_docker(self):
        """[Private] Copies the CSV into the Docker container."""
        print("\nStep 2: Copying CSV to Docker Container...")
        container_import_dir = "/var/lib/neo4j/import"
        destination = f"{self.container_name}:{container_import_dir}/{self.csv_filename}"
        try:
            print(f"Ensuring {container_import_dir} directory exists in '{self.container_name}'...")
            
            exec_command = ['sudo', '-S', 'docker', 'exec', self.container_name, 'mkdir', '-p', container_import_dir]
            cp_command = ['sudo', '-S', 'docker', 'cp', self.csv_filepath, destination]
            

            print(f"Executing: docker cp {self.csv_filepath} {destination}")
            subprocess.run(
                cp_command,
                input=('ubuntu' + '\n').encode(), # Encode the password and add a newline
                check=True,
                capture_output=True
            )

            print("File copied successfully.")
            return True
        except subprocess.CalledProcessError as e:
            print(f"ERROR: A Docker command failed. Stderr: {e.stderr.decode().strip()}")
            return False

    def _build_graph_from_csv(self, rebuild):
        """[Private] Wipes DB (conditionally), creates index, and loads data."""
        print("\nStep 3: Building Graph with LOAD CSV...")
        with self.driver.session() as session:
            if rebuild:
                print("Rebuild flag is True. Wiping existing database...")
                session.run("MATCH (n) DETACH DELETE n")
            else:
                print("Rebuild flag is False. Skipping database wipe.")
            
            print("Ensuring index exists for IP addresses for high performance...")
            session.run("CREATE INDEX ip_address_index IF NOT EXISTS FOR (n:IP) ON (n.address)")

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
            print("Executing LOAD CSV query. This may take several minutes...")
            session.run(load_query)
            print("Graph data loading complete.")

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
        with self.driver.session() as session:
            try: session.run("CALL gds.graph.drop('thesisGraph', false)")
            except Exception: pass
            
            print("1. Projecting graph into GDS...")
            session.run("CALL gds.graph.project('thesisGraph', 'IP', {CONNECTS: {properties: 'is_attack'}})")
            
            print("2. Generating node embeddings with FastRP...")
            session.run("CALL gds.fastRP.mutate('thesisGraph', {embeddingDimension: 128, mutateProperty: 'embedding'})")

            print("3. Fetching embeddings for correlation analysis...")
            early_stage_tactics = ['Reconnaissance', 'Initial Access']
            early_query = "MATCH (a:IP)-[r:CONNECTS]->() WHERE r.tactic IN $tactics WITH a LIMIT 1 CALL gds.util.asNode(id(a)) YIELD embedding RETURN embedding"
            late_query = "MATCH (a:IP)-[r:CONNECTS]->() WHERE r.is_attack = true AND NOT r.tactic IN $early_tactics WITH a LIMIT 1 CALL gds.util.asNode(id(a)) YIELD embedding RETURN embedding"
            
            early_result = session.run(early_query, tactics=early_stage_tactics).single()
            late_result = session.run(late_query, early_tactics=early_stage_tactics + ['none', 'Duplicate']).single()
            
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
        query = "MATCH (a:IP)-[r:CONNECTS {is_attack: TRUE}]->(v:IP) WHERE r.tactic <> 'none' AND r.tactic <> 'Duplicate' RETURN a.address AS attacker_ip, v.address AS victim_ip, r.port AS port, r.tactic AS tactic LIMIT 10"
        with self.driver.session() as session:
            result = session.run(query).data()
            if result:
                for record in result:
                    print(f"Attack Detected: {record['attacker_ip']} -> {record['victim_ip']} on port {record['port']} (Tactic: {record['tactic']})")

    def visualize_attack_graph(self):
        """Generates and saves a PNG visualization of the attack graph."""
        print("\n--- Generating Graph Visualization ---")
        query = "MATCH (a:IP)-[r:CONNECTS]->(v:IP) RETURN a.address AS source, v.address AS target, r.is_attack AS is_attack"
        with self.driver.session() as session:
            results = session.run(query).data()
        if not results: print("No data to visualize."); return
        G = nx.DiGraph()
        attack_edges = set(); attacker_nodes = set()
        for record in results:
            G.add_edge(record['source'], record['target'])
            if record['is_attack']:
                attack_edges.add((record['source'], record['target']))
                attacker_nodes.add(record['source'])
        node_colors = ['red' if node in attacker_nodes else 'skyblue' for node in G.nodes()]
        edge_colors = ['red' if edge in attack_edges else 'lightgray' for edge in G.edges()]
        plt.figure(figsize=(20, 16))
        pos = nx.spring_layout(G, k=0.7, iterations=40)
        nx.draw(G, pos, with_labels=True, node_color=node_colors, edge_color=edge_colors, node_size=1500, font_size=8, width=0.8, arrows=True)
        plt.title("Real Zeek Data Network Graph with Attack Paths Highlighted", size=20)
        plt.savefig("real_attack_graph.png")
        print("Visualization saved to real_attack_graph.png")


