from neo4j import GraphDatabase
import json
import time

class Neo4jConnection:
    """
    A base class to handle Neo4j database connections.
    
    This class manages the driver, connection, and closing logic,
    allowing other analyzer classes to inherit these capabilities.
    """
    def __init__(self, uri, user, password, database="neo4j"):
        """
        Initializes the connection parameters.
        
        Args:
            uri (str): The Bolt URI for the Neo4j instance.
            user (str): The username for the Neo4j instance.
            password (str): The password for the Neo4j instance.
            database (str): The database name.
        """
        self.uri = uri
        self.user = user
        self.password = password
        self.database = database
        self.driver = None
        print(f"📦 {self.__class__.__name__} initialized.")

    def connect(self):
        """Establishes a connection to the Neo4j database."""
        if self.driver is None:
            try:
                self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
                self.driver.verify_connectivity()
                print("✓ Successfully connected to the Neo4j database.")
            except Exception as e:
                print(f"❌ Failed to connect to Neo4j: {e}")
                self.driver = None
        return self.driver is not None

    def close(self):
        """Closes the connection to the Neo4j database."""
        if self.driver is not None:
            self.driver.close()
            self.driver = None
            print("✓ Neo4j connection closed.")




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
            # self.connect() is inherited from the parent
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