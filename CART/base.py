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