from .analyzers import *
from .reporting import *

class AnalysisController:
    """
    A single controller to manage analysis configuration and provide
    access to various analyzer tools.

    Initialize this class once with your Neo4j credentials, then use it
    to create configured instances of the analyzer classes without repeating
    the credentials.
    """
    def __init__(self, uri, user, password, database="neo4j"):
        """Stores the database configuration for the entire session."""
        self.uri = uri
        self.user = user
        self.password = password
        self.database = database
        print("✅ Analysis Controller is configured and ready.")

    def create_thesis_analyzer(self):
        """Creates a pre-configured instance of ThesisAnalyzer."""
        return ThesisAnalyzer(self.uri, self.user, self.password, self.database)
        
    def create_kill_chain_analyzer(self):
        """Creates a pre-configured instance of KillChainAnalyzer."""
        return KillChainAnalyzer(self.uri, self.user, self.password, self.database)

    def create_structural_analyzer(self):
        """Creates a pre-configured instance of StructuralPivotAnalyzer."""
        return StructuralPivotAnalyzer(self.uri, self.user, self.password, self.database)
    
    def create_report_generator(self, filepath):
        """Creates a pre-configured instance of ReportGenerator."""
        return ReportGenerator(filepath)