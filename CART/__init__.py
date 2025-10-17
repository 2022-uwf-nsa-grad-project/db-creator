"""
Cyber Analytics Research Thesis
A comprehensive toolkit for building and analyzing network graphs for threat intelligence.
"""

# Import classes from sub-modules to make them directly accessible at the package level.
# This allows users to write `from CART import ThesisAnalyzer` instead of
# `from CART.analyzers import ThesisAnalyzer`.

from .base import Neo4jConnection
from .analyzers import ThesisAnalyzer, KillChainAnalyzer, StructuralPivotAnalyzer
from .reporting import ReportGenerator
from .controller import AnalysisController

# The __all__ variable explicitly defines the public API of the package.
# It specifies which names are imported when a user does `from CART import *`.
__all__ = [
    'Neo4jConnection',
    'ThesisAnalyzer',
    'KillChainAnalyzer',
    'StructuralPivotAnalyzer',
    'ReportGenerator',
    'AnalysisController'
]