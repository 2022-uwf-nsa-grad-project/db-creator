#!/usr/bin/env python3
"""
Database Exploration Script

This script analyzes the Neo4j database to understand attack patterns,
identify true pivot behaviors, and provide recommendations for analysis.

Usage:
    python explore_database.py
"""

from CART.analyzers import SubnetPivotAnalyzer

def main():
    print("="*80)
    print("DATABASE EXPLORATION FOR PIVOT DETECTION")
    print("="*80)
    print("\nThis script will:")
    print("  1. Analyze attack label distribution")
    print("  2. Identify reconnaissance patterns")
    print("  3. Find lateral movement attacks")
    print("  4. Detect true attack chains (Recon → Pivot)")
    print("  5. Analyze cross-subnet traffic")
    print("  6. Provide recommendations for pivot detection")
    print("\nConnecting to database...")
    
    # Initialize analyzer
    analyzer = SubnetPivotAnalyzer(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="ubuntuubuntu"
    )
    
    # Run exploration
    results = analyzer.explore_database(output_file="database_exploration.json")
    
    print("\n" + "="*80)
    print("EXPLORATION COMPLETE")
    print("="*80)
    print("\nNext steps:")
    print("  1. Review the output above")
    print("  2. Check database_exploration.json for detailed results")
    print("  3. Run your pivot analysis with the improved detection logic")
    print("\nExample:")
    print("  analyzer = SubnetPivotAnalyzer()")
    print("  analyzer.run_full_analysis(")
    print("      mode='both',")
    print("      historical_window_hours=24,")
    print("      detection_window_hours=24,")
    print("      embedding_dim=128")
    print("  )")

if __name__ == "__main__":
    main()
