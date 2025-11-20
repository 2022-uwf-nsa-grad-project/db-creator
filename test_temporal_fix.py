#!/usr/bin/env python3
"""
Test script to verify temporal leakage fix.

This script runs the pivot prediction pipeline in both modes:
1. WITH temporal filtering (causal, no leakage)
2. WITHOUT temporal filtering (original behavior, has leakage)

It then compares the results to quantify the performance impact of removing leakage.
"""

from CART import SubnetPivotAnalyzer
import pandas as pd
import json
from datetime import datetime

def run_comparison_test():
    """Run pivot prediction with and without temporal filtering."""
    
    print("="*80)
    print("TEMPORAL LEAKAGE FIX VALIDATION TEST")
    print("="*80)
    print()
    print("This test will run the pivot prediction pipeline twice:")
    print("  1. WITH temporal filtering (causal prediction, no leakage)")
    print("  2. WITHOUT temporal filtering (original behavior, has leakage)")
    print()
    print("Expected outcome:")
    print("  - Mode 1 should have LOWER AUC-ROC (more realistic)")
    print("  - Mode 2 should match previous results (AUC-ROC ~0.615)")
    print("="*80)
    print()
    
    analyzer = SubnetPivotAnalyzer()
    
    try:
        analyzer.connect()
        analyzer.add_subnet_labels()
        
        # Test 1: WITH temporal filtering (CAUSAL)
        print("\n" + "="*80)
        print("TEST 1: CAUSAL MODE (Temporal Filtering ENABLED)")
        print("="*80)
        
        analyzer.run_pivot_prediction(
            use_labels=True,
            historical_window_hours=48,
            detection_window_hours=24,
            embedding_dim=128,
            output_prefix="temporal_filtered_test",
            enable_temporal_filtering=True  # NEW: Eliminate leakage
        )
        
        # Read results
        try:
            df_causal = pd.read_csv('temporal_filtered_test_method_comparison.csv')
            fastRP_causal = df_causal[df_causal['Method'] == 'FastRP Embedding'].iloc[0]
            auc_roc_causal = fastRP_causal['AUC-ROC']
            cohens_d_causal = fastRP_causal['cohens_d']
            
            print(f"\n✓ CAUSAL MODE RESULTS:")
            print(f"  AUC-ROC: {auc_roc_causal:.4f}")
            print(f"  Cohen's d: {cohens_d_causal:.4f}")
        except Exception as e:
            print(f"⚠ Could not read causal results: {e}")
            auc_roc_causal = None
            cohens_d_causal = None
        
        # Test 2: WITHOUT temporal filtering (LEAKAGE)
        print("\n" + "="*80)
        print("TEST 2: LEAKAGE MODE (Temporal Filtering DISABLED)")
        print("="*80)
        
        analyzer.run_pivot_prediction(
            use_labels=True,
            historical_window_hours=48,
            detection_window_hours=24,
            embedding_dim=128,
            output_prefix="temporal_leakage_test",
            enable_temporal_filtering=False  # Original behavior
        )
        
        # Read results
        try:
            df_leakage = pd.read_csv('temporal_leakage_test_method_comparison.csv')
            fastRP_leakage = df_leakage[df_leakage['Method'] == 'FastRP Embedding'].iloc[0]
            auc_roc_leakage = fastRP_leakage['AUC-ROC']
            cohens_d_leakage = fastRP_leakage['cohens_d']
            
            print(f"\n✓ LEAKAGE MODE RESULTS:")
            print(f"  AUC-ROC: {auc_roc_leakage:.4f}")
            print(f"  Cohen's d: {cohens_d_leakage:.4f}")
        except Exception as e:
            print(f"⚠ Could not read leakage results: {e}")
            auc_roc_leakage = None
            cohens_d_leakage = None
        
        # Comparison
        print("\n" + "="*80)
        print("COMPARISON AND VALIDATION")
        print("="*80)
        
        if auc_roc_causal is not None and auc_roc_leakage is not None:
            diff = auc_roc_leakage - auc_roc_causal
            pct_drop = (diff / auc_roc_leakage) * 100
            
            print(f"\nAUC-ROC Comparison:")
            print(f"  Causal (filtered):  {auc_roc_causal:.4f}")
            print(f"  Leakage (original): {auc_roc_leakage:.4f}")
            print(f"  Difference:         {diff:+.4f} ({pct_drop:+.1f}%)")
            
            if diff > 0.02:
                print(f"\n✓ VALIDATION SUCCESSFUL:")
                print(f"  - Removing temporal leakage reduced AUC-ROC by {diff:.4f}")
                print(f"  - This confirms the original results were inflated by future information")
                print(f"  - The causal AUC-ROC ({auc_roc_causal:.4f}) is the TRUE predictive performance")
            elif diff > 0:
                print(f"\n⚠ VALIDATION INCONCLUSIVE:")
                print(f"  - Small difference ({diff:.4f}) suggests minimal leakage impact")
                print(f"  - Or the temporal structure is truly predictive")
            else:
                print(f"\n⚠ UNEXPECTED RESULT:")
                print(f"  - Causal mode has HIGHER AUC-ROC than leakage mode")
                print(f"  - This should not happen - investigate data or code")
            
            print(f"\nCohen's d Comparison:")
            print(f"  Causal (filtered):  {cohens_d_causal:.4f}")
            print(f"  Leakage (original): {cohens_d_leakage:.4f}")
            print(f"  Difference:         {cohens_d_leakage - cohens_d_causal:+.4f}")
            
            # Save comparison report
            report = {
                'timestamp': datetime.now().isoformat(),
                'causal_mode': {
                    'auc_roc': float(auc_roc_causal),
                    'cohens_d': float(cohens_d_causal),
                    'temporal_filtering': True
                },
                'leakage_mode': {
                    'auc_roc': float(auc_roc_leakage),
                    'cohens_d': float(cohens_d_leakage),
                    'temporal_filtering': False
                },
                'impact': {
                    'auc_roc_difference': float(diff),
                    'auc_roc_percent_drop': float(pct_drop),
                    'conclusion': 'Causal mode provides unbiased estimates' if diff > 0.02 else 'Minimal leakage impact'
                }
            }
            
            with open('temporal_leakage_validation_report.json', 'w') as f:
                json.dump(report, f, indent=2)
            
            print(f"\n✓ Validation report saved to: temporal_leakage_validation_report.json")
        
    finally:
        analyzer.close()
    
    print("\n" + "="*80)
    print("TEST COMPLETE")
    print("="*80)

if __name__ == "__main__":
    run_comparison_test()
