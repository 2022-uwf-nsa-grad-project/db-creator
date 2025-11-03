#!/usr/bin/env python3
"""
Quick Test: Verify Fixed Pivot Detection

Tests the improved pivot detection with expanded tactics.
"""

from CART.analyzers import SubnetPivotAnalyzer

def main():
    print("="*80)
    print("QUICK TEST: Fixed Pivot Detection")
    print("="*80)
    print("\nThis will run ONE analysis to verify the fix works.")
    print("\nBased on exploration:")
    print("  • 12 attack chains found")
    print("  • Pivot timing: 0.1-1.5 hours")
    print("  • Main tactic: Credential Access")
    print("\nExpected results:")
    print("  • Pivot rate: 20-50% (not 99.9%!)")
    print("  • More balanced classes")
    print("  • Better statistical significance")
    
    input("\nPress Enter to start...")
    
    analyzer = SubnetPivotAnalyzer()
    
    # Test with SHORT window (matches observed timing)
    print("\n" + "="*80)
    print("Testing with 6-hour windows (matches pivot timing)")
    print("="*80)
    
    analyzer.run_full_analysis(
        mode='label_aware',  # Just test one mode for speed
        historical_window_hours=6,
        detection_window_hours=6,
        embedding_dim=128
    )
    
    print("\n" + "="*80)
    print("TEST COMPLETE")
    print("="*80)
    print("\nCheck the output above for:")
    print("  ✓ Pivot rate around 20-50% (not 99.9%)")
    print("  ✓ AUC-ROC > 0.5 (better than random)")
    print("  ✓ Statistical significance (p-value)")
    print("\nFiles generated:")
    print("  • label_aware_pivot_predictions.csv")
    print("  • label_aware_method_comparison.csv")
    print("  • label_aware_results.png")
    print("\nIf results look good, run full window optimization:")
    print("  python optimize_windows.py")

if __name__ == "__main__":
    main()
