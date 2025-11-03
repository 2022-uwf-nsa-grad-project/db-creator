#!/usr/bin/env python3
"""
Window Optimization Experiments

Based on exploration results:
- Time to pivot: 0.1-1.5 hours
- Need to test different window sizes

This script tests multiple window configurations to find optimal settings.
"""

from CART.analyzers import SubnetPivotAnalyzer
import json
import time

def test_window_configuration(hist_window_hours, det_window_hours):
    """Test a specific window configuration."""
    print("\n" + "="*80)
    print(f"TESTING: Historical={hist_window_hours}h, Detection={det_window_hours}h")
    print("="*80)
    
    analyzer = SubnetPivotAnalyzer()
    
    try:
        analyzer.connect()
        
        # Just test label-aware mode for speed
        analyzer.run_pivot_prediction(
            use_labels=True,
            historical_window_hours=hist_window_hours,
            detection_window_hours=det_window_hours,
            embedding_dim=128,
            output_prefix=f'test_h{hist_window_hours}_d{det_window_hours}'
        )
        
        print(f"\n✓ Completed test for h={hist_window_hours}, d={det_window_hours}")
        return True
        
    except Exception as e:
        print(f"\n✗ Failed: {e}")
        return False
    finally:
        analyzer.close()

def main():
    print("="*80)
    print("WINDOW OPTIMIZATION EXPERIMENTS")
    print("="*80)
    print("\nBased on exploration results:")
    print("  - Time to pivot: 0.1-1.5 hours (median ~0.5h)")
    print("  - Attack chains found: 12")
    print("  - All data in single time bucket (snapshot dataset)")
    print("\nStrategy:")
    print("  - Start with SHORT windows (matches observed timing)")
    print("  - Test progressively longer windows")
    print("  - Find balance between recall and precision")
    
    # Window configurations to test (historical_hours, detection_hours)
    # Based on exploration: pivots happen 0.1-1.5 hours after recon
    configs = [
        # Short windows - match observed timing
        (1, 2),    # Very short - capture immediate pivots
        (2, 3),    # Short - within observed range
        (6, 6),    # Medium-short
        (12, 12),  # Medium
        (24, 24),  # Long - original setting
        (48, 48),  # Very long - capture delayed pivots
        
        # Asymmetric windows
        (6, 12),   # Short history, longer detection
        (12, 24),  # Medium history, long detection
        (24, 6),   # Long history, short detection
    ]
    
    results_summary = []
    
    print(f"\n\nWill test {len(configs)} configurations...")
    input("Press Enter to start (this will take several minutes)...")
    
    for i, (hist_h, det_h) in enumerate(configs, 1):
        print(f"\n\n{'='*80}")
        print(f"EXPERIMENT {i}/{len(configs)}")
        print(f"{'='*80}")
        
        start_time = time.time()
        success = test_window_configuration(hist_h, det_h)
        elapsed = time.time() - start_time
        
        results_summary.append({
            'historical_hours': hist_h,
            'detection_hours': det_h,
            'success': success,
            'elapsed_seconds': elapsed
        })
        
        print(f"\nElapsed time: {elapsed:.1f} seconds")
        
        # Brief pause between experiments
        if i < len(configs):
            time.sleep(2)
    
    # Print summary
    print("\n\n" + "="*80)
    print("EXPERIMENT SUMMARY")
    print("="*80)
    
    for result in results_summary:
        status = "✓" if result['success'] else "✗"
        print(f"{status} h={result['historical_hours']:2d}, d={result['detection_hours']:2d} "
              f"- {result['elapsed_seconds']:.1f}s")
    
    # Save summary
    with open('window_optimization_results.json', 'w') as f:
        json.dump(results_summary, f, indent=2)
    
    print("\n✓ Results saved to window_optimization_results.json")
    print("\nNow review the generated CSV files:")
    print("  - test_h*_d*_pivot_predictions.csv")
    print("  - test_h*_d*_method_comparison.csv")
    print("\nLook for:")
    print("  1. Balanced pivot rate (30-70%)")
    print("  2. High AUC-ROC (>0.7)")
    print("  3. Statistical significance (p < 0.05)")
    print("  4. Good F1-score")
    
    print("\n" + "="*80)
    print("RECOMMENDATIONS")
    print("="*80)
    print("\nBased on exploration (pivots at 0.1-1.5 hours):")
    print("  - SHORT windows (2-6 hours) should work best")
    print("  - May need asymmetric (longer detection than history)")
    print("  - Compare results to find optimal balance")

if __name__ == "__main__":
    main()
