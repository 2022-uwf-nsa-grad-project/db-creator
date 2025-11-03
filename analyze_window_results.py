#!/usr/bin/env python3
"""
Analyze Window Optimization Results

Compares all the test results to find the best window configuration.
"""

import pandas as pd
import glob
import json

def analyze_results():
    print("="*80)
    print("WINDOW OPTIMIZATION RESULTS ANALYSIS")
    print("="*80)
    
    # Find all method comparison files
    comparison_files = sorted(glob.glob('test_h*_d*_method_comparison.csv'))
    
    if not comparison_files:
        print("\n⚠ No result files found!")
        print("Run optimize_windows.py first.")
        return
    
    print(f"\nFound {len(comparison_files)} result files\n")
    
    all_results = []
    
    for filepath in comparison_files:
        # Extract window sizes from filename
        # Format: test_h{hist}_d{det}_method_comparison.csv
        parts = filepath.split('_')
        hist_hours = int(parts[1][1:])  # Remove 'h' prefix
        det_hours = int(parts[2][1:])   # Remove 'd' prefix
        
        try:
            df = pd.read_csv(filepath)
            
            # Get FastRP results
            fastrp_row = df[df['Method'] == 'FastRP Embedding'].iloc[0]
            
            # Also get prediction file for pivot rate
            pred_file = filepath.replace('_method_comparison.csv', '_pivot_predictions.csv')
            pred_df = pd.read_csv(pred_file)
            pivot_rate = pred_df['became_pivot'].mean() * 100
            total_samples = len(pred_df)
            pivot_count = pred_df['became_pivot'].sum()
            
            all_results.append({
                'Historical (h)': hist_hours,
                'Detection (h)': det_hours,
                'Total Samples': total_samples,
                'Pivot Count': pivot_count,
                'Pivot Rate (%)': pivot_rate,
                'AUC-ROC': fastrp_row['AUC-ROC'],
                'AUC-PR': fastrp_row['AUC-PR'],
                'Accuracy': fastrp_row['Accuracy'],
                'Precision': fastrp_row['Precision'],
                'Recall': fastrp_row['Recall'],
                'F1-Score': fastrp_row['F1-Score']
            })
            
        except Exception as e:
            print(f"⚠ Error processing {filepath}: {e}")
    
    if not all_results:
        print("\n⚠ No valid results found!")
        return
    
    # Create results dataframe
    results_df = pd.DataFrame(all_results)
    results_df = results_df.sort_values('F1-Score', ascending=False)
    
    print("\n" + "="*80)
    print("RESULTS RANKED BY F1-SCORE")
    print("="*80 + "\n")
    print(results_df.to_string(index=False))
    
    # Save to CSV
    results_df.to_csv('window_optimization_comparison.csv', index=False)
    print(f"\n✓ Saved to window_optimization_comparison.csv")
    
    # Recommendations
    print("\n" + "="*80)
    print("RECOMMENDATIONS")
    print("="*80)
    
    # Best overall
    best_row = results_df.iloc[0]
    print(f"\n1. BEST OVERALL (F1-Score):")
    print(f"   Historical: {best_row['Historical (h)']}h, Detection: {best_row['Detection (h)']}h")
    print(f"   Pivot Rate: {best_row['Pivot Rate (%)']:.1f}%")
    print(f"   AUC-ROC: {best_row['AUC-ROC']:.4f}")
    print(f"   F1-Score: {best_row['F1-Score']:.4f}")
    
    # Best balanced (30-70% pivot rate)
    balanced = results_df[
        (results_df['Pivot Rate (%)'] >= 30) & 
        (results_df['Pivot Rate (%)'] <= 70)
    ]
    if not balanced.empty:
        best_balanced = balanced.iloc[0]
        print(f"\n2. BEST BALANCED (30-70% pivot rate):")
        print(f"   Historical: {best_balanced['Historical (h)']}h, Detection: {best_balanced['Detection (h)']}h")
        print(f"   Pivot Rate: {best_balanced['Pivot Rate (%)']:.1f}%")
        print(f"   AUC-ROC: {best_balanced['AUC-ROC']:.4f}")
        print(f"   F1-Score: {best_balanced['F1-Score']:.4f}")
    else:
        print(f"\n2. BEST BALANCED: None found with 30-70% pivot rate")
    
    # Best AUC-ROC
    best_auc = results_df.sort_values('AUC-ROC', ascending=False).iloc[0]
    print(f"\n3. BEST DISCRIMINATION (AUC-ROC):")
    print(f"   Historical: {best_auc['Historical (h)']}h, Detection: {best_auc['Detection (h)']}h")
    print(f"   Pivot Rate: {best_auc['Pivot Rate (%)']:.1f}%")
    print(f"   AUC-ROC: {best_auc['AUC-ROC']:.4f}")
    print(f"   F1-Score: {best_auc['F1-Score']:.4f}")
    
    # Analysis insights
    print("\n" + "="*80)
    print("INSIGHTS")
    print("="*80)
    
    # Pivot rate analysis
    avg_pivot_rate = results_df['Pivot Rate (%)'].mean()
    print(f"\n• Average pivot rate: {avg_pivot_rate:.1f}%")
    
    if avg_pivot_rate > 80:
        print("  ⚠ Still too high - consider stricter pivot criteria")
    elif avg_pivot_rate < 20:
        print("  ⚠ Too low - may be too strict, missing pivots")
    else:
        print("  ✓ Good balance - machine learning should work well")
    
    # AUC-ROC analysis
    avg_auc = results_df['AUC-ROC'].mean()
    print(f"\n• Average AUC-ROC: {avg_auc:.4f}")
    
    if avg_auc > 0.7:
        print("  ✓ Good discrimination - embeddings are working!")
    elif avg_auc > 0.5:
        print("  ~ Moderate discrimination - embeddings partially working")
    else:
        print("  ⚠ Poor discrimination - embeddings not capturing pivot behavior")
    
    # Window size patterns
    short_windows = results_df[results_df['Detection (h)'] <= 6]
    long_windows = results_df[results_df['Detection (h)'] >= 24]
    
    if not short_windows.empty and not long_windows.empty:
        short_avg_f1 = short_windows['F1-Score'].mean()
        long_avg_f1 = long_windows['F1-Score'].mean()
        
        print(f"\n• Short windows (≤6h): avg F1={short_avg_f1:.4f}")
        print(f"• Long windows (≥24h): avg F1={long_avg_f1:.4f}")
        
        if short_avg_f1 > long_avg_f1:
            print("  → Short windows perform better (matches quick pivot timing!)")
        else:
            print("  → Long windows perform better (need more context)")

if __name__ == "__main__":
    analyze_results()
