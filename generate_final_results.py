#!/usr/bin/env python3
"""
Generate Final Thesis Results

Runs the complete analysis with optimal settings and generates
comprehensive results for thesis including:
- Label-aware analysis (using ATT&CK tactics)
- Label-agnostic analysis (structural patterns only)
- Statistical comparisons
- Baseline method comparisons
"""

from CART.analyzers import SubnetPivotAnalyzer
import pandas as pd
import time

def main():
    print("="*80)
    print("GENERATING FINAL THESIS RESULTS")
    print("="*80)
    print("\nDataset: UWF-ZeekData24 with MITRE ATT&CK labels")
    print("Task: Predicting Lateral Movement Pivots")
    print("Method: Subnet-Aware FastRP Embeddings")
    print("\nConfigurations:")
    print("  - Embedding dimension: 128")
    print("  - Historical window: 24 hours")
    print("  - Detection window: 24 hours")
    print("  - Modes: Label-aware AND Label-agnostic")
    
    analyzer = SubnetPivotAnalyzer()
    
    try:
        print("\n" + "="*80)
        print("CONNECTING TO DATABASE")
        print("="*80)
        analyzer.connect()
        print("✓ Connected successfully")
        
        # ==============================================================
        # RUN 1: Label-Aware Analysis (Uses ATT&CK Tactics)
        # ==============================================================
        print("\n\n" + "="*80)
        print("PART 1: LABEL-AWARE ANALYSIS")
        print("="*80)
        print("\nUsing MITRE ATT&CK tactic labels for enhanced embeddings")
        print("Expected: Better discrimination due to semantic information")
        
        start_time = time.time()
        
        analyzer.run_pivot_prediction(
            use_labels=True,
            historical_window_hours=24,
            detection_window_hours=24,
            embedding_dim=128,
            output_prefix='final_label_aware'
        )
        
        elapsed1 = time.time() - start_time
        print(f"\n✓ Label-aware analysis completed in {elapsed1:.1f} seconds")
        
        # ==============================================================
        # RUN 2: Label-Agnostic Analysis (Structure Only)
        # ==============================================================
        print("\n\n" + "="*80)
        print("PART 2: LABEL-AGNOSTIC ANALYSIS")
        print("="*80)
        print("\nUsing only graph structure (no ATT&CK labels)")
        print("Expected: Demonstrates contribution of label information")
        
        start_time = time.time()
        
        analyzer.run_pivot_prediction(
            use_labels=False,
            historical_window_hours=24,
            detection_window_hours=24,
            embedding_dim=128,
            output_prefix='final_label_agnostic'
        )
        
        elapsed2 = time.time() - start_time
        print(f"\n✓ Label-agnostic analysis completed in {elapsed2:.1f} seconds")
        
        # ==============================================================
        # SUMMARY
        # ==============================================================
        print("\n\n" + "="*80)
        print("FINAL RESULTS GENERATED")
        print("="*80)
        
        print("\n📊 Output Files:")
        print("\nLabel-Aware Results:")
        print("  - final_label_aware_pivot_predictions.csv")
        print("  - final_label_aware_method_comparison.csv")
        print("  - final_label_aware_multi_hop_chains.csv")
        
        print("\nLabel-Agnostic Results:")
        print("  - final_label_agnostic_pivot_predictions.csv")
        print("  - final_label_agnostic_method_comparison.csv")
        print("  - final_label_agnostic_multi_hop_chains.csv")
        
        print(f"\n⏱️  Total Processing Time:")
        print(f"  - Label-aware: {elapsed1:.1f}s")
        print(f"  - Label-agnostic: {elapsed2:.1f}s")
        print(f"  - Total: {elapsed1 + elapsed2:.1f}s")
        
        # ==============================================================
        # LOAD AND COMPARE RESULTS
        # ==============================================================
        print("\n\n" + "="*80)
        print("RESULTS COMPARISON")
        print("="*80)
        
        try:
            # Load method comparison results
            df_aware = pd.read_csv('final_label_aware_method_comparison.csv')
            df_agnostic = pd.read_csv('final_label_agnostic_method_comparison.csv')
            
            print("\n--- Label-Aware Performance ---")
            print(df_aware[['method', 'auc_roc', 'f1_score', 'cohens_d', 'p_value']].to_string(index=False))
            
            print("\n--- Label-Agnostic Performance ---")
            print(df_agnostic[['method', 'auc_roc', 'f1_score', 'cohens_d', 'p_value']].to_string(index=False))
            
            # Find best methods
            best_aware = df_aware.loc[df_aware['auc_roc'].idxmax()]
            best_agnostic = df_agnostic.loc[df_agnostic['auc_roc'].idxmax()]
            
            print("\n" + "="*80)
            print("KEY FINDINGS")
            print("="*80)
            
            print(f"\n✓ Best Label-Aware Method: {best_aware['method']}")
            print(f"  - AUC-ROC: {best_aware['auc_roc']:.4f}")
            print(f"  - F1-Score: {best_aware['f1_score']:.4f}")
            print(f"  - Cohen's d: {best_aware['cohens_d']:.4f}")
            print(f"  - p-value: {best_aware['p_value']:.6f}")
            
            print(f"\n✓ Best Label-Agnostic Method: {best_agnostic['method']}")
            print(f"  - AUC-ROC: {best_agnostic['auc_roc']:.4f}")
            print(f"  - F1-Score: {best_agnostic['f1_score']:.4f}")
            print(f"  - Cohen's d: {best_agnostic['cohens_d']:.4f}")
            print(f"  - p-value: {best_agnostic['p_value']:.6f}")
            
            # Load pivot predictions to check rates
            preds_aware = pd.read_csv('final_label_aware_pivot_predictions.csv')
            preds_agnostic = pd.read_csv('final_label_agnostic_pivot_predictions.csv')
            
            pivot_rate_aware = (preds_aware['is_pivot'].sum() / len(preds_aware)) * 100
            pivot_rate_agnostic = (preds_agnostic['is_pivot'].sum() / len(preds_agnostic)) * 100
            
            print(f"\n📊 Dataset Characteristics:")
            print(f"  - Total events analyzed: {len(preds_aware):,}")
            print(f"  - Pivot rate (label-aware): {pivot_rate_aware:.1f}%")
            print(f"  - Pivot rate (label-agnostic): {pivot_rate_agnostic:.1f}%")
            
            print("\n" + "="*80)
            print("THESIS CONTRIBUTIONS")
            print("="*80)
            print("\n1. Novel Subnet-Aware Embedding Method")
            print("   - Incorporates subnet relationships into FastRP")
            print("   - Captures lateral movement patterns")
            
            print("\n2. Label Integration Strategy")
            print("   - Uses MITRE ATT&CK tactics as node features")
            print("   - Demonstrates semantic information value")
            
            print("\n3. Statistical Validation")
            print("   - Multiple significance tests (t-test, Mann-Whitney)")
            print("   - Effect size measurement (Cohen's d)")
            print("   - Baseline comparisons (DeepWalk, Node2Vec, GAT, GraphSAGE)")
            
            print("\n4. Real-World APT Dataset")
            print("   - UWF-ZeekData24 with ground truth labels")
            print("   - Demonstrates practical applicability")
            
        except Exception as e:
            print(f"\n⚠️  Could not load comparison: {e}")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        analyzer.close()
        print("\n✓ Database connection closed")
    
    print("\n" + "="*80)
    print("NEXT STEPS")
    print("="*80)
    print("\n1. Review the generated CSV files for detailed results")
    print("2. Use results in thesis tables and figures")
    print("3. Consider window optimization if needed")
    print("4. Generate visualizations from the CSV data")
    
    print("\n" + "="*80)
    print("COMPLETE")
    print("="*80)

if __name__ == "__main__":
    main()
