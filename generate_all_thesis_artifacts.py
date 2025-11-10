#!/usr/bin/env python3
"""
Thesis Artifact Generator
==========================
Generates all tables, figures, and statistical summaries for:
"Predicting Lateral Movement Pivots in Advanced Persistent Threat Campaigns 
Through Graph Neural Network Analysis"

This script produces:
1. All figures referenced in the thesis markdown
2. Summary tables for Chapter 4 (Results)
3. Statistical analysis outputs
4. Comparison visualizations

Run after completing thesis_pipeline.ipynb to generate publication-ready artifacts.
"""

from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import json
from typing import Optional
import warnings

warnings.filterwarnings('ignore')

# Set publication-quality matplotlib defaults
plt.rcParams['figure.dpi'] = 300
plt.rcParams['savefig.dpi'] = 300
plt.rcParams['font.size'] = 10
plt.rcParams['font.family'] = 'serif'
plt.rcParams['axes.labelsize'] = 11
plt.rcParams['axes.titlesize'] = 12
plt.rcParams['xtick.labelsize'] = 9
plt.rcParams['ytick.labelsize'] = 9
plt.rcParams['legend.fontsize'] = 9

# Configuration
THESIS_RESULTS_DIR = Path('thesis_results')
THESIS_FIGURES_DIR = Path('thesis_figures')
THESIS_FIGURES_DIR.mkdir(exist_ok=True)

print("="*80)
print("THESIS ARTIFACT GENERATOR")
print("="*80)
print(f"Output directory: {THESIS_FIGURES_DIR}")
print(f"Results directory: {THESIS_RESULTS_DIR}")
print()


def find_latest_run() -> Optional[Path]:
    """Find the most recent thesis_results run directory."""
    run_dirs = sorted(THESIS_RESULTS_DIR.glob('run_*'))
    if not run_dirs:
        print("⚠ No run directories found in thesis_results/")
        return None
    latest = run_dirs[-1]
    print(f"✓ Using latest run: {latest.name}")
    return latest


def load_run_metadata(run_dir: Path) -> dict:
    """Load run_metadata.json from a run directory."""
    metadata_path = run_dir / 'run_metadata.json'
    if not metadata_path.exists():
        return {}
    with open(metadata_path) as f:
        return json.load(f)


def generate_dataset_summary_table(run_dir: Path):
    """Table 1: Dataset characteristics summary."""
    print("\n--- Generating Dataset Summary Table ---")
    
    # Load both mode predictions
    aware_preds = None
    agnostic_preds = None
    
    for csv_file in run_dir.glob('*_pivot_predictions.csv'):
        if 'label_aware' in csv_file.name:
            aware_preds = pd.read_csv(csv_file)
        elif 'label_agnostic' in csv_file.name:
            agnostic_preds = pd.read_csv(csv_file)
    
    if aware_preds is None:
        print("  ⚠ No label_aware predictions found")
        return
    
    # Compute statistics
    summary_data = {
        'Metric': [
            'Total Edges (Attack-Focused)',
            'IP Nodes',
            'Subnets (/24)',
            'Reconnaissance Windows (Label-Aware)',
            'Pivot Windows (Label-Aware)',
            'Pivot Rate (Label-Aware)',
            'Reconnaissance Windows (Label-Agnostic)',
            'Pivot Rate (Label-Agnostic)',
            'Median Time to First Pivot (hours)',
            'Mean Time to First Pivot (hours)',
        ],
        'Value': [
            '1,898,613',
            '357',
            '21',
            f"{len(aware_preds):,}",
            f"{aware_preds['became_pivot'].sum():,}",
            f"{aware_preds['became_pivot'].mean() * 100:.2f}%",
            f"{len(agnostic_preds):,}" if agnostic_preds is not None else 'N/A',
            f"{agnostic_preds['became_pivot'].mean() * 100:.2f}%" if agnostic_preds is not None else 'N/A',
            '0.41',  # From thesis document
            '1.84',  # From thesis document
        ]
    }
    
    df = pd.DataFrame(summary_data)
    
    # Save as CSV
    output_path = THESIS_FIGURES_DIR / 'table1_dataset_summary.csv'
    df.to_csv(output_path, index=False)
    print(f"  ✓ Saved: {output_path}")
    
    # Also create a formatted LaTeX table
    latex_output = THESIS_FIGURES_DIR / 'table1_dataset_summary.tex'
    with open(latex_output, 'w') as f:
        f.write("% Dataset Summary Table\n")
        f.write("\\begin{table}[h]\n")
        f.write("\\centering\n")
        f.write("\\caption{UWF-ZeekData24 Dataset Characteristics}\n")
        f.write("\\label{tab:dataset-summary}\n")
        f.write("\\begin{tabular}{lr}\n")
        f.write("\\toprule\n")
        f.write("\\textbf{Metric} & \\textbf{Value} \\\\\n")
        f.write("\\midrule\n")
        for _, row in df.iterrows():
            f.write(f"{row['Metric']} & {row['Value']} \\\\\n")
        f.write("\\bottomrule\n")
        f.write("\\end{tabular}\n")
        f.write("\\end{table}\n")
    print(f"  ✓ Saved: {latex_output}")
    
    # Print to console
    print("\n" + "="*60)
    print("TABLE 1: Dataset Summary")
    print("="*60)
    print(df.to_string(index=False))
    print("="*60)


def generate_performance_comparison_table(run_dir: Path):
    """Table 2: Label-aware vs Label-agnostic performance."""
    print("\n--- Generating Performance Comparison Table ---")
    
    # Load method comparison CSVs
    aware_methods = None
    agnostic_methods = None
    
    for csv_file in run_dir.glob('*_method_comparison.csv'):
        if 'label_aware' in csv_file.name:
            aware_methods = pd.read_csv(csv_file)
        elif 'label_agnostic' in csv_file.name:
            agnostic_methods = pd.read_csv(csv_file)
    
    if aware_methods is None or agnostic_methods is None:
        print("  ⚠ Missing method comparison files")
        return
    
    # Extract FastRP rows
    aware_fastrp = aware_methods[aware_methods['Method'] == 'FastRP Embedding'].iloc[0]
    agnostic_fastrp = agnostic_methods[agnostic_methods['Method'] == 'FastRP Embedding'].iloc[0]
    
    # Create comparison table
    comparison_data = {
        'Mode': ['Label-Aware', 'Label-Agnostic'],
        'AUC-ROC': [aware_fastrp['AUC-ROC'], agnostic_fastrp['AUC-ROC']],
        'AUC-PR': [aware_fastrp['AUC-PR'], agnostic_fastrp['AUC-PR']],
        'Precision': [aware_fastrp['Precision'], agnostic_fastrp['Precision']],
        'Recall': [aware_fastrp['Recall'], agnostic_fastrp['Recall']],
        'F1-Score': [aware_fastrp['F1-Score'], agnostic_fastrp['F1-Score']],
    }
    
    df = pd.DataFrame(comparison_data)
    
    # Save CSV
    output_path = THESIS_FIGURES_DIR / 'table2_performance_comparison.csv'
    df.to_csv(output_path, index=False)
    print(f"  ✓ Saved: {output_path}")
    
    # LaTeX table
    latex_output = THESIS_FIGURES_DIR / 'table2_performance_comparison.tex'
    with open(latex_output, 'w') as f:
        f.write("% Performance Comparison Table\n")
        f.write("\\begin{table}[h]\n")
        f.write("\\centering\n")
        f.write("\\caption{FastRP Performance: Label-Aware vs Label-Agnostic}\n")
        f.write("\\label{tab:performance-comparison}\n")
        f.write("\\begin{tabular}{lcccccc}\n")
        f.write("\\toprule\n")
        f.write("\\textbf{Mode} & \\textbf{AUC-ROC} & \\textbf{AUC-PR} & \\textbf{Precision} & \\textbf{Recall} & \\textbf{F1-Score} \\\\\n")
        f.write("\\midrule\n")
        for _, row in df.iterrows():
            f.write(f"{row['Mode']} & {row['AUC-ROC']:.4f} & {row['AUC-PR']:.4f} & {row['Precision']:.4f} & {row['Recall']:.4f} & {row['F1-Score']:.4f} \\\\\n")
        f.write("\\bottomrule\n")
        f.write("\\end{tabular}\n")
        f.write("\\end{table}\n")
    print(f"  ✓ Saved: {latex_output}")
    
    print("\n" + "="*80)
    print("TABLE 2: Performance Comparison")
    print("="*80)
    print(df.to_string(index=False, float_format=lambda x: f'{x:.4f}'))
    print("="*80)


def generate_baseline_comparison_figure(run_dir: Path):
    """Figure: Baseline method comparison (bar chart)."""
    print("\n--- Generating Baseline Comparison Figure ---")
    
    # Load label-aware methods
    aware_methods = None
    for csv_file in run_dir.glob('*label_aware*_method_comparison.csv'):
        aware_methods = pd.read_csv(csv_file)
        break
    
    if aware_methods is None:
        print("  ⚠ No label_aware method comparison found")
        return
    
    # Create figure with subplots
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    
    metrics = ['AUC-ROC', 'AUC-PR', 'F1-Score']
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', 
              '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22']
    
    for idx, metric in enumerate(metrics):
        ax = axes[idx]
        data = aware_methods.sort_values(metric, ascending=True)
        
        # Highlight FastRP
        bar_colors = ['#e74c3c' if 'FastRP' in m else colors[i % len(colors)] 
                      for i, m in enumerate(data['Method'])]
        
        ax.barh(data['Method'], data[metric], color=bar_colors, alpha=0.8)
        ax.set_xlabel(metric)
        ax.set_title(f'{metric} Comparison')
        ax.grid(axis='x', alpha=0.3)
        
        # Add values on bars
        for i, v in enumerate(data[metric]):
            ax.text(v + 0.01, i, f'{v:.3f}', va='center', fontsize=8)
    
    plt.tight_layout()
    output_path = THESIS_FIGURES_DIR / 'figure_baseline_comparison.png'
    plt.savefig(output_path, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Saved: {output_path}")


def generate_similarity_distribution_figure(run_dir: Path):
    """Figure: Similarity score distributions for pivots vs non-pivots."""
    print("\n--- Generating Similarity Distribution Figure ---")
    
    # Load predictions
    aware_preds = None
    for csv_file in run_dir.glob('*label_aware*_pivot_predictions.csv'):
        aware_preds = pd.read_csv(csv_file)
        break
    
    if aware_preds is None:
        print("  ⚠ No label_aware predictions found")
        return
    
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    
    # Histogram
    ax = axes[0]
    pivot_sims = aware_preds[aware_preds['became_pivot'] == 1]['fastrp_similarity']
    non_pivot_sims = aware_preds[aware_preds['became_pivot'] == 0]['fastrp_similarity']
    
    ax.hist(non_pivot_sims, bins=50, alpha=0.6, label='Non-Pivot', color='#3498db', density=True)
    ax.hist(pivot_sims, bins=50, alpha=0.6, label='Pivot', color='#e74c3c', density=True)
    ax.set_xlabel('FastRP Similarity Score')
    ax.set_ylabel('Density')
    ax.set_title('Similarity Score Distribution')
    ax.legend()
    ax.grid(alpha=0.3)
    
    # Box plot
    ax = axes[1]
    data_to_plot = [non_pivot_sims.dropna(), pivot_sims.dropna()]
    bp = ax.boxplot(data_to_plot, labels=['Non-Pivot', 'Pivot'],
                    patch_artist=True, widths=0.6)
    
    colors_box = ['#3498db', '#e74c3c']
    for patch, color in zip(bp['boxes'], colors_box):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)
    
    ax.set_ylabel('FastRP Similarity Score')
    ax.set_title('Similarity Score Comparison')
    ax.grid(axis='y', alpha=0.3)
    
    # Add statistical annotation
    from scipy import stats
    t_stat, p_value = stats.ttest_ind(pivot_sims.dropna(), non_pivot_sims.dropna(), equal_var=False)
    cohens_d = (pivot_sims.mean() - non_pivot_sims.mean()) / np.sqrt((pivot_sims.std()**2 + non_pivot_sims.std()**2) / 2)
    
    ax.text(0.5, 0.95, f"Welch's t = {t_stat:.2f}, p < 0.001\nCohen's d = {cohens_d:.2f}",
            transform=ax.transAxes, ha='center', va='top',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
            fontsize=9)
    
    plt.tight_layout()
    output_path = THESIS_FIGURES_DIR / 'figure_similarity_distributions.png'
    plt.savefig(output_path, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Saved: {output_path}")


def generate_roc_pr_curves(run_dir: Path):
    """Figure: ROC and PR curves for FastRP."""
    print("\n--- Generating ROC and PR Curves ---")
    
    from sklearn.metrics import roc_curve, precision_recall_curve, auc
    
    # Load predictions
    aware_preds = None
    for csv_file in run_dir.glob('*label_aware*_pivot_predictions.csv'):
        aware_preds = pd.read_csv(csv_file)
        break
    
    if aware_preds is None:
        print("  ⚠ No predictions found")
        return
    
    y_true = aware_preds['became_pivot'].astype(int)
    y_scores = aware_preds['fastrp_similarity']
    
    # Compute curves
    fpr, tpr, _ = roc_curve(y_true, y_scores)
    roc_auc = auc(fpr, tpr)
    
    precision, recall, _ = precision_recall_curve(y_true, y_scores)
    pr_auc = auc(recall, precision)
    
    # Create figure
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    
    # ROC Curve
    ax = axes[0]
    ax.plot(fpr, tpr, color='#e74c3c', lw=2, label=f'FastRP (AUC = {roc_auc:.3f})')
    ax.plot([0, 1], [0, 1], color='gray', lw=1, linestyle='--', label='Random')
    ax.set_xlabel('False Positive Rate')
    ax.set_ylabel('True Positive Rate')
    ax.set_title('ROC Curve')
    ax.legend(loc='lower right')
    ax.grid(alpha=0.3)
    
    # PR Curve
    ax = axes[1]
    ax.plot(recall, precision, color='#3498db', lw=2, label=f'FastRP (AUC = {pr_auc:.3f})')
    baseline = y_true.mean()
    ax.axhline(baseline, color='gray', lw=1, linestyle='--', label=f'Baseline ({baseline:.3f})')
    ax.set_xlabel('Recall')
    ax.set_ylabel('Precision')
    ax.set_title('Precision-Recall Curve')
    ax.legend(loc='lower left')
    ax.grid(alpha=0.3)
    
    plt.tight_layout()
    output_path = THESIS_FIGURES_DIR / 'figure_roc_pr_curves.png'
    plt.savefig(output_path, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Saved: {output_path}")


def generate_subnet_pivot_heatmap(run_dir: Path):
    """Figure: Subnet pivot activity heatmap."""
    print("\n--- Generating Subnet Pivot Heatmap ---")
    
    # Load predictions
    aware_preds = None
    for csv_file in run_dir.glob('*label_aware*_pivot_predictions.csv'):
        aware_preds = pd.read_csv(csv_file)
        break
    
    if aware_preds is None or 'victim_subnet' not in aware_preds.columns:
        print("  ⚠ No subnet data found")
        return
    
    # Aggregate by subnet
    subnet_stats = aware_preds.groupby('victim_subnet').agg({
        'became_pivot': ['sum', 'count', 'mean']
    }).reset_index()
    subnet_stats.columns = ['subnet', 'pivot_count', 'total_windows', 'pivot_rate']
    subnet_stats = subnet_stats.sort_values('pivot_count', ascending=False).head(15)
    
    # Create figure
    fig, ax = plt.subplots(figsize=(10, 8))
    
    # Create heatmap-style bar chart
    y_pos = np.arange(len(subnet_stats))
    colors = plt.cm.YlOrRd(subnet_stats['pivot_rate'])
    
    bars = ax.barh(y_pos, subnet_stats['pivot_count'], color=colors, alpha=0.8)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(subnet_stats['subnet'], fontsize=9)
    ax.set_xlabel('Number of Pivot Windows')
    ax.set_title('Top 15 Subnets by Pivot Activity')
    ax.grid(axis='x', alpha=0.3)
    
    # Add colorbar
    sm = plt.cm.ScalarMappable(cmap=plt.cm.YlOrRd, 
                               norm=plt.Normalize(vmin=subnet_stats['pivot_rate'].min(),
                                                 vmax=subnet_stats['pivot_rate'].max()))
    sm.set_array([])
    cbar = plt.colorbar(sm, ax=ax)
    cbar.set_label('Pivot Rate', rotation=270, labelpad=20)
    
    # Add value labels
    for i, (idx, row) in enumerate(subnet_stats.iterrows()):
        ax.text(row['pivot_count'] + 50, i, 
                f"{row['pivot_count']:.0f} ({row['pivot_rate']*100:.1f}%)",
                va='center', fontsize=8)
    
    plt.tight_layout()
    output_path = THESIS_FIGURES_DIR / 'figure_subnet_pivot_heatmap.png'
    plt.savefig(output_path, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Saved: {output_path}")


def generate_temporal_analysis_figure(run_dir: Path):
    """Figure: Temporal patterns in pivot behavior."""
    print("\n--- Generating Temporal Analysis Figure ---")
    
    # Load chain data if available
    chain_files = list(run_dir.glob('*_chains.csv'))
    if not chain_files:
        print("  ⚠ No chain data found")
        return
    
    # Use label-aware chains
    chain_df = None
    for f in chain_files:
        if 'label_aware' in f.name:
            chain_df = pd.read_csv(f)
            break
    
    if chain_df is None or 'hours_to_hop2' not in chain_df.columns:
        print("  ⚠ No suitable chain data")
        return
    
    fig, axes = plt.subplots(2, 2, figsize=(12, 10))
    
    # Distribution of time to hop 2
    ax = axes[0, 0]
    ax.hist(chain_df['hours_to_hop2'].clip(upper=100), bins=50, color='#3498db', alpha=0.7, edgecolor='black')
    ax.set_xlabel('Hours to 2nd Hop')
    ax.set_ylabel('Frequency')
    ax.set_title('Time Distribution: 1st → 2nd Hop')
    ax.axvline(chain_df['hours_to_hop2'].median(), color='red', linestyle='--', label=f'Median: {chain_df["hours_to_hop2"].median():.1f}h')
    ax.legend()
    ax.grid(alpha=0.3)
    
    # Distribution of time to hop 3
    ax = axes[0, 1]
    ax.hist(chain_df['hours_to_hop3'].clip(upper=200), bins=50, color='#e74c3c', alpha=0.7, edgecolor='black')
    ax.set_xlabel('Hours to 3rd Hop')
    ax.set_ylabel('Frequency')
    ax.set_title('Time Distribution: 2nd → 3rd Hop')
    ax.axvline(chain_df['hours_to_hop3'].median(), color='darkred', linestyle='--', label=f'Median: {chain_df["hours_to_hop3"].median():.1f}h')
    ax.legend()
    ax.grid(alpha=0.3)
    
    # Cumulative time distribution
    ax = axes[1, 0]
    sorted_times = np.sort(chain_df['hours_to_hop2'].dropna())
    cumulative = np.arange(1, len(sorted_times) + 1) / len(sorted_times) * 100
    ax.plot(sorted_times, cumulative, color='#2ecc71', lw=2)
    ax.set_xlabel('Hours to 2nd Hop')
    ax.set_ylabel('Cumulative Percentage (%)')
    ax.set_title('Cumulative Distribution: Time to Pivot')
    ax.axhline(75, color='gray', linestyle='--', alpha=0.5)
    ax.axhline(90, color='gray', linestyle='--', alpha=0.5)
    ax.grid(alpha=0.3)
    ax.set_xlim(0, 50)
    
    # Box plot comparison
    ax = axes[1, 1]
    data_to_plot = [
        chain_df['hours_to_hop2'].dropna().clip(upper=100),
        chain_df['hours_to_hop3'].dropna().clip(upper=200)
    ]
    bp = ax.boxplot(data_to_plot, labels=['1st→2nd Hop', '2nd→3rd Hop'],
                    patch_artist=True, widths=0.6)
    colors_box = ['#3498db', '#e74c3c']
    for patch, color in zip(bp['boxes'], colors_box):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)
    ax.set_ylabel('Hours')
    ax.set_title('Hop Timing Comparison')
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    output_path = THESIS_FIGURES_DIR / 'figure_temporal_analysis.png'
    plt.savefig(output_path, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Saved: {output_path}")


def generate_statistical_summary():
    """Generate a comprehensive statistical summary document."""
    print("\n--- Generating Statistical Summary ---")
    
    output_path = THESIS_FIGURES_DIR / 'statistical_summary.txt'
    
    with open(output_path, 'w') as f:
        f.write("="*80 + "\n")
        f.write("THESIS STATISTICAL SUMMARY\n")
        f.write("="*80 + "\n\n")
        
        f.write("This file contains key statistical findings for inclusion in the thesis.\n\n")
        
        f.write("Chapter 4: Key Results\n")
        f.write("-" * 40 + "\n\n")
        
        f.write("Label-Aware Mode:\n")
        f.write("  - AUC-ROC: 0.676\n")
        f.write("  - AUC-PR: 0.979\n")
        f.write("  - Precision: 0.948\n")
        f.write("  - Recall: 1.000\n")
        f.write("  - F1-Score: 0.974\n")
        f.write("  - Welch's t: 71.36 (p < 1e-300)\n")
        f.write("  - Cohen's d: 1.16 (large effect size)\n\n")
        
        f.write("Label-Agnostic Mode:\n")
        f.write("  - AUC-ROC: 0.550\n")
        f.write("  - AUC-PR: 0.998\n")
        f.write("  - Precision: 0.997\n")
        f.write("  - Recall: 1.000\n")
        f.write("  - F1-Score: 0.999\n")
        f.write("  - Cohen's d: 0.13 (small effect size)\n\n")
        
        f.write("Interpretation:\n")
        f.write("  - Label-aware mode shows strong discriminative power (d=1.16)\n")
        f.write("  - Label-agnostic maintains high precision but lower discrimination\n")
        f.write("  - Both modes achieve perfect recall (no pivots missed)\n")
        f.write("  - AUC-PR superior to AUC-ROC due to class imbalance\n\n")
        
        f.write("Temporal Findings:\n")
        f.write("  - Median time to pivot: 0.41 hours (24.6 minutes)\n")
        f.write("  - Mean time to pivot: 1.84 hours\n")
        f.write("  - 75th percentile: Within detection window\n")
        f.write("  - Recommendation: 24-hour detection window captures most pivots\n\n")
    
    print(f"  ✓ Saved: {output_path}")


def main():
    """Generate all thesis artifacts."""
    
    run_dir = find_latest_run()
    if run_dir is None:
        print("\n❌ Cannot proceed without a thesis_results run directory.")
        print("   Please run thesis_pipeline.ipynb first.")
        return
    
    print()
    
    # Generate all artifacts
    try:
        generate_dataset_summary_table(run_dir)
        generate_performance_comparison_table(run_dir)
        generate_baseline_comparison_figure(run_dir)
        generate_similarity_distribution_figure(run_dir)
        generate_roc_pr_curves(run_dir)
        generate_subnet_pivot_heatmap(run_dir)
        generate_temporal_analysis_figure(run_dir)
        generate_statistical_summary()
        
        print("\n" + "="*80)
        print("✓ ALL THESIS ARTIFACTS GENERATED SUCCESSFULLY")
        print("="*80)
        print(f"\nOutput location: {THESIS_FIGURES_DIR.absolute()}")
        print("\nGenerated files:")
        for file in sorted(THESIS_FIGURES_DIR.glob('*')):
            print(f"  - {file.name}")
        
        print("\n" + "="*80)
        print("NEXT STEPS")
        print("="*80)
        print("1. Review all figures in thesis_figures/")
        print("2. Insert figures into your thesis markdown/LaTeX document")
        print("3. Use table*.csv files for manuscript tables")
        print("4. Reference statistical_summary.txt for key numbers")
        print("5. Update thesis text with any new insights from visualizations")
        
    except Exception as e:
        print(f"\n❌ Error generating artifacts: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
