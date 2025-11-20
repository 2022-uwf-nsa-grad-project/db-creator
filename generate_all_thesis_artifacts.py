
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import networkx as nx
from pathlib import Path
import json

# Set style
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_context("paper", font_scale=1.2)

def load_data(run_dir):
    """Load necessary datasets from the run directory."""
    data = {}
    run_path = Path(run_dir)
    
    # Load pivot predictions
    for mode in ['label_aware', 'label_agnostic']:
        pred_file = list(run_path.glob(f"{mode}_*_pivot_predictions.csv"))
        if pred_file:
            data[f'{mode}_preds'] = pd.read_csv(pred_file[0])
            
    # Load chain data (summary or raw if available)
    # We might need to look for chain files in the run dir or parent
    # The pipeline moves them to run_dir
    for mode in ['label_aware', 'label_agnostic']:
        for hop in [2, 3, 4]:
            chain_file = list(run_path.glob(f"{mode}_*_chains.csv"))
            # If not found, maybe they are still in thesis_results/chain_temp or similar?
            # The pipeline moves them.
            pass

    return data

def plot_similarity_scatter(df, output_path):
    """Scatter plot contrasting FastRP similarity with normalized burst score."""
    if 'avg_burst' not in df.columns or 'fastrp_similarity' not in df.columns:
        return
    
    plt.figure(figsize=(10, 6))
    sns.scatterplot(
        data=df, 
        x='fastrp_similarity', 
        y='avg_burst', 
        hue='became_pivot', 
        alpha=0.6,
        palette={True: 'red', False: 'blue'}
    )
    plt.title('FastRP Similarity vs. Burst Score')
    plt.xlabel('FastRP Similarity')
    plt.ylabel('Average Burst Score')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

def plot_cumulative_pivots(df, output_path):
    """Dual-axis chart with cumulative pivot detections and evolving pivot rate."""
    df = df.sort_values('recon_time')
    df['cumulative_pivots'] = df['became_pivot'].cumsum()
    df['cumulative_count'] = np.arange(len(df)) + 1
    df['pivot_rate'] = df['cumulative_pivots'] / df['cumulative_count']
    
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    ax1.plot(df['cumulative_count'], df['cumulative_pivots'], color='red', label='Cumulative Pivots')
    ax1.set_xlabel('Reconnaissance Windows Observed')
    ax1.set_ylabel('Cumulative Pivots', color='red')
    ax1.tick_params(axis='y', labelcolor='red')
    
    ax2 = ax1.twinx()
    ax2.plot(df['cumulative_count'], df['pivot_rate'], color='blue', linestyle='--', label='Pivot Rate')
    ax2.set_ylabel('Pivot Rate', color='blue')
    ax2.tick_params(axis='y', labelcolor='blue')
    
    plt.title('Cumulative Pivot Detections and Rate')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

def plot_degree_distribution(df, output_path):
    """Kernel density comparison of normalized subnet sizes."""
    if 'subnet_size' not in df.columns:
        return

    plt.figure(figsize=(10, 6))
    sns.kdeplot(data=df, x='subnet_size', hue='became_pivot', common_norm=False, fill=True, palette={True: 'red', False: 'blue'})
    plt.title('Subnet Size Distribution by Pivot Status')
    plt.xlabel('Subnet Size (Nodes)')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

def plot_effect_size_forest(df, output_path):
    """Cohen’s d values computed from pivot and non-pivot distributions."""
    metrics = ['fastrp_similarity', 'avg_burst', 'avg_velocity', 'avg_pagerank', 'subnet_size']
    effects = []
    
    pivot = df[df['became_pivot'] == True]
    non_pivot = df[df['became_pivot'] == False]
    
    if pivot.empty or non_pivot.empty:
        return

    for metric in metrics:
        if metric in df.columns:
            d = (pivot[metric].mean() - non_pivot[metric].mean()) / np.sqrt((pivot[metric].std()**2 + non_pivot[metric].std()**2) / 2)
            effects.append({'Metric': metric, 'Cohen\'s d': d})
            
    if not effects:
        return
        
    eff_df = pd.DataFrame(effects)
    
    plt.figure(figsize=(8, 6))
    sns.barplot(data=eff_df, y='Metric', x='Cohen\'s d', color='teal')
    plt.axvline(0, color='black', linestyle='-')
    plt.axvline(0.2, color='gray', linestyle='--', alpha=0.5)
    plt.axvline(0.5, color='gray', linestyle='--', alpha=0.5)
    plt.axvline(0.8, color='gray', linestyle='--', alpha=0.5)
    plt.title('Effect Size (Cohen\'s d) of Features')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

def main():
    if len(sys.argv) < 2:
        print("Usage: python generate_all_thesis_artifacts.py <run_dir>")
        sys.exit(1)
        
    run_dir = Path(sys.argv[1])
    output_dir = Path('thesis_figures')
    output_dir.mkdir(exist_ok=True)
    
    print(f"Loading data from {run_dir}...")
    data = load_data(run_dir)
    
    if 'label_aware_preds' in data:
        print("Generating label-aware figures...")
        df = data['label_aware_preds']
        
        plot_similarity_scatter(df, output_dir / 'similarity_scatter.png')
        plot_cumulative_pivots(df, output_dir / 'cumulative_pivots.png')
        plot_degree_distribution(df, output_dir / 'degree_distribution.png')
        plot_effect_size_forest(df, output_dir / 'effect_size_forest.png')
        
    else:
        print("Label-aware predictions not found.")

    print("Artifact generation complete.")

if __name__ == "__main__":
    main()
