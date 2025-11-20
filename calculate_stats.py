
import csv
import math
import os
import glob

# Set the run directory
RUN_DIR = 'thesis_results/run_20251119_194956_h48_d24'

def mean(data):
    return sum(data) / len(data) if data else 0

def variance(data, m):
    if len(data) < 2: return 0
    return sum((x - m) ** 2 for x in data) / (len(data) - 1)

def analyze_mode(mode_prefix, mode_name):
    print(f"\n{'='*40}")
    print(f"ANALYZING: {mode_name}")
    print(f"{'='*40}")
    
    # 1. Load Predictions for Stats
    pred_file = os.path.join(RUN_DIR, f'{mode_prefix}_h48_d24_pivot_predictions.csv')
    try:
        pivot_sims = []
        non_pivot_sims = []
        
        with open(pred_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Handle boolean strings
                bp = row['became_pivot'].lower() == 'true'
                try:
                    sim = float(row['fastrp_similarity'])
                    if bp:
                        pivot_sims.append(sim)
                    else:
                        non_pivot_sims.append(sim)
                except ValueError:
                    continue

        total = len(pivot_sims) + len(non_pivot_sims)
        pivot_count = len(pivot_sims)
        pivot_pct = (pivot_count / total * 100) if total > 0 else 0
        
        print(f"Samples: {total:,}")
        print(f"Pivots: {pivot_count:,} ({pivot_pct:.2f}%)")
        
        if pivot_sims and non_pivot_sims:
            mean_pivot = mean(pivot_sims)
            mean_non = mean(non_pivot_sims)
            var_pivot = variance(pivot_sims, mean_pivot)
            var_non = variance(non_pivot_sims, mean_non)
            std_pivot = math.sqrt(var_pivot)
            std_non = math.sqrt(var_non)
            
            print(f"Mean Similarity (Pivot): {mean_pivot:.4f}")
            print(f"Mean Similarity (Non-Pivot): {mean_non:.4f}")
            
            # Welch's t-test
            # t = (m1 - m2) / sqrt(v1/n1 + v2/n2)
            denom = math.sqrt(var_pivot/len(pivot_sims) + var_non/len(non_pivot_sims))
            t_stat = (mean_pivot - mean_non) / denom if denom > 0 else 0
            print(f"Welch's t: {t_stat:.4f}")
            
            # Cohen's d
            # d = (m1 - m2) / sqrt((s1^2 + s2^2) / 2)
            pooled_std = math.sqrt((var_pivot + var_non) / 2)
            cohens_d = (mean_pivot - mean_non) / pooled_std if pooled_std > 0 else 0
            print(f"Cohen's d: {cohens_d:.4f}")
            
    except FileNotFoundError:
        print(f"Prediction file {pred_file} not found.")

    # 2. Load Method Comparison for AUCs
    comp_file = os.path.join(RUN_DIR, f'{mode_prefix}_h48_d24_method_comparison.csv')
    try:
        with open(comp_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['Method'] == 'FastRP Embedding':
                    print(f"FastRP AUC-ROC: {float(row['AUC-ROC']):.4f}")
                    print(f"FastRP AUC-PR: {float(row['AUC-PR']):.4f}")
                    print(f"FastRP Precision: {float(row['Precision']):.4f}")
                    print(f"FastRP Recall: {float(row['Recall']):.4f}")
                    print(f"FastRP F1: {float(row['F1-Score']):.4f}")
                    break
        
    except FileNotFoundError:
        print(f"Comparison file {comp_file} not found.")
        
    # 3. Chain Counts
    print("\n--- Chain Counts ---")
    chain_files = sorted(glob.glob(os.path.join(RUN_DIR, f'{mode_prefix}_h48_d24_*hop_chains.csv')))
    
    # Sort by hop number
    def get_hop(fname):
        try:
            return int(os.path.basename(fname).split('_')[-2].replace('hop', ''))
        except:
            return 0
            
    chain_files.sort(key=get_hop)
    
    for cf in chain_files:
        basename = os.path.basename(cf)
        try:
            hop = get_hop(cf)
            with open(cf, 'r') as f:
                count = sum(1 for _ in f) - 1
            print(f"{hop}-hop chains: {count:,}")
        except Exception as e:
            print(f"Error reading {basename}: {e}")

analyze_mode('label_aware', 'Label-Aware Mode')
analyze_mode('label_agnostic', 'Label-Agnostic Mode')
