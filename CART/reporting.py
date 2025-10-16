import pandas as pd
import json

class ReportGenerator:
    """
    Loads pivot candidate data from a JSON file and generates a validation
    report to evaluate the accuracy of the structural prediction model.
    """
    
    def __init__(self, filepath="structural_pivots.json"):
        self.filepath = filepath
        self.df = None
        
    def load_data(self):
        """Loads the JSON data into a pandas DataFrame."""
        print(f"--- Loading data from {self.filepath} ---")
        with open(self.filepath, 'r') as f:
            data = json.load(f)
        self.df = pd.DataFrame(data)
        
        if self.df.empty:
            print("⚠ Data file is empty. No report can be generated.")
            return False
            
        # Convert time_to_pivot from seconds to hours for better readability
        self.df['hours_to_pivot'] = self.df['time_to_pivot'] / 3600
        print(f"✓ Loaded {len(self.df):,} records into DataFrame.")
        return True

    def generate_report(self):
        """Analyzes the loaded data and prints a comprehensive report."""
        if not self.load_data():
            return
            
        print("\n" + "="*70)
        print("PIVOT PREDICTION VALIDATION REPORT")
        print("="*70)

        total_candidates = len(self.df)
        
        # --- Validation: Check how many candidates were REAL pivots ---
        # A true pivot is where BOTH the compromise and the subsequent attack were malicious.
        true_pivots_df = self.df[
            (self.df['compromise_is_attack'] == 1) & 
            (self.df['pivot_is_attack'] == 1)
        ]
        num_true_pivots = len(true_pivots_df)
        
        precision = (num_true_pivots / total_candidates) * 100 if total_candidates > 0 else 0
        
        print("\n--- MODEL PERFORMANCE ---")
        print(f"  Total Structural Candidates Found: {total_candidates:,}")
        print(f"  Verified True Pivots (A->B and B->C were attacks): {num_true_pivots:,}")
        print(f"  Precision of Structural Prediction: {precision:.2f}%")
        
        if num_true_pivots == 0:
            print("\nNo true pivots found among candidates. Analysis will stop here.")
            return
            
        # --- Analysis of TRUE PIVOTS only ---
        print("\n--- ANALYSIS OF VERIFIED PIVOTS ---")
        print("  Timing (Compromise to Pivot):")
        print(f"    Mean: {true_pivots_df['hours_to_pivot'].mean():.2f} hours")
        print(f"    Median: {true_pivots_df['hours_to_pivot'].median():.2f} hours")
        print(f"    Min: {true_pivots_df['hours_to_pivot'].min():.2f} hours")
        print(f"    Max: {true_pivots_df['hours_to_pivot'].max():.2f} hours")
        
        print("\n  Most Common Pivot Nodes:")
        print(true_pivots_df['pivot_ip'].value_counts().head(5).to_string())
        
        print("\n  Most Common Tactic Transitions (Compromise → Pivot):")
        transitions = true_pivots_df.groupby(['compromise_tactic', 'pivot_tactic']).size().nlargest(5)
        print(transitions.to_string())
        
        print("\n" + "="*70)
        print("REPORT COMPLETE")
        print("="*70)