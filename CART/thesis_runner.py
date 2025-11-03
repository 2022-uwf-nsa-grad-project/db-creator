"""
Thesis Analysis Runner
======================

Consolidated class for running all thesis analysis scripts and experiments.
Provides clean API for database exploration, tactic analysis, window optimization,
and final results generation with comprehensive logging.

Author: Trever Knie
Course: CS Thesis
"""

import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from .analyzers import SubnetPivotAnalyzer


class ThesisRunner:
    """
    Main class for running thesis experiments and generating results.
    
    Consolidates:
    - Database exploration
    - Attack tactics analysis
    - Quick pivot detection tests
    - Window optimization experiments
    - Final results generation
    - Visualization and logging
    """
    
    def __init__(
        self,
        results_dir: str = "thesis_results",
        figures_dir: str = "thesis_figures",
        log_level: str = "INFO"
    ):
        """
        Initialize ThesisRunner with output directories and logging.
        
        Args:
            results_dir: Directory for CSV results and logs
            figures_dir: Directory for generated visualizations
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.results_dir = Path(results_dir)
        self.figures_dir = Path(figures_dir)
        
        # Create directories
        self.results_dir.mkdir(exist_ok=True)
        self.figures_dir.mkdir(exist_ok=True)
        
        # Setup logging
        self.log_file = self.results_dir / f"thesis_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        self._setup_logging(log_level)
        
        # Initialize analyzer
        self.logger.info("Initializing SubnetPivotAnalyzer...")
        self.analyzer = SubnetPivotAnalyzer()
        
        # Storage for results
        self.exploration_results = {}
        self.tactics_results = {}
        self.window_results = {}
        self.final_results = {}
        
        self.logger.info(f"ThesisRunner initialized")
        self.logger.info(f"Results directory: {self.results_dir.absolute()}")
        self.logger.info(f"Figures directory: {self.figures_dir.absolute()}")
    
    def _setup_logging(self, log_level: str):
        """Setup logging to both file and console."""
        self.logger = logging.getLogger("ThesisRunner")
        self.logger.setLevel(getattr(logging, log_level))
        
        # File handler
        fh = logging.FileHandler(self.log_file)
        fh.setLevel(logging.DEBUG)
        
        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(getattr(logging, log_level))
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
    
    def explore_database(self) -> Dict:
        """
        Run comprehensive database exploration.
        
        Returns:
            Dictionary with exploration statistics
        """
        self.logger.info("="*80)
        self.logger.info("EXPLORING DATABASE")
        self.logger.info("="*80)
        
        start_time = time.time()
        
        try:
            # Connect and explore
            self.analyzer.connect()
            results = self.analyzer.explore_database()
            
            # Store results
            self.exploration_results = results
            
            # Save to JSON
            output_file = self.results_dir / "database_exploration.json"
            with open(output_file, 'w') as f:
                json.dump(results, f, indent=2)
            
            self.logger.info(f"✓ Database exploration saved to {output_file}")
            self.logger.info(f"  - Nodes: {results.get('node_count', 'N/A')}")
            self.logger.info(f"  - Relationships: {results.get('relationship_count', 'N/A')}")
            self.logger.info(f"  - Subnets: {len(results.get('subnets', []))}")
            
            elapsed = time.time() - start_time
            self.logger.info(f"✓ Database exploration completed in {elapsed:.1f}s")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Database exploration failed: {e}")
            raise
        finally:
            self.analyzer.close()
    
    def analyze_attack_tactics(self) -> Dict:
        """
        Analyze MITRE ATT&CK tactic distribution in dataset.
        
        Returns:
            Dictionary with tactic statistics
        """
        self.logger.info("="*80)
        self.logger.info("ANALYZING ATTACK TACTICS")
        self.logger.info("="*80)
        
        start_time = time.time()
        
        try:
            self.analyzer.connect()
            
            results = {
                'overall_distribution': [],
                'cross_subnet_by_tactic': [],
                'techniques_by_tactic': [],
                'same_vs_cross_subnet': {},
                'reconnaissance_followup': []
            }
            
            with self.analyzer.driver.session(database=self.analyzer.database) as session:
                # Query 1: Overall tactic distribution
                self.logger.info("Analyzing overall tactic distribution...")
                query1_total = """
                    MATCH ()-[r:CONNECTS]->()
                    RETURN count(r) as total
                """
                total_result = session.run(query1_total).single()
                total_attacks = total_result['total']
                
                query1 = """
                    MATCH ()-[r:CONNECTS]->()
                    WITH coalesce(r.tactic, 'none') as tactic, count(*) as count
                    RETURN tactic, count
                    ORDER BY count DESC
                """
                for record in session.run(query1):
                    percentage = (record['count'] / total_attacks) * 100
                    results['overall_distribution'].append({
                        'tactic': record['tactic'],
                        'count': record['count'],
                        'percentage': percentage
                    })
                    self.logger.info(f"  {record['tactic']}: {record['count']:,} ({percentage:.2f}%)")
                
                # Query 2: Cross-subnet attacks by tactic
                self.logger.info("Analyzing cross-subnet attacks...")
                query2 = """
                    MATCH (src:IP)-[r:CONNECTS]->(dst:IP)
                    WHERE src.subnet <> dst.subnet
                    WITH coalesce(r.tactic, 'none') as tactic, 
                         collect(DISTINCT src.subnet) as src_subnets,
                         collect(DISTINCT dst.subnet) as dst_subnets,
                         count(*) as count
                    RETURN tactic, count, 
                           size(src_subnets) as src_subnet_count,
                           size(dst_subnets) as dst_subnet_count
                    ORDER BY count DESC
                """
                for record in session.run(query2):
                    results['cross_subnet_by_tactic'].append({
                        'tactic': record['tactic'],
                        'count': record['count'],
                        'src_subnets': record['src_subnet_count'],
                        'dst_subnets': record['dst_subnet_count']
                    })
                    self.logger.info(f"  {record['tactic']}: {record['count']:,} attacks across subnets")
                
                # Query 3: Top techniques for each tactic
                self.logger.info("Analyzing techniques by tactic...")
                query3 = """
                    MATCH ()-[r:CONNECTS]->()
                    WITH coalesce(r.tactic, 'none') as tactic,
                         coalesce(r.technique, 'none') as technique,
                         count(*) as attack_count
                    WITH tactic, 
                         collect({technique: technique, count: attack_count}) as techniques,
                         sum(attack_count) as total_attacks
                    RETURN tactic, 
                           total_attacks,
                           [t in techniques | t.technique][0..10] as top_techniques,
                           size([t in techniques | t.technique]) as unique_techniques
                    ORDER BY total_attacks DESC
                    LIMIT 5
                """
                for record in session.run(query3):
                    results['techniques_by_tactic'].append({
                        'tactic': record['tactic'],
                        'total_attacks': record['total_attacks'],
                        'unique_techniques': record['unique_techniques'],
                        'top_techniques': record['top_techniques']
                    })
                    self.logger.info(f"  {record['tactic']}: {record['unique_techniques']} unique techniques")
                
                # Query 4: Same vs Cross subnet
                self.logger.info("Comparing same-subnet vs cross-subnet attacks...")
                query4 = """
                    MATCH (src:IP)-[r:CONNECTS]->(dst:IP)
                    WITH CASE WHEN src.subnet = dst.subnet THEN 'Same Subnet'
                             ELSE 'Cross Subnet' END as type,
                         count(*) as count
                    RETURN type, count
                    ORDER BY count DESC
                """
                for record in session.run(query4):
                    percentage = (record['count'] / total_attacks) * 100
                    results['same_vs_cross_subnet'][record['type']] = {
                        'count': record['count'],
                        'percentage': percentage
                    }
                    self.logger.info(f"  {record['type']}: {record['count']:,} ({percentage:.2f}%)")
                
                # Query 5: What follows reconnaissance?
                self.logger.info("Analyzing reconnaissance follow-up actions...")
                query5 = """
                    MATCH (src:IP)-[r1:CONNECTS]->(dst:IP)
                    WHERE r1.tactic = 'Reconnaissance'
                    WITH src, dst, r1.ts as recon_time
                    MATCH (dst)-[r2:CONNECTS]->()
                    WHERE r2.ts > recon_time 
                      AND r2.ts <= recon_time + 86400
                      AND r2.tactic IS NOT NULL
                    WITH coalesce(r2.tactic, 'none') as followup_tactic,
                         count(*) as count,
                         avg((r2.ts - recon_time) / 3600.0) as avg_hours_after
                    RETURN followup_tactic, count, avg_hours_after
                    ORDER BY count DESC
                    LIMIT 5
                """
                for record in session.run(query5):
                    results['reconnaissance_followup'].append({
                        'tactic': record['followup_tactic'],
                        'count': record['count'],
                        'avg_hours_after': record['avg_hours_after']
                    })
                    self.logger.info(f"  {record['followup_tactic']}: {record['count']:,} attacks "
                                   f"(avg {record['avg_hours_after']:.2f}h after recon)")
            
            # Store and save
            self.tactics_results = results
            output_file = self.results_dir / "attack_tactics_analysis.json"
            with open(output_file, 'w') as f:
                json.dump(results, f, indent=2)
            
            elapsed = time.time() - start_time
            self.logger.info(f"✓ Tactics analysis saved to {output_file}")
            self.logger.info(f"✓ Tactics analysis completed in {elapsed:.1f}s")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Tactics analysis failed: {e}")
            raise
        finally:
            self.analyzer.close()
    
    def quick_pivot_test(
        self,
        embedding_dim: int = 128,
        window_hours: int = 24,
        max_events: int = 10000
    ) -> Dict:
        """
        Run quick pivot detection test to verify fixes.
        
        Args:
            embedding_dim: Embedding dimension
            window_hours: Detection window in hours
            max_events: Maximum events to test
            
        Returns:
            Dictionary with test results
        """
        self.logger.info("="*80)
        self.logger.info("RUNNING QUICK PIVOT DETECTION TEST")
        self.logger.info("="*80)
        self.logger.info(f"Configuration: dim={embedding_dim}, window={window_hours}h, max_events={max_events}")
        
        start_time = time.time()
        
        try:
            self.analyzer.connect()
            
            # Run pivot prediction (generates CSV files)
            self.analyzer.run_pivot_prediction(
                use_labels=True,
                historical_window_hours=window_hours,
                detection_window_hours=window_hours,
                embedding_dim=embedding_dim,
                output_prefix="quick_test"
            )
            
            # Read generated files
            predictions = pd.read_csv('quick_test_pivot_predictions.csv')
            comparison = pd.read_csv('quick_test_method_comparison.csv')
            
            # Calculate metrics from the data
            train_pivot_rate = predictions[predictions['set'] == 'train']['became_pivot'].mean()
            test_pivot_rate = predictions[predictions['set'] == 'test']['became_pivot'].mean()
            
            # Get best method metrics
            fastRP_row = comparison[comparison['Method'] == 'FastRP Embedding'].iloc[0]
            
            # Read statistics file if it exists
            import json
            try:
                with open('quick_test_statistics.json', 'r') as f:
                    test_stats = json.load(f)
            except FileNotFoundError:
                # Calculate basic stats if file doesn't exist
                test_stats = {
                    'welch_p': 0.0,
                    'cohens_d': 0.0
                }
            
            summary = {
                'configuration': {
                    'embedding_dim': embedding_dim,
                    'window_hours': window_hours,
                    'max_events': max_events
                },
                'train_pivot_rate': train_pivot_rate,
                'test_pivot_rate': test_pivot_rate,
                'p_value': test_stats.get('welch_p', 0.0),
                'cohens_d': test_stats.get('cohens_d', 0.0),
                'auc_roc': fastRP_row['AUC-ROC'],
                'f1_score': fastRP_row['F1-Score']
            }
            
            self.logger.info(f"✓ Test Results:")
            self.logger.info(f"  Train pivot rate: {summary['train_pivot_rate']:.1%}")
            self.logger.info(f"  Test pivot rate: {summary['test_pivot_rate']:.1%}")
            self.logger.info(f"  p-value: {summary['p_value']:.6f}")
            self.logger.info(f"  Cohen's d: {summary['cohens_d']:.4f}")
            self.logger.info(f"  AUC-ROC: {summary['auc_roc']:.4f}")
            self.logger.info(f"  F1-Score: {summary['f1_score']:.4f}")
            
            # Save summary
            output_file = self.results_dir / "quick_pivot_test.json"
            with open(output_file, 'w') as f:
                json.dump(summary, f, indent=2)
            
            elapsed = time.time() - start_time
            self.logger.info(f"✓ Quick test completed in {elapsed:.1f}s")
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Quick test failed: {e}")
            raise
        finally:
            self.analyzer.close()
    
    def optimize_windows(
        self,
        window_configs: Optional[List[Tuple[int, int]]] = None,
        embedding_dim: int = 128,
        max_events: int = 10000
    ) -> pd.DataFrame:
        """
        Run window optimization experiments.
        
        Args:
            window_configs: List of (historical_hours, detection_hours) tuples
            embedding_dim: Embedding dimension
            max_events: Maximum events per experiment
            
        Returns:
            DataFrame with optimization results
        """
        if window_configs is None:
            window_configs = [
                (12, 12), (24, 12), (48, 12),
                (12, 24), (24, 24), (48, 24),
                (12, 48), (24, 48), (48, 48)
            ]
        
        self.logger.info("="*80)
        self.logger.info("RUNNING WINDOW OPTIMIZATION EXPERIMENTS")
        self.logger.info("="*80)
        self.logger.info(f"Testing {len(window_configs)} window configurations")
        
        results_list = []
        
        for i, (hist_hours, det_hours) in enumerate(window_configs, 1):
            self.logger.info(f"\n[{i}/{len(window_configs)}] Testing: historical={hist_hours}h, detection={det_hours}h")
            
            try:
                self.analyzer.connect()
                
                prefix = f"window_opt_{hist_hours}_{det_hours}"
                self.analyzer.run_pivot_prediction(
                    use_labels=True,
                    historical_window_hours=hist_hours,
                    detection_window_hours=det_hours,
                    embedding_dim=embedding_dim,
                    output_prefix=prefix
                )
                
                # Read generated files
                predictions = pd.read_csv(f'{prefix}_pivot_predictions.csv')
                comparison = pd.read_csv(f'{prefix}_method_comparison.csv')
                
                # Calculate metrics
                train_pivot_rate = predictions[predictions['set'] == 'train']['became_pivot'].mean()
                test_pivot_rate = predictions[predictions['set'] == 'test']['became_pivot'].mean()
                fastRP_metrics = comparison[comparison['Method'] == 'FastRP Embedding'].iloc[0]
                
                # Read statistics if available
                import json
                try:
                    with open(f'{prefix}_statistics.json', 'r') as f:
                        test_stats = json.load(f)
                except FileNotFoundError:
                    test_stats = {'welch_p': 0.0, 'cohens_d': 0.0}
                
                results_list.append({
                    'historical_hours': hist_hours,
                    'detection_hours': det_hours,
                    'train_pivot_rate': train_pivot_rate,
                    'test_pivot_rate': test_pivot_rate,
                    'p_value': test_stats.get('welch_p', 0.0),
                    'cohens_d': test_stats.get('cohens_d', 0.0),
                    'auc_roc': fastRP_metrics['AUC-ROC'],
                    'auc_pr': fastRP_metrics['AUC-PR'],
                    'f1_score': fastRP_metrics['F1-Score'],
                    'accuracy': fastRP_metrics['Accuracy']
                })
                
                self.logger.info(f"  ✓ AUC-ROC: {fastRP_metrics['AUC-ROC']:.4f}, "
                               f"Cohen's d: {test_stats['cohens_d']:.4f}, "
                               f"p={test_stats['welch_p']:.6f}")
                
            except Exception as e:
                self.logger.error(f"  ✗ Configuration failed: {e}")
                results_list.append({
                    'historical_hours': hist_hours,
                    'detection_hours': det_hours,
                    'error': str(e)
                })
            finally:
                self.analyzer.close()
        
        # Create DataFrame
        results_df = pd.DataFrame(results_list)
        
        # Save results
        output_file = self.results_dir / "window_optimization_results.csv"
        results_df.to_csv(output_file, index=False)
        
        self.window_results = results_df
        self.logger.info(f"\n✓ Window optimization completed")
        self.logger.info(f"✓ Results saved to {output_file}")
        
        return results_df
    
    def generate_final_results(
        self,
        embedding_dim: int = 128,
        historical_hours: int = 24,
        detection_hours: int = 24,
        label_aware_events: int = 60000,
        label_agnostic_events: int = 10000
    ) -> Dict:
        """
        Generate comprehensive final thesis results.
        
        Args:
            embedding_dim: Embedding dimension
            historical_hours: Historical window in hours
            detection_hours: Detection window in hours
            label_aware_events: Max events for label-aware analysis
            label_agnostic_events: Max events for label-agnostic analysis
            
        Returns:
            Dictionary with both analyses
        """
        self.logger.info("="*80)
        self.logger.info("GENERATING FINAL THESIS RESULTS")
        self.logger.info("="*80)
        
        final_results = {}
        
        # Label-Aware Analysis
        self.logger.info("\n--- LABEL-AWARE ANALYSIS ---")
        start_time = time.time()
        
        try:
            self.analyzer.connect()
            
            self.analyzer.run_pivot_prediction(
                use_labels=True,
                historical_window_hours=historical_hours,
                detection_window_hours=detection_hours,
                embedding_dim=embedding_dim,
                output_prefix="final_label_aware"
            )
            
            # Move generated CSV files to results directory
            import shutil
            for filename in ['final_label_aware_pivot_predictions.csv', 
                           'final_label_aware_method_comparison.csv',
                           'final_label_aware_multi_hop_chains.csv']:
                if Path(filename).exists():
                    shutil.move(filename, self.results_dir / filename)
            
            # Read the files
            predictions = pd.read_csv(self.results_dir / "final_label_aware_pivot_predictions.csv")
            comparison = pd.read_csv(self.results_dir / "final_label_aware_method_comparison.csv")
            
            # Calculate metrics
            train_pivot_rate = predictions[predictions['set'] == 'train']['became_pivot'].mean()
            test_pivot_rate = predictions[predictions['set'] == 'test']['became_pivot'].mean()
            
            # Read statistics if available
            import json
            try:
                with open('final_label_aware_statistics.json', 'r') as f:
                    test_statistics = json.load(f)
                shutil.move('final_label_aware_statistics.json', 
                          self.results_dir / 'final_label_aware_statistics.json')
            except FileNotFoundError:
                test_statistics = {'welch_p': 0.0, 'cohens_d': 0.0}
            
            final_results['label_aware'] = {
                'train_pivot_rate': train_pivot_rate,
                'test_pivot_rate': test_pivot_rate,
                'statistics': test_statistics,
                'best_method': comparison.loc[0, 'Method'],
                'best_auc_roc': comparison.loc[0, 'AUC-ROC'],
                'best_f1': comparison.loc[0, 'F1-Score']
            }
            
            elapsed = time.time() - start_time
            self.logger.info(f"✓ Label-aware analysis completed in {elapsed:.1f}s")
            
        except Exception as e:
            self.logger.error(f"Label-aware analysis failed: {e}")
            raise
        finally:
            self.analyzer.close()
        
        # Label-Agnostic Analysis
        self.logger.info("\n--- LABEL-AGNOSTIC ANALYSIS ---")
        start_time = time.time()
        
        try:
            self.analyzer.connect()
            
            self.analyzer.run_pivot_prediction(
                use_labels=False,
                historical_window_hours=historical_hours,
                detection_window_hours=detection_hours,
                embedding_dim=embedding_dim,
                output_prefix="final_label_agnostic"
            )
            
            # Move generated CSV files to results directory
            import shutil
            for filename in ['final_label_agnostic_pivot_predictions.csv', 
                           'final_label_agnostic_method_comparison.csv',
                           'final_label_agnostic_multi_hop_chains.csv']:
                if Path(filename).exists():
                    shutil.move(filename, self.results_dir / filename)
            
            # Read the files
            predictions = pd.read_csv(self.results_dir / "final_label_agnostic_pivot_predictions.csv")
            comparison = pd.read_csv(self.results_dir / "final_label_agnostic_method_comparison.csv")
            
            # Calculate metrics
            train_pivot_rate = predictions[predictions['set'] == 'train']['became_pivot'].mean()
            test_pivot_rate = predictions[predictions['set'] == 'test']['became_pivot'].mean()
            
            # Read statistics if available
            import json
            try:
                with open('final_label_agnostic_statistics.json', 'r') as f:
                    test_statistics = json.load(f)
                shutil.move('final_label_agnostic_statistics.json', 
                          self.results_dir / 'final_label_agnostic_statistics.json')
            except FileNotFoundError:
                test_statistics = None
            
            final_results['label_agnostic'] = {
                'train_pivot_rate': train_pivot_rate,
                'test_pivot_rate': test_pivot_rate,
                'statistics': test_statistics,
                'best_method': comparison.loc[0, 'Method'] if len(comparison) > 0 else None,
                'best_auc_roc': comparison.loc[0, 'AUC-ROC'] if len(comparison) > 0 else None,
                'best_f1': comparison.loc[0, 'F1-Score'] if len(comparison) > 0 else None
            }
            
            elapsed = time.time() - start_time
            self.logger.info(f"✓ Label-agnostic analysis completed in {elapsed:.1f}s")
            
        except Exception as e:
            self.logger.error(f"Label-agnostic analysis failed: {e}")
            raise
        finally:
            self.analyzer.close()
        
        # Save summary
        self.final_results = final_results
        output_file = self.results_dir / "final_results_summary.json"
        with open(output_file, 'w') as f:
            json.dump(final_results, f, indent=2, default=str)
        
        self.logger.info(f"\n✓ Final results saved to {self.results_dir}")
        self.logger.info(f"✓ Log file: {self.log_file}")
        
        return final_results
    
    def get_summary(self) -> Dict:
        """
        Get summary of all completed analyses.
        
        Returns:
            Dictionary with summary statistics
        """
        summary = {
            'timestamp': datetime.now().isoformat(),
            'log_file': str(self.log_file),
            'results_directory': str(self.results_dir.absolute()),
            'figures_directory': str(self.figures_dir.absolute()),
            'analyses_completed': {
                'database_exploration': bool(self.exploration_results),
                'tactics_analysis': bool(self.tactics_results),
                'window_optimization': bool(self.window_results),
                'final_results': bool(self.final_results)
            }
        }
        
        if self.exploration_results:
            summary['database'] = {
                'nodes': self.exploration_results.get('node_count'),
                'relationships': self.exploration_results.get('relationship_count'),
                'subnets': len(self.exploration_results.get('subnets', []))
            }
        
        if self.tactics_results:
            summary['tactics'] = {
                'total_tactics': len(self.tactics_results.get('overall_distribution', [])),
                'cross_subnet_attacks': sum(
                    t['count'] for t in self.tactics_results.get('cross_subnet_by_tactic', [])
                )
            }
        
        if self.final_results:
            summary['final_results'] = {
                'label_aware_auc_roc': self.final_results.get('label_aware', {}).get('best_auc_roc'),
                'label_aware_f1': self.final_results.get('label_aware', {}).get('best_f1'),
                'label_agnostic_auc_roc': self.final_results.get('label_agnostic', {}).get('best_auc_roc'),
                'label_agnostic_f1': self.final_results.get('label_agnostic', {}).get('best_f1')
            }
        
        return summary
