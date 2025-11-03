from .base import Neo4jConnection
from typing import Optional, Dict, List, Tuple
import pandas as pd
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.metrics import roc_curve, auc, confusion_matrix, precision_recall_curve
import time
import warnings
import json
from tqdm import tqdm
from scipy import stats
from collections import defaultdict
import ipaddress

warnings.filterwarnings('ignore')


class TemporalWindowAnalyzer(Neo4jConnection):
    """
    Analyzes optimal temporal windows for pivot behavior detection.
    Determines appropriate historical observation periods and pivot detection windows.
    """
    
    def __init__(self, connection: Optional[Neo4jConnection] = None,
                 uri="bolt://localhost:7687", user="neo4j", password=None,
                 database="neo4j", container_name="neo4j_temporal_analyzer", **kwargs):
        if connection is not None:
            self._shared_connection = connection
            self.driver = connection.driver
            self.uri = connection.uri
            self.user = connection.user
            self.password = connection.password
            self.database = connection.database
            self.container_name = connection.container_name
            self._owns_driver = False
        else:
            super().__init__(uri=uri, user=user, password=password,
                             database=database, container_name=container_name, **kwargs)
            self._shared_connection = None
            self._owns_driver = True

    def start(self, password: Optional[str] = None):
        if getattr(self, "_shared_connection", None):
            return self._shared_connection.start(password)
        return super().start(password)

    def stop(self):
        if getattr(self, "_shared_connection", None):
            return self._shared_connection.stop()
        return super().stop()

    def remove(self):
        if getattr(self, "_shared_connection", None):
            return self._shared_connection.remove()
        return super().remove()

    def restart(self):
        if getattr(self, "_shared_connection", None):
            return self._shared_connection.restart()
        return super().restart()

    def connect(self):
        if getattr(self, "_shared_connection", None):
            ok = self._shared_connection.connect()
            self.driver = self._shared_connection.driver
            return ok
        return super().connect()

    def close(self):
        if getattr(self, "_shared_connection", None):
            print("Shared connection managed by controller; not closing driver here.")
            return
        return super().close()
    
    def run_analysis(self, output_filepath="temporal_window_analysis.json"):
        """Runs comprehensive temporal window analysis."""
        try:
            if not self.connect():
                print("Could not connect to Neo4j; aborting temporal analysis.")
                return
            
            print("\n" + "="*80)
            print("TEMPORAL WINDOW ANALYSIS FOR PIVOT DETECTION")
            print("="*80)
            
            # Analyze pivot timing distributions
            pivot_timings = self.analyze_pivot_timing_distribution()
            
            # Test different historical windows
            window_results = self.test_historical_windows([3600, 12*3600, 24*3600, 48*3600])
            
            # Test different pivot detection windows
            detection_results = self.test_pivot_detection_windows([3600, 6*3600, 12*3600, 24*3600, 48*3600])
            
            # Generate recommendations
            recommendations = self.generate_recommendations(pivot_timings, window_results, detection_results)
            
            # Save results
            results = {
                'pivot_timings': pivot_timings,
                'historical_window_tests': window_results,
                'detection_window_tests': detection_results,
                'recommendations': recommendations
            }
            
            with open(output_filepath, 'w') as f:
                json.dump(results, f, indent=4)
            
            print(f"\n✓ Results saved to {output_filepath}")
            self.visualize_temporal_analysis(results)
            
        finally:
            self.close()
    
    def analyze_pivot_timing_distribution(self):
        """Analyze the time distribution between reconnaissance and pivot attacks."""
        print("\n--- Analyzing Pivot Timing Distribution ---")
        
        with self.driver.session(database=self.database) as session:
            query = """
            MATCH (a:IP)-[r1:CONNECTS]->(b:IP)-[r2:CONNECTS]->(c:IP)
            WHERE r1.is_attack = 1 AND r1.tactic = 'Reconnaissance'
              AND r2.is_attack = 1 AND r2.timestamp > r1.timestamp
              AND a <> c
            WITH (r2.timestamp - r1.timestamp) as time_diff
            WHERE time_diff > 0 AND time_diff < 7*24*3600  // Within 1 week
            RETURN 
                time_diff / 3600 as hours_to_pivot,
                count(*) as frequency
            ORDER BY hours_to_pivot
            """
            
            result = session.run(query).data()
            
            if not result:
                print("  ⚠ No pivot timing data found")
                return {}
            
            df = pd.DataFrame(result)
            
            percentiles = [10, 25, 50, 75, 90, 95, 99]
            stats_dict = {
                'mean_hours': float(df['hours_to_pivot'].mean()),
                'median_hours': float(df['hours_to_pivot'].median()),
                'std_hours': float(df['hours_to_pivot'].std()),
                'percentiles': {f'p{p}': float(np.percentile(df['hours_to_pivot'], p)) for p in percentiles},
                'total_pivots': int(df['frequency'].sum())
            }
            
            print(f"\n  Pivot Timing Statistics:")
            print(f"    Total pivots analyzed: {stats_dict['total_pivots']:,}")
            print(f"    Mean time to pivot: {stats_dict['mean_hours']:.2f} hours")
            print(f"    Median time to pivot: {stats_dict['median_hours']:.2f} hours")
            print(f"\n  Percentiles:")
            for p, val in stats_dict['percentiles'].items():
                print(f"    {p}: {val:.2f} hours")
            
            return stats_dict
    
    def test_historical_windows(self, window_sizes: List[int]):
        """Test different historical window sizes for feature extraction."""
        print("\n--- Testing Historical Window Sizes ---")
        
        results = []
        
        with self.driver.session(database=self.database) as session:
            for window_sec in tqdm(window_sizes, desc="Testing windows"):
                window_hours = window_sec / 3600
                
                query = """
                MATCH (victim:IP)
                WHERE exists((victim)<-[:CONNECTS]-(:IP))
                WITH victim LIMIT 1000  // Sample for speed
                
                OPTIONAL MATCH (victim)<-[r_in:CONNECTS]-(src:IP)
                WHERE r_in.timestamp >= timestamp() - $window_sec
                WITH victim, count(r_in) as in_degree, 
                     sum(CASE WHEN r_in.is_attack = 1 THEN 1 ELSE 0 END) as attacks_received
                
                OPTIONAL MATCH (victim)-[r_out:CONNECTS]->(dst:IP)
                WHERE r_out.timestamp >= timestamp() - $window_sec
                WITH victim, in_degree, attacks_received, count(r_out) as out_degree
                
                WHERE in_degree > 0 OR out_degree > 0
                RETURN 
                    avg(in_degree + out_degree) as avg_degree,
                    stdDev(in_degree + out_degree) as std_degree,
                    avg(attacks_received) as avg_attacks,
                    count(victim) as sample_size
                """
                
                result = session.run(query, window_sec=window_sec).single()
                
                if result:
                    results.append({
                        'window_hours': window_hours,
                        'window_seconds': window_sec,
                        'avg_degree': float(result['avg_degree'] or 0),
                        'std_degree': float(result['std_degree'] or 0),
                        'avg_attacks': float(result['avg_attacks'] or 0),
                        'sample_size': int(result['sample_size'] or 0)
                    })
        
        print("\n  Window Size Comparison:")
        for r in results:
            print(f"    {r['window_hours']:.0f}h: avg_degree={r['avg_degree']:.2f}, "
                  f"avg_attacks={r['avg_attacks']:.2f}, samples={r['sample_size']}")
        
        return results
    
    def test_pivot_detection_windows(self, window_sizes: List[int]):
        """Test different time windows for detecting pivot behavior."""
        print("\n--- Testing Pivot Detection Windows ---")
        
        results = []
        
        with self.driver.session(database=self.database) as session:
            for window_sec in tqdm(window_sizes, desc="Testing detection windows"):
                window_hours = window_sec / 3600
                
                query = """
                MATCH (a:IP)-[r1:CONNECTS]->(b:IP)
                WHERE r1.is_attack = 1 AND r1.tactic = 'Reconnaissance'
                WITH b, r1.timestamp as recon_time
                LIMIT 500  // Sample for speed
                
                OPTIONAL MATCH (b)-[r2:CONNECTS]->(c:IP)
                WHERE r2.is_attack = 1 
                  AND r2.timestamp > recon_time
                  AND r2.timestamp <= recon_time + $window_sec
                
                WITH b, recon_time, count(r2) as pivot_attacks
                RETURN 
                    sum(CASE WHEN pivot_attacks > 0 THEN 1 ELSE 0 END) as pivots_detected,
                    count(b) as total_recon_victims,
                    avg(pivot_attacks) as avg_pivot_attacks
                """
                
                result = session.run(query, window_sec=window_sec).single()
                
                if result:
                    total = int(result['total_recon_victims'] or 0)
                    detected = int(result['pivots_detected'] or 0)
                    rate = (detected / total * 100) if total > 0 else 0
                    
                    results.append({
                        'window_hours': window_hours,
                        'window_seconds': window_sec,
                        'pivots_detected': detected,
                        'total_victims': total,
                        'detection_rate': rate,
                        'avg_pivot_attacks': float(result['avg_pivot_attacks'] or 0)
                    })
        
        print("\n  Detection Window Comparison:")
        for r in results:
            print(f"    {r['window_hours']:.0f}h: {r['pivots_detected']}/{r['total_victims']} "
                  f"({r['detection_rate']:.1f}%) pivots detected")
        
        return results
    
    def generate_recommendations(self, pivot_timings, window_results, detection_results):
        """Generate recommendations for optimal window sizes."""
        print("\n--- Generating Recommendations ---")
        
        recommendations = {}
        
        # Recommend historical window (capture 75th percentile of activity)
        if window_results:
            # Choose window with good degree coverage but not too sparse
            recommended_hist = max(
                [w for w in window_results if w['avg_degree'] > 5],
                key=lambda x: x['avg_degree'],
                default=window_results[2] if len(window_results) > 2 else window_results[0]
            )
            recommendations['historical_window'] = {
                'seconds': recommended_hist['window_seconds'],
                'hours': recommended_hist['window_hours'],
                'rationale': f"Captures sufficient activity (avg degree: {recommended_hist['avg_degree']:.2f})"
            }
        
        # Recommend detection window (capture 75-90th percentile of pivots)
        if pivot_timings and 'percentiles' in pivot_timings:
            p75_hours = pivot_timings['percentiles']['p75']
            p90_hours = pivot_timings['percentiles']['p90']
            
            # Find detection window closest to P75-P90 range
            if detection_results:
                recommended_det = min(
                    detection_results,
                    key=lambda x: abs(x['window_hours'] - p75_hours)
                )
                recommendations['detection_window'] = {
                    'seconds': recommended_det['window_seconds'],
                    'hours': recommended_det['window_hours'],
                    'rationale': f"Captures {recommended_det['detection_rate']:.1f}% of pivots, "
                                f"aligned with P75 timing ({p75_hours:.1f}h)"
                }
        
        print("\n  Recommendations:")
        for key, rec in recommendations.items():
            print(f"    {key}: {rec['hours']:.0f} hours ({rec['seconds']} seconds)")
            print(f"      Rationale: {rec['rationale']}")
        
        return recommendations
    
    def visualize_temporal_analysis(self, results):
        """Generate visualizations for temporal analysis."""
        print("\n--- Generating Visualizations ---")
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        # Plot 1: Pivot timing distribution (if available)
        ax = axes[0, 0]
        if 'pivot_timings' in results and 'percentiles' in results['pivot_timings']:
            percentiles = results['pivot_timings']['percentiles']
            p_names = list(percentiles.keys())
            p_values = list(percentiles.values())
            
            ax.bar(p_names, p_values, color='steelblue', edgecolor='black')
            ax.axhline(results['pivot_timings']['median_hours'], color='red', 
                      linestyle='--', label=f"Median: {results['pivot_timings']['median_hours']:.1f}h")
            ax.set_xlabel('Percentile')
            ax.set_ylabel('Hours to Pivot')
            ax.set_title('Pivot Timing Distribution')
            ax.legend()
            ax.grid(alpha=0.3, axis='y')
        
        # Plot 2: Historical window comparison
        ax = axes[0, 1]
        if 'historical_window_tests' in results and results['historical_window_tests']:
            df_hist = pd.DataFrame(results['historical_window_tests'])
            ax.plot(df_hist['window_hours'], df_hist['avg_degree'], 
                   marker='o', linewidth=2, label='Avg Degree')
            ax.set_xlabel('Historical Window (hours)')
            ax.set_ylabel('Average Degree')
            ax.set_title('Activity Capture vs Window Size')
            ax.grid(alpha=0.3)
            ax.legend()
        
        # Plot 3: Detection window comparison
        ax = axes[1, 0]
        if 'detection_window_tests' in results and results['detection_window_tests']:
            df_det = pd.DataFrame(results['detection_window_tests'])
            ax.plot(df_det['window_hours'], df_det['detection_rate'], 
                   marker='o', linewidth=2, color='coral')
            ax.set_xlabel('Detection Window (hours)')
            ax.set_ylabel('Pivot Detection Rate (%)')
            ax.set_title('Pivot Detection Rate vs Window Size')
            ax.grid(alpha=0.3)
        
        # Plot 4: Recommendations summary
        ax = axes[1, 1]
        ax.axis('off')
        
        if 'recommendations' in results:
            rec_text = "RECOMMENDED WINDOWS\n\n"
            for key, rec in results['recommendations'].items():
                rec_text += f"{key.replace('_', ' ').title()}:\n"
                rec_text += f"  {rec['hours']:.0f} hours\n"
                rec_text += f"  {rec['rationale']}\n\n"
            
            ax.text(0.1, 0.9, rec_text, transform=ax.transAxes,
                   fontsize=12, verticalalignment='top',
                   bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        plt.tight_layout()
        plt.savefig('temporal_window_analysis.png', dpi=150, bbox_inches='tight')
        print("  ✓ Saved visualizations to 'temporal_window_analysis.png'")


class SubnetPivotAnalyzer(Neo4jConnection):
    """
    Subnet-aware pivot detection using FastRP embeddings.
    Implements both label-aware and label-agnostic analysis modes.
    """
    
    def __init__(self, connection: Optional[Neo4jConnection] = None,
                 uri="bolt://localhost:7687", user="neo4j", password="ubuntuubuntu",
                 database="neo4j", container_name="neo4j_subnet_analyzer", **kwargs):
        if connection is not None:
            self._shared_connection = connection
            self.driver = connection.driver
            self.uri = connection.uri
            self.user = connection.user
            self.password = connection.password
            self.database = connection.database
            self.container_name = connection.container_name
            self._owns_driver = False
        else:
            super().__init__(uri=uri, user=user, password=password,
                             database=database, container_name=container_name, **kwargs)
            self._shared_connection = None
            self._owns_driver = True

    def start(self, password: Optional[str] = None):
        if getattr(self, "_shared_connection", None):
            return self._shared_connection.start(password)
        return super().start(password)

    def stop(self):
        if getattr(self, "_shared_connection", None):
            return self._shared_connection.stop()
        return super().stop()

    def remove(self):
        if getattr(self, "_shared_connection", None):
            return self._shared_connection.remove()
        return super().remove()

    def restart(self):
        if getattr(self, "_shared_connection", None):
            return self._shared_connection.restart()
        return super().restart()

    def connect(self):
        if getattr(self, "_shared_connection", None):
            ok = self._shared_connection.connect()
            self.driver = self._shared_connection.driver
            return ok
        return super().connect()

    def close(self):
        if getattr(self, "_shared_connection", None):
            print("Shared connection managed by controller; not closing driver here.")
            return
        return super().close()
    
    def explore_database(self, output_file="database_exploration.json"):
        """Comprehensive database exploration to understand attack patterns."""
        
        try:
            if not self.connect():
                print("Failed to connect to database")
                return
            
            print("\n" + "="*80)
            print("DATABASE EXPLORATION & ATTACK PATTERN ANALYSIS")
            print("="*80)
            
            exploration = {}
            
            with self.driver.session(database=self.database) as session:
                
                # 1. Basic Statistics
                print("\n--- Basic Database Statistics ---")
                stats_query = """
                MATCH (n:IP)
                OPTIONAL MATCH (n)-[r:CONNECTS]->()
                WITH count(DISTINCT n) as total_ips,
                     count(r) as total_connections,
                     count(DISTINCT n.subnet) as total_subnets
                RETURN total_ips, total_connections, total_subnets
                """
                stats = session.run(stats_query).single()
                exploration['basic_stats'] = {
                    'total_ips': stats['total_ips'],
                    'total_connections': stats['total_connections'],
                    'total_subnets': stats['total_subnets']
                }
                print(f"  Total IPs: {stats['total_ips']:,}")
                print(f"  Total Connections: {stats['total_connections']:,}")
                print(f"  Total Subnets: {stats['total_subnets']}")
                
                # 2. Attack Label Distribution
                print("\n--- Attack Label Distribution ---")
                label_query = """
                MATCH ()-[r:CONNECTS]->()
                WITH r.is_attack as is_attack,
                     r.tactic as tactic,
                     r.technique as technique
                RETURN is_attack,
                       count(*) as count,
                       collect(DISTINCT tactic)[0..10] as sample_tactics,
                       collect(DISTINCT technique)[0..10] as sample_techniques
                ORDER BY is_attack
                """
                label_results = session.run(label_query).data()
                exploration['attack_distribution'] = label_results
                
                for record in label_results:
                    is_attack = "ATTACK" if record['is_attack'] == 1 else "BENIGN"
                    print(f"\n  {is_attack}: {record['count']:,} connections")
                    if record['is_attack'] == 1:
                        print(f"    Sample Tactics: {', '.join([str(t) for t in record['sample_tactics'] if t])}")
                        print(f"    Sample Techniques: {', '.join([str(t) for t in record['sample_techniques'] if t])}")
                
                # 3. Reconnaissance Pattern Analysis
                print("\n--- Reconnaissance Attack Patterns ---")
                recon_query = """
                MATCH (attacker:IP)-[r:CONNECTS]->(victim:IP)
                WHERE r.is_attack = 1 AND r.tactic = 'Reconnaissance'
                WITH attacker, victim, r
                ORDER BY r.timestamp
                WITH attacker.subnet as attacker_subnet,
                     victim.subnet as victim_subnet,
                     count(r) as recon_count,
                     min(r.timestamp) as first_recon,
                     max(r.timestamp) as last_recon,
                     collect(DISTINCT attacker.address)[0..3] as sample_attackers,
                     collect(DISTINCT victim.address)[0..3] as sample_victims
                RETURN attacker_subnet, victim_subnet, recon_count,
                       first_recon, last_recon, sample_attackers, sample_victims
                ORDER BY recon_count DESC
                LIMIT 20
                """
                recon_results = session.run(recon_query).data()
                exploration['reconnaissance_patterns'] = recon_results
                
                print(f"  Found {len(recon_results)} subnet-to-subnet reconnaissance patterns")
                print("\n  Top Reconnaissance Patterns:")
                for i, rec in enumerate(recon_results[:5], 1):
                    print(f"    {i}. {rec['attacker_subnet']} → {rec['victim_subnet']}: {rec['recon_count']} scans")
                
                # 4. Lateral Movement Analysis
                print("\n--- Lateral Movement Attack Patterns ---")
                lateral_query = """
                MATCH (attacker:IP)-[r:CONNECTS]->(victim:IP)
                WHERE r.is_attack = 1 
                  AND r.tactic IN ['Lateral Movement', 'Execution', 'Command and Control']
                WITH attacker.subnet as attacker_subnet,
                     victim.subnet as victim_subnet,
                     r.tactic as tactic,
                     count(r) as attack_count,
                     collect(DISTINCT r.technique)[0..5] as techniques
                RETURN attacker_subnet, victim_subnet, tactic, attack_count, techniques
                ORDER BY attack_count DESC
                LIMIT 20
                """
                lateral_results = session.run(lateral_query).data()
                exploration['lateral_movement_patterns'] = lateral_results
                
                print(f"  Found {len(lateral_results)} lateral movement patterns")
                print("\n  Top Lateral Movement Patterns:")
                for i, lat in enumerate(lateral_results[:5], 1):
                    print(f"    {i}. {lat['attacker_subnet']} → {lat['victim_subnet']}")
                    print(f"       Tactic: {lat['tactic']}, Count: {lat['attack_count']}")
                    print(f"       Techniques: {', '.join([str(t) for t in lat['techniques'] if t])}")
                
                # 5. Attack Chain Analysis (Recon → Pivot)
                print("\n--- Attack Chain Analysis (Reconnaissance → Lateral Movement) ---")
                chain_query = """
                MATCH (a:IP)-[r1:CONNECTS]->(v:IP)
                WHERE r1.is_attack = 1 AND r1.tactic = 'Reconnaissance'
                
                WITH v.subnet as victim_subnet, 
                     r1.timestamp as recon_time,
                     a.subnet as attacker_subnet
                ORDER BY recon_time
                LIMIT 1000
                
                MATCH (pivot:IP)-[r2:CONNECTS]->(target:IP)
                WHERE pivot.subnet = victim_subnet
                  AND r2.is_attack = 1
                  AND r2.timestamp > recon_time
                  AND r2.timestamp <= recon_time + 86400  // Within 24 hours
                  AND r2.tactic IN ['Lateral Movement', 'Execution', 'Command and Control', 'Credential Access']
                  AND target.subnet <> victim_subnet  // Cross-subnet attack
                
                WITH victim_subnet, 
                     attacker_subnet,
                     count(DISTINCT r2) as pivot_attacks,
                     collect(DISTINCT r2.tactic)[0..5] as pivot_tactics,
                     collect(DISTINCT target.subnet)[0..5] as target_subnets,
                     min(recon_time) as first_recon,
                     min(r2.timestamp) as first_pivot,
                     (min(r2.timestamp) - min(recon_time)) as time_to_pivot
                
                RETURN victim_subnet,
                       attacker_subnet,
                       pivot_attacks,
                       pivot_tactics,
                       target_subnets,
                       first_recon,
                       first_pivot,
                       time_to_pivot / 3600.0 as hours_to_pivot
                ORDER BY pivot_attacks DESC
                LIMIT 50
                """
                chain_results = session.run(chain_query).data()
                exploration['attack_chains'] = chain_results
                
                print(f"  Found {len(chain_results)} true attack chains (Recon → Pivot)")
                if chain_results:
                    print("\n  Sample Attack Chains:")
                    for i, chain in enumerate(chain_results[:10], 1):
                        print(f"    {i}. {chain['attacker_subnet']} scans → {chain['victim_subnet']} pivots → {chain['target_subnets']}")
                        print(f"       Time to pivot: {chain['hours_to_pivot']:.1f} hours, Pivot attacks: {chain['pivot_attacks']}")
                        print(f"       Pivot tactics: {', '.join([str(t) for t in chain['pivot_tactics'] if t])}")
                else:
                    print("  ⚠ WARNING: No true attack chains found!")
                    print("    This explains why pivot detection is finding everything as pivots.")
                
                # 6. Cross-Subnet Traffic Analysis
                print("\n--- Cross-Subnet Traffic Patterns ---")
                cross_subnet_query = """
                MATCH (src:IP)-[r:CONNECTS]->(dst:IP)
                WHERE src.subnet <> dst.subnet
                WITH src.subnet as src_subnet,
                     dst.subnet as dst_subnet,
                     sum(CASE WHEN r.is_attack = 1 THEN 1 ELSE 0 END) as attack_count,
                     sum(CASE WHEN r.is_attack = 0 THEN 1 ELSE 0 END) as benign_count,
                     count(r) as total_count
                RETURN src_subnet, dst_subnet, attack_count, benign_count, total_count
                ORDER BY total_count DESC
                LIMIT 20
                """
                cross_subnet_results = session.run(cross_subnet_query).data()
                exploration['cross_subnet_patterns'] = cross_subnet_results
                
                print(f"  Top Cross-Subnet Communications:")
                for i, cs in enumerate(cross_subnet_results[:5], 1):
                    attack_pct = (cs['attack_count'] / cs['total_count'] * 100) if cs['total_count'] > 0 else 0
                    print(f"    {i}. {cs['src_subnet']} → {cs['dst_subnet']}: {cs['total_count']:,} connections")
                    print(f"       Attacks: {cs['attack_count']:,} ({attack_pct:.1f}%), Benign: {cs['benign_count']:,}")
                
                # 7. Temporal Analysis
                print("\n--- Temporal Attack Distribution ---")
                temporal_query = """
                MATCH ()-[r:CONNECTS]->()
                WHERE r.is_attack = 1
                WITH r.timestamp / 3600 as hour_bucket
                RETURN min(hour_bucket * 3600) as time_period,
                       count(*) as attack_count
                ORDER BY time_period
                LIMIT 100
                """
                temporal_results = session.run(temporal_query).data()
                exploration['temporal_distribution'] = temporal_results
                
                if temporal_results:
                    print(f"  Attack activity spans {len(temporal_results)} time buckets")
                    print(f"  First attacks: timestamp {temporal_results[0]['time_period']}")
                    print(f"  Latest attacks: timestamp {temporal_results[-1]['time_period']}")
                    print(f"  Peak hour: {max(temporal_results, key=lambda x: x['attack_count'])['attack_count']} attacks")
            
            # Save to JSON
            with open(output_file, 'w') as f:
                json.dump(exploration, f, indent=2, default=str)
            
            print(f"\n✓ Exploration results saved to {output_file}")
            
            # Generate recommendations
            print("\n" + "="*80)
            print("RECOMMENDATIONS FOR PIVOT DETECTION")
            print("="*80)
            
            true_chains = len(exploration.get('attack_chains', []))
            if true_chains == 0:
                print("\n⚠ CRITICAL: No true attack chains found in the data!")
                print("   Your dataset may not contain actual pivot behavior.")
                print("   Consider:")
                print("   1. Check if lateral movement attacks exist in your data")
                print("   2. Verify MITRE ATT&CK labels are correct")
                print("   3. Look for cross-subnet attacks after reconnaissance")
            elif true_chains < 100:
                print(f"\n⚠ WARNING: Only {true_chains} true attack chains found")
                print("   This is a small number for training. Consider:")
                print("   1. Expanding the time window (currently 24 hours)")
                print("   2. Including more attack tactics as 'pivots'")
            else:
                print(f"\n✓ Found {true_chains} attack chains - good for analysis!")
            
            return exploration
            
        finally:
            self.close()
    
    @staticmethod
    def ip_to_subnet(ip_address: str, prefix_len: int = 24) -> str:
        """Convert IP address to subnet notation (e.g., '192.168.1.0/24')."""
        try:
            network = ipaddress.ip_network(f"{ip_address}/{prefix_len}", strict=False)
            return str(network)
        except:
            return None
    
    def add_subnet_labels(self):
        """Add subnet property and numeric subnet ID to all IP nodes."""
        print("\n--- Adding Subnet Labels to IP Nodes ---")
        
        with self.driver.session(database=self.database) as session:
            # First pass: Add subnet strings
            print("  Adding subnet strings...")
            subnet_query = """
            MATCH (n:IP)
            WHERE n.subnet IS NULL
            WITH n, split(n.address, '.') as parts
            SET n.subnet = parts[0] + '.' + parts[1] + '.' + parts[2] + '.0/24'
            """
            session.run(subnet_query)
            
            # Second pass: Create subnet mapping
            print("  Creating subnet ID mapping...")
            # Collect distinct subnet strings and assign deterministic integer IDs
            # using UNWIND over a range (avoids unsupported window functions)
            mapping_query = """
            MATCH (n:IP)
            WITH collect(DISTINCT n.subnet) AS subs
            UNWIND range(0, size(subs)-1) AS idx
            WITH subs[idx] AS subnet, idx AS subnet_id, subs
            MATCH (m:IP)
            WHERE m.subnet = subnet
            SET m.subnet_id = subnet_id
            RETURN size(subs) AS subnet_count
            """
            
            result = session.run(mapping_query).single()
            print(f"  ✓ Assigned numeric IDs to {result['subnet_count']} subnets")
            
            # Create indices
            session.run("CREATE INDEX subnet_index IF NOT EXISTS FOR (n:IP) ON (n.subnet)")
            session.run("CREATE INDEX subnet_id_index IF NOT EXISTS FOR (n:IP) ON (n.subnet_id)")
            print("  ✓ Subnet indices created")
    
    def run_full_analysis(self, mode='both', historical_window_hours=24, 
                         detection_window_hours=24, embedding_dim=128):
        """
        Run complete subnet-aware pivot prediction analysis.
        
        Args:
            mode: 'label_aware', 'label_agnostic', or 'both'
            historical_window_hours: Hours of history to consider before reconnaissance
            detection_window_hours: Hours after reconnaissance to check for pivot behavior
            embedding_dim: Dimension of FastRP embeddings
        """
        try:
            if not self.connect():
                print("Could not connect to Neo4j; aborting analysis.")
                return
            
            print("\n" + "="*80)
            print("SUBNET-AWARE PIVOT PREDICTION WITH FASTRP EMBEDDINGS")
            print("="*80)
            print(f"  Mode: {mode.upper()}")
            print(f"  Historical window: {historical_window_hours} hours")
            print(f"  Detection window: {detection_window_hours} hours")
            print(f"  Embedding dimension: {embedding_dim}")
            
            # Add subnet labels if not already present
            self.add_subnet_labels()
            
            # Run analysis based on mode
            if mode in ['label_aware', 'both']:
                print("\n" + "="*80)
                print("RUNNING LABEL-AWARE ANALYSIS")
                print("="*80)
                self.run_pivot_prediction(
                    use_labels=True,
                    historical_window_hours=historical_window_hours,
                    detection_window_hours=detection_window_hours,
                    embedding_dim=embedding_dim,
                    output_prefix='label_aware'
                )
            
            if mode in ['label_agnostic', 'both']:
                print("\n" + "="*80)
                print("RUNNING LABEL-AGNOSTIC ANALYSIS")
                print("="*80)
                self.run_pivot_prediction(
                    use_labels=False,
                    historical_window_hours=historical_window_hours,
                    detection_window_hours=detection_window_hours,
                    embedding_dim=embedding_dim,
                    output_prefix='label_agnostic'
                )
            
            # Compare results if both modes were run
            if mode == 'both':
                self.compare_analysis_modes()
            
        finally:
            self.close()
    
    def drop_graph_projection(self, graph_name: str) -> bool:
        """Safely drop a graph projection if it exists."""
        with self.driver.session(database=self.database) as session:
            try:
                # Direct drop attempt first
                drop_query = "CALL gds.graph.drop($name)"
                session.run(drop_query, name=graph_name)
                return True
            except Exception:
                try:
                    # Check if it exists
                    exists_query = "CALL gds.graph.exists($name) YIELD exists RETURN exists"
                    result = session.run(exists_query, name=graph_name).single()
                    if not result or not result['exists']:
                        return True
                    print(f"  ⚠ Failed to drop projection: {graph_name}")
                    return False
                except Exception as e:
                    print(f"  ⚠ Error checking projection {graph_name}: {str(e)}")
                    return False

    def drop_all_graph_projections(self):
        """Drop all existing graph projections."""
        print("\n--- Dropping All Existing Graph Projections ---")
        
        with self.driver.session(database=self.database) as session:
            try:
                # Check existing projections first
                exists_query = """
                CALL gds.graph.exists($name)
                YIELD exists
                RETURN exists
                """
                
                # Try to drop structure projection if it exists
                result = session.run(exists_query, name=f"pivot_graph_labeled_structure").single()
                if result and result['exists']:
                    session.run("CALL gds.graph.drop($name)", 
                              name="pivot_graph_labeled_structure")
                    print(f"  ✓ Dropped structure projection")
                
                # Try to drop labels projection if it exists
                result = session.run(exists_query, name=f"pivot_graph_labeled_labels").single()
                if result and result['exists']:
                    session.run("CALL gds.graph.drop($name)", 
                              name="pivot_graph_labeled_labels")
                    print(f"  ✓ Dropped labels projection")
                
                # Try to drop unlabeled projection if it exists
                result = session.run(exists_query, name=f"pivot_graph_unlabeled").single()
                if result and result['exists']:
                    session.run("CALL gds.graph.drop($name)",
                              name="pivot_graph_unlabeled")
                    print(f"  ✓ Dropped unlabeled projection")
                
            except Exception as e:
                print(f"  ⚠ Warning: {str(e)}")
                # Continue execution even if cleanup fails
    
    def create_graph_projection(self, projection_name: str, use_labels: bool):
        """Create Neo4j GDS graph projection for FastRP."""
        print(f"\n--- Creating Graph Projection: {projection_name} ---")
        
        # First ensure all existing projections are cleaned up
        self.drop_all_graph_projections()
        
        with self.driver.session(database=self.database) as session:
            # Create projection with or without relationship properties
            if use_labels:
                # Ensure old projections are dropped
                struct_name = f"{projection_name}_structure"
                
                # Check if projection exists before trying to drop
                try:
                    exists_result = session.run(
                        "CALL gds.graph.exists($name) YIELD exists RETURN exists",
                        name=struct_name
                    ).single()
                    
                    if exists_result and exists_result['exists']:
                        session.run("CALL gds.graph.drop($name)", name=struct_name)
                        print(f"  ✓ Dropped existing structure projection")
                except Exception as e:
                    # Ignore errors - projection doesn't exist or already dropped
                    pass
                
                # Create structure projection with UNDIRECTED orientation
                # (required for clustering coefficient)
                create_query = """
                CALL gds.graph.project(
                    $name,
                    'IP',
                    {
                        CONNECTS: {
                            orientation: 'UNDIRECTED'
                        }
                    },
                    {
                        nodeProperties: ['subnet_id']
                    }
                )
                YIELD graphName, nodeCount, relationshipCount
                RETURN graphName, nodeCount, relationshipCount
                """
                result = session.run(create_query, name=struct_name).single()
                if result:
                    print(f"  ✓ Created structure projection: {result['graphName']}")
                    print(f"    Nodes: {result['nodeCount']:,}")
                    print(f"    Relationships: {result['relationshipCount']:,}")
                else:
                    print("  ⚠ Error: Failed to create structure projection")
                    return
                
                # Second projection for attack labels
                labels_name = f"{projection_name}_labels"
                
                # Check if projection exists before trying to drop
                try:
                    exists_result = session.run(
                        "CALL gds.graph.exists($name) YIELD exists RETURN exists",
                        name=labels_name
                    ).single()
                    
                    if exists_result and exists_result['exists']:
                        session.run("CALL gds.graph.drop($name)", name=labels_name)
                        print(f"  ✓ Dropped existing labels projection")
                except Exception as e:
                    # Ignore errors - projection doesn't exist or already dropped
                    pass
                
                labels_query = """
                CALL gds.graph.project(
                    $name,
                    'IP',
                    {
                        CONNECTS: {
                            properties: ['is_attack'],
                            orientation: 'UNDIRECTED'
                        }
                    }
                )
                YIELD graphName, nodeCount, relationshipCount
                RETURN graphName, nodeCount, relationshipCount
                """
                result = session.run(labels_query, name=labels_name).single()
                if result:
                    print(f"  ✓ Created labels projection: {result['graphName']}")
                    print(f"    Nodes: {result['nodeCount']:,}")
                    print(f"    Relationships: {result['relationshipCount']:,}")
                else:
                    print("  ⚠ Error: Failed to create labels projection")
                    return
            else:
                # Check if projection exists before trying to drop
                try:
                    exists_result = session.run(
                        "CALL gds.graph.exists($name) YIELD exists RETURN exists",
                        name=projection_name
                    ).single()
                    
                    if exists_result and exists_result['exists']:
                        session.run("CALL gds.graph.drop($name)", name=projection_name)
                        print(f"  ✓ Dropped existing projection")
                except Exception as e:
                    # Ignore errors - projection doesn't exist or already dropped
                    pass
                
                # Single projection for structure only with UNDIRECTED orientation
                # (required for clustering coefficient)
                create_query = """
                CALL gds.graph.project(
                    $name,
                    'IP',
                    {
                        CONNECTS: {
                            orientation: 'UNDIRECTED'
                        }
                    },
                    {
                        nodeProperties: ['subnet_id']
                    }
                )
                YIELD graphName, nodeCount, relationshipCount
                RETURN graphName, nodeCount, relationshipCount
                """
                result = session.run(create_query, name=projection_name).single()
                if result:
                    print(f"  ✓ Created projection: {result['graphName']}")
                    print(f"    Nodes: {result['nodeCount']:,}")
                    print(f"    Relationships: {result['relationshipCount']:,}")
                else:
                    print("  ⚠ Error: Failed to create projection")
                    return
    
    def compute_fastrp_embeddings(self, projection_name: str, embedding_dim: int, 
                                  use_labels: bool):
        """Compute FastRP embeddings using Neo4j GDS."""
        print(f"\n--- Computing FastRP Embeddings (dim={embedding_dim}) ---")
        
        with self.driver.session(database=self.database) as session:
            if use_labels:
                # First compute structural embeddings
                query = f"""
                CALL gds.fastRP.write(
                    '{projection_name}_structure',
                    {{
                        embeddingDimension: {embedding_dim},
                        iterationWeights: [0.0, 1.0, 1.0, 1.0],
                        normalizationStrength: 0.5,
                        writeProperty: 'embedding_structure'
                    }}
                )
                YIELD nodePropertiesWritten, computeMillis
                RETURN nodePropertiesWritten, computeMillis
                """
                result = session.run(query).single()
                if result:
                    print(f"  ✓ Structural embeddings computed in {result['computeMillis']/1000.0:.2f}s")
                else:
                    print("  ⚠ Error: Failed to compute structural embeddings")
                    return
                
                # Then compute label-aware embeddings
                query = f"""
                CALL gds.fastRP.write(
                    '{projection_name}_labels',
                    {{
                        embeddingDimension: {embedding_dim},
                        relationshipWeightProperty: 'is_attack',
                        iterationWeights: [0.0, 1.0, 1.0, 1.0],
                        normalizationStrength: 0.5,
                        writeProperty: 'embedding_labels'
                    }}
                )
                YIELD nodePropertiesWritten, computeMillis
                RETURN nodePropertiesWritten, computeMillis
                """
                result = session.run(query).single()
                if result:
                    print(f"  ✓ Label embeddings computed in {result['computeMillis']/1000.0:.2f}s")
                else:
                    print("  ⚠ Error: Failed to compute label embeddings")
                    return
                
                # Combine embeddings
                query = """
                MATCH (n:IP)
                WHERE n.embedding_structure IS NOT NULL AND n.embedding_labels IS NOT NULL
                SET n.embedding_label_aware = n.embedding_structure + n.embedding_labels
                WITH count(n) as nodes_updated
                RETURN nodes_updated
                """
                result = session.run(query).single()
                print(f"  ✓ Combined embeddings for {result['nodes_updated']:,} nodes")
                
                write_property = 'embedding_label_aware'
            else:
                # Pure structural embeddings
                query = f"""
                CALL gds.fastRP.write(
                    '{projection_name}',
                    {{
                        embeddingDimension: {embedding_dim},
                        iterationWeights: [0.0, 1.0, 1.0, 1.0],
                        normalizationStrength: 0.5,
                        writeProperty: 'embedding_label_agnostic'
                    }}
                )
                YIELD nodePropertiesWritten, computeMillis
                RETURN nodePropertiesWritten, computeMillis
                """
                result = session.run(query).single()
                if result:
                    print(f"  ✓ Embeddings computed in {result['computeMillis']/1000.0:.2f}s")
                    print(f"    Nodes with embeddings: {result['nodePropertiesWritten']:,}")
                    print(f"    Property name: embedding_label_agnostic")
                else:
                    print("  ⚠ Error: Failed to compute embeddings")
                    return
    
    def compute_centrality_metrics(self, projection_name: str):
        """Compute PageRank, Betweenness, and Clustering Coefficient."""
        print(f"\n--- Computing Centrality Metrics ---")
        
        with self.driver.session(database=self.database) as session:
            # PageRank
            print("  Computing PageRank...")
            pr_query = f"""
            CALL gds.pageRank.write(
                '{projection_name}',
                {{
                    writeProperty: 'pagerank',
                    maxIterations: 20,
                    dampingFactor: 0.85
                }}
            )
            YIELD nodePropertiesWritten, ranIterations
            RETURN nodePropertiesWritten
            """
            result = session.run(pr_query).single()
            print(f"    ✓ PageRank: {result['nodePropertiesWritten']:,} nodes")
            
            # Betweenness Centrality (sampled for performance)
            print("  Computing Betweenness Centrality (sampled)...")
            bc_query = f"""
            CALL gds.betweenness.write(
                '{projection_name}',
                {{
                    writeProperty: 'betweenness',
                    samplingSize: 1000,
                    samplingSeed: 42
                }}
            )
            YIELD nodePropertiesWritten, computeMillis
            RETURN nodePropertiesWritten, computeMillis
            """
            result = session.run(bc_query).single()
            print(f"    ✓ Betweenness: {result['nodePropertiesWritten']:,} nodes in {result['computeMillis']/1000:.2f}s")
            
            # Local Clustering Coefficient
            print("  Computing Clustering Coefficient...")
            cc_query = f"""
            CALL gds.localClusteringCoefficient.write(
                '{projection_name}',
                {{
                    writeProperty: 'clustering_coef'
                }}
            )
            YIELD nodePropertiesWritten, averageClusteringCoefficient
            RETURN nodePropertiesWritten, averageClusteringCoefficient
            """
            result = session.run(cc_query).single()
            print(f"    ✓ Clustering Coefficient: {result['nodePropertiesWritten']:,} nodes")
            print(f"      Average: {result['averageClusteringCoefficient']:.4f}")
    
    def compute_temporal_features(self):
        """Compute temporal features: connection velocity and burst patterns."""
        print(f"\n--- Computing Temporal Features ---")
        
        with self.driver.session(database=self.database) as session:
            query = """
            MATCH (n:IP)
            OPTIONAL MATCH (n)-[r:CONNECTS]->()
            WITH n, r ORDER BY r.timestamp
            WITH n, collect(r.timestamp) as timestamps
            WHERE size(timestamps) > 1
            
            WITH n, timestamps,
                 timestamps[-1] - timestamps[0] as time_span,
                 size(timestamps) as total_connections
            
            WITH n,
                 CASE WHEN time_span > 0 
                      THEN toFloat(total_connections) / (time_span / 3600.0)
                      ELSE 0.0 END as conn_velocity,
                 total_connections,
                 timestamps
            
            // Compute burst score (variance in inter-arrival times)
            WITH n, conn_velocity, total_connections, timestamps,
                 [i in range(0, size(timestamps)-2) | timestamps[i+1] - timestamps[i]] as intervals
            
            WITH n, conn_velocity, total_connections,
                 CASE WHEN size(intervals) > 1
                      THEN reduce(sum = 0.0, x IN intervals | sum + x) / size(intervals)
                      ELSE 0.0 END as mean_interval,
                 intervals
            
            WITH n, conn_velocity, total_connections, mean_interval, intervals,
                 CASE WHEN size(intervals) > 1 AND mean_interval > 0
                      THEN sqrt(reduce(sum = 0.0, x IN intervals | sum + (x - mean_interval)^2) / size(intervals)) / mean_interval
                      ELSE 0.0 END as burst_score
            
            SET n.conn_velocity = conn_velocity,
                n.burst_score = burst_score,
                n.total_connections = total_connections
            
            RETURN count(n) as nodes_updated,
                   avg(conn_velocity) as avg_velocity,
                   avg(burst_score) as avg_burst
            """
            
            result = session.run(query).single()
            print(f"  ✓ Temporal features computed")
            print(f"    Nodes updated: {result['nodes_updated']:,}")
            print(f"    Avg connection velocity: {result['avg_velocity']:.2f} conn/hour")
            print(f"    Avg burst score: {result['avg_burst']:.4f}")
    
    def identify_reconnaissance_victims_by_subnet(self, use_labels: bool, 
                                                   historical_window_hours: int):
        """Identify subnets that were victims of reconnaissance."""
        print(f"\n--- Identifying Reconnaissance Victims by Subnet ---")
        
        with self.driver.session(database=self.database) as session:
            if use_labels:
                # Use MITRE ATT&CK labels
                query = """
                MATCH (a:IP)-[r:CONNECTS]->(v:IP)
                WHERE r.is_attack = 1 AND r.tactic = 'Reconnaissance'
                WITH DISTINCT v.subnet as victim_subnet, r.timestamp as recon_time
                ORDER BY recon_time
                RETURN victim_subnet, recon_time
                """
            else:
                # Label-agnostic: any incoming connection followed by outgoing
                query = """
                MATCH (a:IP)-[r1:CONNECTS]->(v:IP)
                WHERE exists { (v)-[:CONNECTS]->() }
                WITH DISTINCT v.subnet as victim_subnet, r1.timestamp as recon_time
                ORDER BY recon_time
                RETURN victim_subnet, recon_time
                LIMIT 10000  // Reasonable sample
                """
            
            result = session.run(query).data()
            print(f"  ✓ Found {len(result):,} reconnaissance events on subnets")
            
            return result
    
    def check_subnet_pivot_behavior(self, subnet: str, recon_time: int, 
                                   detection_window_hours: int, use_labels: bool):
        """Check if ANY IP in the subnet launched attacks after reconnaissance."""
        
        with self.driver.session(database=self.database) as session:
            window_sec = detection_window_hours * 3600
            
            if use_labels:
                query = """
                MATCH (pivot:IP)-[r:CONNECTS]->(target:IP)
                WHERE pivot.subnet = $subnet
                  AND r.timestamp > $recon_time
                  AND r.timestamp <= $recon_time + $window_sec
                  AND r.is_attack = 1
                RETURN count(r) > 0 as became_pivot,
                       count(r) as attack_count,
                       collect(DISTINCT pivot.address)[0..5] as pivot_ips
                """
            else:
                # Label-agnostic: just check for outgoing connections
                query = """
                MATCH (pivot:IP)-[r:CONNECTS]->(target:IP)
                WHERE pivot.subnet = $subnet
                  AND r.timestamp > $recon_time
                  AND r.timestamp <= $recon_time + $window_sec
                RETURN count(r) > 0 as became_pivot,
                       count(r) as attack_count,
                       collect(DISTINCT pivot.address)[0..5] as pivot_ips
                """
            
            result = session.run(query, subnet=subnet, recon_time=recon_time, 
                               window_sec=window_sec).single()
            
            return {
                'became_pivot': result['became_pivot'],
                'attack_count': result['attack_count'],
                'pivot_ips': result['pivot_ips']
            }
    
    def get_subnet_features(self, subnet: str, recon_time: int, 
                           historical_window_hours: int, use_labels: bool):
        """Extract all features for a subnet (embedding + centrality + temporal)."""
        
        with self.driver.session(database=self.database) as session:
            window_sec = historical_window_hours * 3600
            embedding_prop = 'embedding_label_aware' if use_labels else 'embedding_label_agnostic'
            
            query = f"""
            MATCH (n:IP)
            WHERE n.subnet = $subnet
            
            // Get embeddings and centrality for IPs in subnet
            WITH n,
                 n.{embedding_prop} as embedding,
                 coalesce(n.pagerank, 0.0) as pagerank,
                 coalesce(n.betweenness, 0.0) as betweenness,
                 coalesce(n.clustering_coef, 0.0) as clustering,
                 coalesce(n.conn_velocity, 0.0) as velocity,
                 coalesce(n.burst_score, 0.0) as burst
            
            // Aggregate subnet-level features
            RETURN 
                collect(embedding) as embeddings,
                avg(pagerank) as avg_pagerank,
                max(pagerank) as max_pagerank,
                avg(betweenness) as avg_betweenness,
                max(betweenness) as max_betweenness,
                avg(clustering) as avg_clustering,
                avg(velocity) as avg_velocity,
                max(velocity) as max_velocity,
                avg(burst) as avg_burst,
                count(n) as subnet_size
            """
            
            result = session.run(query, subnet=subnet, 
                               recon_time=recon_time, 
                               window_sec=window_sec).single()
            
            if not result or not result['embeddings']:
                return None
            
            # Average embeddings across subnet IPs
            embeddings = [emb for emb in result['embeddings'] if emb is not None]
            if not embeddings:
                return None
            
            avg_embedding = np.mean(embeddings, axis=0)
            
            return {
                'embedding': avg_embedding,
                'subnet_size': result['subnet_size'],
                'avg_pagerank': float(result['avg_pagerank'] or 0),
                'max_pagerank': float(result['max_pagerank'] or 0),
                'avg_betweenness': float(result['avg_betweenness'] or 0),
                'max_betweenness': float(result['max_betweenness'] or 0),
                'avg_clustering': float(result['avg_clustering'] or 0),
                'avg_velocity': float(result['avg_velocity'] or 0),
                'max_velocity': float(result['max_velocity'] or 0),
                'avg_burst': float(result['avg_burst'] or 0)
            }
    
    def run_pivot_prediction(self, use_labels: bool, historical_window_hours: int,
                            detection_window_hours: int, embedding_dim: int,
                            output_prefix: str):
        """Run the complete pivot prediction pipeline."""
        
        # Step 1: Create graph projection
        projection_name = f"pivot_graph_{'labeled' if use_labels else 'unlabeled'}"
        self.create_graph_projection(projection_name, use_labels)
        
        # Step 2: Compute FastRP embeddings
        self.compute_fastrp_embeddings(projection_name, embedding_dim, use_labels)
        
        # Step 3: Compute centrality metrics
        # For label-aware mode, use the structure projection
        centrality_projection = f"{projection_name}_structure" if use_labels else projection_name
        self.compute_centrality_metrics(centrality_projection)
        
        # Step 4: Compute temporal features
        self.compute_temporal_features()
        
        # Step 5: Identify reconnaissance victims by subnet
        recon_events = self.identify_reconnaissance_victims_by_subnet(
            use_labels, historical_window_hours
        )
        
        if not recon_events:
            print("  ⚠ No reconnaissance events found")
            return
        
        # Step 6: Train/test split (50/50 temporal)
        split_idx = len(recon_events) // 2
        train_events = recon_events[:split_idx]
        test_events = recon_events[split_idx:]
        
        print(f"\n--- Train/Test Split ---")
        print(f"  Training: {len(train_events):,} events")
        print(f"  Testing: {len(test_events):,} events")
        
        # Step 7: Process training set
        print(f"\n--- Processing Training Set ---")
        train_results = self.process_event_set(
            train_events, use_labels, historical_window_hours, 
            detection_window_hours, "Training"
        )
        
        if not train_results:
            print("  ⚠ No valid training data")
            return
        
        train_df = pd.DataFrame(train_results)
        train_pivot_count = train_df['became_pivot'].sum()
        
        print(f"\n  Training Results:")
        print(f"    Valid samples: {len(train_df):,}")
        print(f"    Pivots: {train_pivot_count:,} ({train_pivot_count/len(train_df)*100:.1f}%)")
        
        if train_pivot_count == 0:
            print("  ⚠ No pivots in training set")
            return
        
        # Create reference pivot embedding
        train_pivot_embeddings = np.array([
            r['embedding'] for r in train_results if r['became_pivot']
        ])
        reference_pivot_embedding = np.mean(train_pivot_embeddings, axis=0)
        
        # Step 8: Process test set
        print(f"\n--- Processing Test Set ---")
        test_results = self.process_event_set(
            test_events, use_labels, historical_window_hours,
            detection_window_hours, "Testing"
        )
        
        if not test_results:
            print("  ⚠ No valid test data")
            return
        
        test_df = pd.DataFrame(test_results)
        test_pivot_count = test_df['became_pivot'].sum()
        
        print(f"\n  Test Results:")
        print(f"    Valid samples: {len(test_df):,}")
        print(f"    Pivots: {test_pivot_count:,} ({test_pivot_count/len(test_df)*100:.1f}%)")
        
        # Step 9: Compute similarities and evaluate
        test_df['fastrp_similarity'] = test_df['embedding'].apply(
            lambda emb: cosine_similarity(
                np.array(emb).reshape(1, -1),
                reference_pivot_embedding.reshape(1, -1)
            )[0][0]
        )
        
        # Step 10: Statistical analysis
        self.perform_statistical_analysis(test_df, output_prefix)
        
        # Step 11: Baseline comparison
        self.compare_with_baselines(test_df, output_prefix)
        
        # Step 12: Multi-hop chain analysis
        self.analyze_multi_hop_chains(use_labels, output_prefix)
        
        # Step 13: Save results and visualize
        test_df.to_csv(f'{output_prefix}_pivot_predictions.csv', index=False)
        self.visualize_results(test_df, output_prefix)
        
        # Cleanup projection
        with self.driver.session(database=self.database) as session:
            session.run(f"CALL gds.graph.drop('{projection_name}')")
    
    def process_event_set(self, events: List[Dict], use_labels: bool,
                         historical_window_hours: int, detection_window_hours: int,
                         set_name: str):
        """Process a set of reconnaissance events (SUPER OPTIMIZED: pre-fetch all data)."""
        
        print(f"  Pre-fetching all subnet features and pivot data...")
        
        # Step 1: Get ALL subnet features in ONE query
        subnet_features = self.get_all_subnet_features(use_labels)
        
        # Step 2: Get ALL pivot behaviors in ONE query
        pivot_behaviors = self.get_all_pivot_behaviors(
            events, use_labels, detection_window_hours
        )
        
        # Step 3: Process in Python (fast!)
        results = []
        
        print(f"  Processing {len(events)} events in memory...")
        with tqdm(total=len(events), desc=f"  Processing {set_name} events", 
                 unit="subnet") as pbar:
            
            for event in events:
                subnet = event['victim_subnet']
                recon_time = event['recon_time']
                
                # Get features from pre-fetched data
                features = subnet_features.get(subnet)
                if not features or not features['embeddings']:
                    pbar.update(1)
                    continue
                
                # Get pivot info from pre-fetched data
                pivot_key = (subnet, recon_time)
                pivot_info = pivot_behaviors.get(pivot_key, {
                    'became_pivot': False,
                    'attack_count': 0,
                    'pivot_ips': []
                })
                
                # Average embeddings
                avg_embedding = np.mean(features['embeddings'], axis=0)
                
                results.append({
                    'subnet': subnet,
                    'recon_time': recon_time,
                    'embedding': avg_embedding,
                    'became_pivot': pivot_info['became_pivot'],
                    'attack_count': pivot_info['attack_count'],
                    'pivot_ips': pivot_info['pivot_ips'],
                    'subnet_size': features['subnet_size'],
                    'avg_pagerank': features['avg_pagerank'],
                    'max_pagerank': features['max_pagerank'],
                    'avg_betweenness': features['avg_betweenness'],
                    'max_betweenness': features['max_betweenness'],
                    'avg_clustering': features['avg_clustering'],
                    'avg_velocity': features['avg_velocity'],
                    'max_velocity': features['max_velocity'],
                    'avg_burst': features['avg_burst']
                })
                
                pbar.update(1)
        
        return results
    
    def get_all_subnet_features(self, use_labels: bool):
        """Pre-fetch ALL subnet features in a single query."""
        
        with self.driver.session(database=self.database) as session:
            embedding_prop = 'embedding_label_aware' if use_labels else 'embedding_label_agnostic'
            
            query = f"""
            MATCH (n:IP)
            WHERE n.subnet IS NOT NULL
            
            WITH n.subnet as subnet,
                 collect(n.{embedding_prop}) as embeddings,
                 avg(coalesce(n.pagerank, 0.0)) as avg_pagerank,
                 max(coalesce(n.pagerank, 0.0)) as max_pagerank,
                 avg(coalesce(n.betweenness, 0.0)) as avg_betweenness,
                 max(coalesce(n.betweenness, 0.0)) as max_betweenness,
                 avg(coalesce(n.clustering_coef, 0.0)) as avg_clustering,
                 avg(coalesce(n.conn_velocity, 0.0)) as avg_velocity,
                 max(coalesce(n.conn_velocity, 0.0)) as max_velocity,
                 avg(coalesce(n.burst_score, 0.0)) as avg_burst,
                 count(n) as subnet_size
            
            RETURN subnet, embeddings, subnet_size,
                   avg_pagerank, max_pagerank, avg_betweenness, max_betweenness,
                   avg_clustering, avg_velocity, max_velocity, avg_burst
            """
            
            result = session.run(query).data()
            
            # Build lookup dictionary
            features_map = {}
            for record in result:
                # Filter out None embeddings
                embeddings = [emb for emb in record['embeddings'] if emb is not None]
                
                features_map[record['subnet']] = {
                    'embeddings': embeddings,
                    'subnet_size': record['subnet_size'],
                    'avg_pagerank': float(record['avg_pagerank'] or 0),
                    'max_pagerank': float(record['max_pagerank'] or 0),
                    'avg_betweenness': float(record['avg_betweenness'] or 0),
                    'max_betweenness': float(record['max_betweenness'] or 0),
                    'avg_clustering': float(record['avg_clustering'] or 0),
                    'avg_velocity': float(record['avg_velocity'] or 0),
                    'max_velocity': float(record['max_velocity'] or 0),
                    'avg_burst': float(record['avg_burst'] or 0)
                }
            
            print(f"    ✓ Loaded features for {len(features_map)} subnets")
            return features_map
    
    def get_all_pivot_behaviors(self, events: List[Dict], use_labels: bool, 
                               detection_window_hours: int):
        """Pre-fetch ALL pivot behaviors - FIXED to detect true lateral movement."""
        
        with self.driver.session(database=self.database) as session:
            det_window_sec = detection_window_hours * 3600
            
            # Get unique subnets from events and time range
            subnets_in_events = set(e['victim_subnet'] for e in events)
            min_time = min(e['recon_time'] for e in events)
            max_time = max(e['recon_time'] for e in events) + det_window_sec
            
            print(f"    Fetching LATERAL MOVEMENT attacks for {len(subnets_in_events)} unique subnets...")
            print(f"    Time range: {min_time} to {max_time} ({(max_time - min_time) / 3600:.1f} hours)")
            
            # Get ONLY lateral movement attacks (cross-subnet, post-recon)
            # OPTIMIZATION: Filter by time range in Cypher to reduce data transfer
            if use_labels:
                # Label-aware: Use MITRE ATT&CK tactics that indicate lateral movement
                # Based on exploration: dataset uses Credential Access, Defense Evasion, Exfiltration
                query = """
                MATCH (pivot:IP)-[r:CONNECTS]->(target:IP)
                WHERE pivot.subnet IN $subnets
                  AND r.is_attack = 1
                  AND r.tactic IN ['Lateral Movement', 'Execution', 'Command and Control', 
                                   'Credential Access', 'Defense Evasion', 'Exfiltration', 
                                   'Collection', 'Discovery']
                  AND target.subnet <> pivot.subnet  // Must be cross-subnet
                  AND r.timestamp >= $min_time      // Filter by time range
                  AND r.timestamp <= $max_time
                RETURN pivot.subnet as pivot_subnet,
                       target.subnet as target_subnet,
                       r.timestamp as timestamp,
                       pivot.address as pivot_ip,
                       r.tactic as tactic,
                       r.technique as technique
                ORDER BY pivot.subnet, r.timestamp
                """
            else:
                # Label-agnostic: Cross-subnet connections with burst behavior
                # (multiple connections to new subnets in short time)
                query = """
                MATCH (pivot:IP)-[r:CONNECTS]->(target:IP)
                WHERE pivot.subnet IN $subnets
                  AND target.subnet <> pivot.subnet  // Must be cross-subnet
                  AND r.timestamp >= $min_time      // Filter by time range
                  AND r.timestamp <= $max_time
                WITH pivot, target, r
                ORDER BY r.timestamp
                WITH pivot.subnet as pivot_subnet,
                     target.subnet as target_subnet,
                     collect(r.timestamp) as timestamps,
                     collect(pivot.address) as pivot_ips,
                     count(r) as connection_count
                WHERE connection_count >= 3  // At least 3 connections = suspicious
                  AND (timestamps[-1] - timestamps[0]) <= 3600  // Within 1 hour = burst
                UNWIND range(0, size(timestamps)-1) as idx
                RETURN pivot_subnet,
                       target_subnet,
                       timestamps[idx] as timestamp,
                       pivot_ips[idx] as pivot_ip,
                       null as tactic,
                       null as technique
                ORDER BY pivot_subnet, timestamp
                """
            
            result = session.run(query, 
                                subnets=list(subnets_in_events),
                                min_time=min_time,
                                max_time=max_time).data()
            
            print(f"    ✓ Fetched {len(result)} lateral movement attacks")
            
            if len(result) == 0:
                print("    ⚠ WARNING: No lateral movement attacks found!")
                print("      All events will be classified as non-pivots.")
            
            print(f"    Processing pivot behaviors in memory...")
            
            # Build subnet -> [(timestamp, ip, target_subnet, tactic)] mapping
            subnet_lateral_moves = defaultdict(list)
            for record in result:
                subnet_lateral_moves[record['pivot_subnet']].append({
                    'timestamp': record['timestamp'],
                    'pivot_ip': record['pivot_ip'],
                    'target_subnet': record['target_subnet'],
                    'tactic': record.get('tactic'),
                    'technique': record.get('technique')
                })
            
            # Now check each event against the lateral movements
            pivot_map = {}
            for event in events:
                subnet = event['victim_subnet']
                recon_time = event['recon_time']
                
                # Filter lateral movements that happened after recon_time
                lateral_moves = [
                    move for move in subnet_lateral_moves.get(subnet, [])
                    if recon_time < move['timestamp'] <= recon_time + det_window_sec
                ]
                
                # Get unique pivot IPs and target subnets
                pivot_ips = list(set(move['pivot_ip'] for move in lateral_moves))[:5]
                target_subnets = list(set(move['target_subnet'] for move in lateral_moves))[:5]
                tactics = list(set(move['tactic'] for move in lateral_moves if move['tactic']))[:5]
                
                key = (subnet, recon_time)
                pivot_map[key] = {
                    'became_pivot': len(lateral_moves) > 0,
                    'attack_count': len(lateral_moves),
                    'pivot_ips': pivot_ips,
                    'target_subnets': target_subnets,
                    'tactics': tactics
                }
            
            # Count how many are actually pivots
            pivot_count = sum(1 for v in pivot_map.values() if v['became_pivot'])
            pivot_pct = (pivot_count / len(pivot_map) * 100) if pivot_map else 0
            
            print(f"    ✓ Loaded pivot behaviors for {len(pivot_map)} events")
            print(f"    ✓ {pivot_count} ({pivot_pct:.1f}%) are TRUE PIVOTS (lateral movement detected)")
            
            return pivot_map
    
    def perform_statistical_analysis(self, test_df: pd.DataFrame, output_prefix: str):
        """Perform statistical significance testing."""
        print(f"\n--- Statistical Analysis ---")
        
        pivot_sims = test_df[test_df['became_pivot']]['fastrp_similarity']
        non_pivot_sims = test_df[~test_df['became_pivot']]['fastrp_similarity']
        
        print(f"\n  FastRP Similarity Statistics:")
        print(f"    Pivots:     mean={pivot_sims.mean():.4f}, std={pivot_sims.std():.4f}")
        print(f"    Non-pivots: mean={non_pivot_sims.mean():.4f}, std={non_pivot_sims.std():.4f}")
        print(f"    Difference: {pivot_sims.mean() - non_pivot_sims.mean():.4f}")
        
        # Welch's t-test
        t_stat, p_value = stats.ttest_ind(pivot_sims, non_pivot_sims, equal_var=False)
        print(f"\n  Welch's t-test: t={t_stat:.4f}, p={p_value:.6f}")
        
        if p_value < 0.05:
            print(f"  ✓ STATISTICALLY SIGNIFICANT (p < 0.05)")
        else:
            print(f"  ⚠ Not statistically significant")
        
        # Effect size (Cohen's d)
        pooled_std = np.sqrt((pivot_sims.std()**2 + non_pivot_sims.std()**2) / 2)
        cohens_d = (pivot_sims.mean() - non_pivot_sims.mean()) / pooled_std if pooled_std > 0 else 0
        
        print(f"  Cohen's d: {cohens_d:.4f}", end="")
        if abs(cohens_d) < 0.2:
            print(" (small effect)")
        elif abs(cohens_d) < 0.5:
            print(" (medium effect)")
        else:
            print(" (large effect)")
        
        # Mann-Whitney U test (non-parametric alternative)
        u_stat, u_p_value = stats.mannwhitneyu(pivot_sims, non_pivot_sims, alternative='greater')
        print(f"  Mann-Whitney U: U={u_stat:.0f}, p={u_p_value:.6f}")
    
    def compare_with_baselines(self, test_df: pd.DataFrame, output_prefix: str):
        """Compare FastRP with baseline methods."""
        print(f"\n--- Baseline Comparison ---")
        
        # Normalize features
        for col in ['avg_pagerank', 'max_pagerank', 'avg_betweenness', 
                   'max_betweenness', 'avg_clustering', 'avg_velocity', 
                   'max_velocity', 'avg_burst', 'subnet_size']:
            if test_df[col].std() > 0:
                test_df[f'{col}_norm'] = (test_df[col] - test_df[col].mean()) / test_df[col].std()
            else:
                test_df[f'{col}_norm'] = 0
        
        methods = {
            'FastRP Embedding': 'fastrp_similarity',
            'Avg PageRank': 'avg_pagerank_norm',
            'Max PageRank': 'max_pagerank_norm',
            'Avg Betweenness': 'avg_betweenness_norm',
            'Max Betweenness': 'max_betweenness_norm',
            'Avg Clustering': 'avg_clustering_norm',
            'Connection Velocity': 'avg_velocity_norm',
            'Burst Score': 'avg_burst_norm',
            'Subnet Size': 'subnet_size_norm'
        }
        
        comparison_results = []
        
        for method_name, score_column in methods.items():
            scores = test_df[score_column].fillna(0)
            y_true = test_df['became_pivot'].astype(int)
            
            # ROC curve
            fpr, tpr, thresholds = roc_curve(y_true, scores)
            roc_auc = auc(fpr, tpr)
            
            # Precision-Recall curve
            precision, recall, _ = precision_recall_curve(y_true, scores)
            pr_auc = auc(recall, precision)
            
            # Find optimal threshold (maximize F1)
            best_f1 = 0
            best_metrics = {}
            
            for threshold in thresholds:
                preds = (scores >= threshold).astype(int)
                tp = ((preds == 1) & (y_true == 1)).sum()
                fp = ((preds == 1) & (y_true == 0)).sum()
                fn = ((preds == 0) & (y_true == 1)).sum()
                tn = ((preds == 0) & (y_true == 0)).sum()
                
                accuracy = (tp + tn) / len(test_df) if len(test_df) > 0 else 0
                prec = tp / (tp + fp) if (tp + fp) > 0 else 0
                rec = tp / (tp + fn) if (tp + fn) > 0 else 0
                f1 = 2 * prec * rec / (prec + rec) if (prec + rec) > 0 else 0
                
                if f1 > best_f1:
                    best_f1 = f1
                    best_metrics = {
                        'accuracy': accuracy,
                        'precision': prec,
                        'recall': rec,
                        'f1': f1
                    }
            
            comparison_results.append({
                'Method': method_name,
                'AUC-ROC': roc_auc,
                'AUC-PR': pr_auc,
                'Accuracy': best_metrics['accuracy'],
                'Precision': best_metrics['precision'],
                'Recall': best_metrics['recall'],
                'F1-Score': best_metrics['f1']
            })
        
        comparison_df = pd.DataFrame(comparison_results)
        print("\n" + "="*100)
        print("METHOD COMPARISON")
        print("="*100)
        print(comparison_df.to_string(index=False, float_format=lambda x: f'{x:.4f}'))
        print("="*100)
        
        # Save comparison
        comparison_df.to_csv(f'{output_prefix}_method_comparison.csv', index=False)
        
        # Determine best method
        best_method = comparison_df.loc[comparison_df['F1-Score'].idxmax()]
        fastrp_f1 = comparison_df[comparison_df['Method'] == 'FastRP Embedding']['F1-Score'].values[0]
        
        print(f"\n  Best Method: {best_method['Method']} (F1={best_method['F1-Score']:.4f})")
        
        if best_method['Method'] == 'FastRP Embedding':
            print(f"  ✓ FastRP outperforms all baselines")
        else:
            diff = (fastrp_f1 - best_method['F1-Score']) * 100
            print(f"  ⚠ FastRP is {abs(diff):.2f}pp {'behind' if diff < 0 else 'ahead of'} best baseline")
    
    def analyze_multi_hop_chains(self, use_labels: bool, output_prefix: str):
        """Analyze multi-hop attack chains (A→B→C→D)."""
        print(f"\n--- Multi-Hop Attack Chain Analysis ---")
        
        with self.driver.session(database=self.database) as session:
            if use_labels:
                query = """
                MATCH path = (a:IP)-[r1:CONNECTS]->(b:IP)-[r2:CONNECTS]->(c:IP)-[r3:CONNECTS]->(d:IP)
                WHERE r1.is_attack = 1 AND r2.is_attack = 1 AND r3.is_attack = 1
                  AND r2.timestamp > r1.timestamp
                  AND r3.timestamp > r2.timestamp
                  AND a <> c AND b <> d AND a <> d
                WITH 
                    a.subnet as hop1_subnet,
                    b.subnet as hop2_subnet,
                    c.subnet as hop3_subnet,
                    d.subnet as hop4_subnet,
                    r1.timestamp as t1,
                    r2.timestamp as t2,
                    r3.timestamp as t3,
                    r1.tactic as tactic1,
                    r2.tactic as tactic2,
                    r3.tactic as tactic3
                LIMIT 100
                RETURN 
                    hop1_subnet, hop2_subnet, hop3_subnet, hop4_subnet,
                    (t2 - t1) / 3600.0 as hours_to_hop2,
                    (t3 - t2) / 3600.0 as hours_to_hop3,
                    tactic1, tactic2, tactic3
                """
            else:
                query = """
                MATCH path = (a:IP)-[r1:CONNECTS]->(b:IP)-[r2:CONNECTS]->(c:IP)-[r3:CONNECTS]->(d:IP)
                WHERE r2.timestamp > r1.timestamp
                  AND r3.timestamp > r2.timestamp
                  AND a <> c AND b <> d AND a <> d
                WITH 
                    a.subnet as hop1_subnet,
                    b.subnet as hop2_subnet,
                    c.subnet as hop3_subnet,
                    d.subnet as hop4_subnet,
                    r1.timestamp as t1,
                    r2.timestamp as t2,
                    r3.timestamp as t3
                LIMIT 100
                RETURN 
                    hop1_subnet, hop2_subnet, hop3_subnet, hop4_subnet,
                    (t2 - t1) / 3600.0 as hours_to_hop2,
                    (t3 - t2) / 3600.0 as hours_to_hop3
                """
            
            result = session.run(query).data()
            
            if not result:
                print("  ⚠ No multi-hop chains found")
                return
            
            print(f"  ✓ Found {len(result):,} multi-hop attack chains")
            
            df = pd.DataFrame(result)
            
            print(f"\n  Chain Timing Statistics:")
            print(f"    Mean time to 2nd hop: {df['hours_to_hop2'].mean():.2f} hours")
            print(f"    Mean time to 3rd hop: {df['hours_to_hop3'].mean():.2f} hours")
            print(f"    Median time to 2nd hop: {df['hours_to_hop2'].median():.2f} hours")
            print(f"    Median time to 3rd hop: {df['hours_to_hop3'].median():.2f} hours")
            
            if use_labels and 'tactic1' in df.columns:
                print(f"\n  Most Common Tactic Sequences:")
                tactic_sequences = df.groupby(['tactic1', 'tactic2', 'tactic3']).size().sort_values(ascending=False).head(5)
                for (t1, t2, t3), count in tactic_sequences.items():
                    print(f"    {t1} → {t2} → {t3}: {count} chains")
            
            # Save chains
            df.to_csv(f'{output_prefix}_multi_hop_chains.csv', index=False)
    
    def compare_analysis_modes(self):
        """Compare label-aware vs label-agnostic analysis results."""
        print("\n" + "="*80)
        print("COMPARING LABEL-AWARE VS LABEL-AGNOSTIC ANALYSIS")
        print("="*80)
        
        try:
            # Load results from both modes
            label_aware_df = pd.read_csv('label_aware_pivot_predictions.csv')
            label_agnostic_df = pd.read_csv('label_agnostic_pivot_predictions.csv')
            
            label_aware_comp = pd.read_csv('label_aware_method_comparison.csv')
            label_agnostic_comp = pd.read_csv('label_agnostic_method_comparison.csv')
            
            print("\n--- Performance Comparison ---")
            print("\nLabel-Aware FastRP:")
            la_fastrp = label_aware_comp[label_aware_comp['Method'] == 'FastRP Embedding'].iloc[0]
            print(f"  AUC-ROC: {la_fastrp['AUC-ROC']:.4f}")
            print(f"  AUC-PR:  {la_fastrp['AUC-PR']:.4f}")
            print(f"  F1-Score: {la_fastrp['F1-Score']:.4f}")
            print(f"  Precision: {la_fastrp['Precision']:.4f}")
            print(f"  Recall: {la_fastrp['Recall']:.4f}")
            
            print("\nLabel-Agnostic FastRP:")
            lag_fastrp = label_agnostic_comp[label_agnostic_comp['Method'] == 'FastRP Embedding'].iloc[0]
            print(f"  AUC-ROC: {lag_fastrp['AUC-ROC']:.4f}")
            print(f"  AUC-PR:  {lag_fastrp['AUC-PR']:.4f}")
            print(f"  F1-Score: {lag_fastrp['F1-Score']:.4f}")
            print(f"  Precision: {lag_fastrp['Precision']:.4f}")
            print(f"  Recall: {lag_fastrp['Recall']:.4f}")
            
            # Compute differences
            print("\n--- Performance Differences (Label-Aware minus Label-Agnostic) ---")
            print(f"  ΔAUC-ROC: {(la_fastrp['AUC-ROC'] - lag_fastrp['AUC-ROC']):.4f}")
            print(f"  ΔAUC-PR:  {(la_fastrp['AUC-PR'] - lag_fastrp['AUC-PR']):.4f}")
            print(f"  ΔF1-Score: {(la_fastrp['F1-Score'] - lag_fastrp['F1-Score']):.4f}")
            
            # Determine which is better
            if la_fastrp['F1-Score'] > lag_fastrp['F1-Score']:
                improvement = ((la_fastrp['F1-Score'] - lag_fastrp['F1-Score']) / lag_fastrp['F1-Score']) * 100
                print(f"\n✓ Label-aware analysis performs {improvement:.1f}% better")
                print("  Conclusion: MITRE ATT&CK labels enhance pivot prediction")
            elif lag_fastrp['F1-Score'] > la_fastrp['F1-Score']:
                improvement = ((lag_fastrp['F1-Score'] - la_fastrp['F1-Score']) / la_fastrp['F1-Score']) * 100
                print(f"\n✓ Label-agnostic analysis performs {improvement:.1f}% better")
                print("  Conclusion: Structural features alone are sufficient for pivot prediction")
            else:
                print("\n  Both approaches perform equally")
            
            # Create comparison visualization
            fig, axes = plt.subplots(1, 2, figsize=(14, 5))
            
            # Plot 1: F1-Score comparison across all methods
            ax = axes[0]
            methods = label_aware_comp['Method'].values
            la_f1 = label_aware_comp['F1-Score'].values
            lag_f1 = label_agnostic_comp['F1-Score'].values
            
            x = np.arange(len(methods))
            width = 0.35
            
            ax.bar(x - width/2, la_f1, width, label='Label-Aware', color='steelblue')
            ax.bar(x + width/2, lag_f1, width, label='Label-Agnostic', color='coral')
            
            ax.set_ylabel('F1-Score')
            ax.set_title('Method Comparison: Label-Aware vs Label-Agnostic')
            ax.set_xticks(x)
            ax.set_xticklabels(methods, rotation=45, ha='right')
            ax.legend()
            ax.grid(alpha=0.3, axis='y')
            
            # Plot 2: Similarity score distributions
            ax = axes[1]
            ax.hist(label_aware_df['fastrp_similarity'], bins=30, alpha=0.5,
                   label='Label-Aware', color='steelblue', edgecolor='black')
            ax.hist(label_agnostic_df['fastrp_similarity'], bins=30, alpha=0.5,
                   label='Label-Agnostic', color='coral', edgecolor='black')
            ax.set_xlabel('FastRP Similarity Score')
            ax.set_ylabel('Frequency')
            ax.set_title('Similarity Score Distribution Comparison')
            ax.legend()
            ax.grid(alpha=0.3)
            
            plt.tight_layout()
            plt.savefig('mode_comparison.png', dpi=150, bbox_inches='tight')
            print("\n✓ Saved comparison visualization to 'mode_comparison.png'")
            
        except FileNotFoundError as e:
            print(f"  ⚠ Could not load results files: {e}")
            print("  Run both analysis modes first before comparing")
    
    def visualize_results(self, test_df: pd.DataFrame, output_prefix: str):
        """Generate comprehensive visualizations."""
        print(f"\n--- Generating Visualizations ---")
        
        fig, axes = plt.subplots(3, 3, figsize=(20, 18))
        
        pivot_df = test_df[test_df['became_pivot']]
        non_pivot_df = test_df[~test_df['became_pivot']]
        
        # Plot 1: FastRP similarity distribution
        ax = axes[0, 0]
        ax.hist(non_pivot_df['fastrp_similarity'], bins=30, alpha=0.5, 
               label='Non-Pivots', color='blue', edgecolor='black')
        ax.hist(pivot_df['fastrp_similarity'], bins=30, alpha=0.5, 
               label='Pivots', color='red', edgecolor='black')
        ax.axvline(pivot_df['fastrp_similarity'].mean(), color='red', 
                  linestyle='--', label=f'Pivot Mean: {pivot_df["fastrp_similarity"].mean():.3f}')
        ax.axvline(non_pivot_df['fastrp_similarity'].mean(), color='blue', 
                  linestyle='--', label=f'Non-Pivot Mean: {non_pivot_df["fastrp_similarity"].mean():.3f}')
        ax.set_xlabel('FastRP Similarity to Reference Pivot')
        ax.set_ylabel('Frequency')
        ax.set_title('Distribution of FastRP Similarity Scores')
        ax.legend()
        ax.grid(alpha=0.3)
        
        # Plot 2: ROC Curve
        ax = axes[0, 1]
        y_true = test_df['became_pivot'].astype(int)
        fpr, tpr, _ = roc_curve(y_true, test_df['fastrp_similarity'])
        roc_auc = auc(fpr, tpr)
        ax.plot(fpr, tpr, lw=2, label=f'FastRP (AUC={roc_auc:.3f})', color='red')
        ax.plot([0, 1], [0, 1], 'k--', lw=2, label='Random')