#!/usr/bin/env python3
"""
Analyze Attack Tactics Distribution

See exactly what MITRE ATT&CK tactics are in the dataset
and how they're distributed.
"""

from CART.analyzers import SubnetPivotAnalyzer

def main():
    print("="*80)
    print("ATTACK TACTICS DISTRIBUTION ANALYSIS")
    print("="*80)
    
    analyzer = SubnetPivotAnalyzer()
    
    try:
        analyzer.connect()
        
        with analyzer.driver.session(database=analyzer.database) as session:
            
            # 1. Overall tactic distribution
            print("\n--- Overall Tactic Distribution ---")
            
            # First get total count
            total_query = """
            MATCH ()-[r:CONNECTS]->()
            WHERE r.is_attack = 1
            RETURN count(*) as total
            """
            total_attacks = session.run(total_query).single()['total']
            
            query1 = """
            MATCH ()-[r:CONNECTS]->()
            WHERE r.is_attack = 1
            RETURN r.tactic as tactic, 
                   count(*) as count
            ORDER BY count DESC
            """
            results1 = session.run(query1).data()
            
            # Calculate percentages in Python
            for row in results1:
                row['percentage'] = (row['count'] / total_attacks * 100) if total_attacks > 0 else 0
            
            print(f"\n{'Tactic':<30} {'Count':>12} {'Percentage':>12}")
            print("-" * 60)
            for row in results1:
                tactic = row['tactic'] if row['tactic'] else 'null/none'
                print(f"{tactic:<30} {row['count']:>12,} {row['percentage']:>11.2f}%")
            
            # 2. Cross-subnet attacks by tactic
            print("\n\n--- Cross-Subnet Attacks by Tactic ---")
            query2 = """
            MATCH (src:IP)-[r:CONNECTS]->(dst:IP)
            WHERE r.is_attack = 1
              AND src.subnet <> dst.subnet
            RETURN r.tactic as tactic,
                   count(*) as count,
                   count(DISTINCT src.subnet) as source_subnets,
                   count(DISTINCT dst.subnet) as target_subnets
            ORDER BY count DESC
            """
            results2 = session.run(query2).data()
            
            print(f"\n{'Tactic':<30} {'Count':>12} {'Src Subnets':>12} {'Dst Subnets':>12}")
            print("-" * 72)
            for row in results2:
                tactic = row['tactic'] if row['tactic'] else 'null/none'
                print(f"{tactic:<30} {row['count']:>12,} {row['source_subnets']:>12} {row['target_subnets']:>12}")
            
            # 3. Technique distribution for top tactics
            print("\n\n--- Techniques for Top Tactics ---")
            query3 = """
            MATCH ()-[r:CONNECTS]->()
            WHERE r.is_attack = 1
              AND r.tactic IS NOT NULL
            RETURN r.tactic as tactic,
                   collect(DISTINCT r.technique)[0..10] as techniques,
                   count(DISTINCT r.technique) as technique_count,
                   count(*) as total_attacks
            ORDER BY total_attacks DESC
            LIMIT 5
            """
            results3 = session.run(query3).data()
            
            for row in results3:
                print(f"\n{row['tactic']}:")
                print(f"  Total attacks: {row['total_attacks']:,}")
                print(f"  Unique techniques: {row['technique_count']}")
                techniques = [t for t in row['techniques'] if t]
                if techniques:
                    print(f"  Sample techniques: {', '.join(techniques[:5])}")
                else:
                    print(f"  No techniques recorded")
            
            # 4. Same-subnet vs Cross-subnet attacks
            print("\n\n--- Same-Subnet vs Cross-Subnet Attacks ---")
            query4 = """
            MATCH (src:IP)-[r:CONNECTS]->(dst:IP)
            WHERE r.is_attack = 1
            WITH CASE WHEN src.subnet = dst.subnet THEN 'Same Subnet' ELSE 'Cross Subnet' END as type,
                 count(*) as count
            RETURN type, count
            ORDER BY count DESC
            """
            results4 = session.run(query4).data()
            
            total_attacks = sum(r['count'] for r in results4)
            print(f"\n{'Type':<20} {'Count':>12} {'Percentage':>12}")
            print("-" * 48)
            for row in results4:
                pct = row['count'] / total_attacks * 100
                print(f"{row['type']:<20} {row['count']:>12,} {pct:>11.2f}%")
            
            # 5. Reconnaissance followed by other tactics
            print("\n\n--- What Follows Reconnaissance? ---")
            query5 = """
            MATCH (a:IP)-[r1:CONNECTS]->(v:IP)
            WHERE r1.is_attack = 1 AND r1.tactic = 'Reconnaissance'
            
            WITH v.subnet as victim_subnet, r1.timestamp as recon_time
            LIMIT 1000
            
            MATCH (pivot:IP)-[r2:CONNECTS]->(target:IP)
            WHERE pivot.subnet = victim_subnet
              AND r2.is_attack = 1
              AND r2.timestamp > recon_time
              AND r2.timestamp <= recon_time + 21600  // 6 hours
              AND target.subnet <> victim_subnet
            
            RETURN r2.tactic as follow_up_tactic,
                   count(*) as count,
                   avg(r2.timestamp - recon_time) / 3600.0 as avg_hours_after
            ORDER BY count DESC
            """
            results5 = session.run(query5).data()
            
            print(f"\n{'Follow-up Tactic':<30} {'Count':>12} {'Avg Hours After':>15}")
            print("-" * 62)
            for row in results5:
                tactic = row['follow_up_tactic'] if row['follow_up_tactic'] else 'null/none'
                print(f"{tactic:<30} {row['count']:>12,} {row['avg_hours_after']:>14.2f}")
        
        print("\n" + "="*80)
        print("RECOMMENDATIONS")
        print("="*80)
        
        # Calculate what percentage are cross-subnet
        cross_subnet = [r for r in results4 if r['type'] == 'Cross Subnet']
        if cross_subnet:
            cross_pct = cross_subnet[0]['count'] / total_attacks * 100
            
            if cross_pct < 30:
                print(f"\n⚠ Only {cross_pct:.1f}% of attacks are cross-subnet!")
                print("  This is why your pivot rate is still high.")
                print("\n  Options:")
                print("  1. Relax cross-subnet requirement (but may be less meaningful)")
                print("  2. Add other constraints (e.g., burst behavior, target diversity)")
                print("  3. Accept high pivot rate but focus on embedding discrimination")
            else:
                print(f"\n✓ {cross_pct:.1f}% of attacks are cross-subnet - good!")
        
        # Check if we should focus on specific tactics
        if results5:
            top_follow_up = results5[0]
            print(f"\n✓ After reconnaissance, '{top_follow_up['follow_up_tactic']}' is most common")
            print(f"  Consider focusing ONLY on this tactic for true pivots")
    
    finally:
        analyzer.close()

if __name__ == "__main__":
    main()
