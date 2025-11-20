import argparse
import sys
import os
from pathlib import Path
from datetime import datetime

# Add parent directory to path to allow importing CART
sys.path.append(str(Path(__file__).parent.parent))

from CART import Controller

def main():
    parser = argparse.ArgumentParser(description="Run multi-hop chain analysis in a separate process")
    parser.add_argument('--mode', required=True, choices=['label_aware', 'label_agnostic'])
    parser.add_argument('--hop', type=int, required=True)
    parser.add_argument('--hist', type=int, default=48)
    parser.add_argument('--det', type=int, default=24)
    parser.add_argument('--format', default='jsonl')
    args = parser.parse_args()

    print(f"[{datetime.utcnow().isoformat()}] Starting isolated chain build job")
    print(f"Configuration: Mode={args.mode}, Hop={args.hop}, Window={args.hist}h/{args.det}h, Format={args.format}")

    # Check if output file already exists to avoid wasteful regeneration
    expected_filename = f"{args.mode}_{args.hop}hop_chains.csv"
    if os.path.exists(expected_filename) and os.path.getsize(expected_filename) > 0:
        print(f"[{datetime.utcnow().isoformat()}] Output file {expected_filename} already exists and is not empty. Skipping generation.")
        sys.exit(0)

    try:
        # Initialize controller and analyzer
        controller = Controller()
        # We assume container is running since notebook checks it, but we can check too
        if not controller.status().get('running'):
            print("Neo4j container not running, attempting to start...")
            controller.start()
        
        if not controller.connect():
            print("FATAL: Could not connect to Neo4j")
            sys.exit(1)

        analyzer = controller.SubnetPivotAnalyzer
        if not analyzer.connect():
            print("FATAL: Could not connect to Analyzer")
            sys.exit(1)

        use_labels = (args.mode == 'label_aware')
        
        # Run the analysis
        # This will generate the manifest file in thesis_results/chain_temp/
        analyzer.analyze_multi_hop_chains(
            use_labels=use_labels,
            output_prefix=args.mode,
            n_hops=args.hop,
            use_cache=True,
            workers=1,
            output_format=args.format
        )
        
        print(f"[{datetime.utcnow().isoformat()}] Job completed successfully")
        
    except Exception as e:
        print(f"FATAL: Job crashed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        # Try to close connections gracefully
        try:
            if 'analyzer' in locals():
                analyzer.close()
            if 'controller' in locals():
                controller.close()
        except:
            pass

if __name__ == "__main__":
    main()
