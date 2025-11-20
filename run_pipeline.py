
from pathlib import Path
from datetime import datetime
import glob
import json
import shutil
import pandas as pd
import numpy as np
import sys
import os
import multiprocessing

# Add current directory to path so we can import CART
sys.path.append(str(Path.cwd()))

from CART import Controller

# Window configuration (optimized from prior sweep)
HISTORICAL_WINDOW_HOURS = 48
DETECTION_WINDOW_HOURS = 24
EMBEDDING_DIM = 128

# Multi-hop chain analysis configuration
N_HOPS = list(range(2, 16))  # Based on max depth of 8 hops, add buffer to 15
USE_CHAIN_CACHE = True
CHAIN_OUTPUT_FORMAT = 'jsonl'
WORKERS = multiprocessing.cpu_count()

# Pipeline control flags
REBUILD_DATABASE = True    # Rebuild to fix duplicate data issue
RUN_WINDOW_SWEEP = False
USE_OPTIMIZED_WINDOW = True

# Experiment selection
RUN_LABEL_AWARE = True
RUN_LABEL_AGNOSTIC = True
LABEL_AGNOSTIC_LIMIT = None

# Post-processing
SHUTDOWN_AFTER_RUN = False
REFRESH_CONNECTS_EXPORT = True
GENERATE_THESIS_ARTIFACTS = True

OUTPUT_DIR = Path('thesis_results')
OUTPUT_DIR.mkdir(exist_ok=True)

def main():
    # Start or connect to the shared Neo4j controller container
    controller = Controller()
    
    status = controller.status()
    controller.stop()
    controller.remove()
    controller.start()
    if not status.get('running'):
        print('Starting Neo4j container...')
        controller.start()
    else:
        print('Neo4j container already running.')

    if not controller.connect():
        raise RuntimeError('Could not connect to Neo4j; check container logs.')
    print('Controller connected to Neo4j.')

    # Optional database rebuild
    if REBUILD_DATABASE:
        print('Rebuilding database with full dataset...')
        controller.build_database(rebuild=True)
    else:
        print('Skipping database rebuild.')

    # Prepare SubnetPivotAnalyzer
    analyzer = controller.SubnetPivotAnalyzer
    if not analyzer.connect():
        raise RuntimeError('Analyzer could not connect to Neo4j.')

    # Patch full recon sampling
    original_fn = analyzer.identify_reconnaissance_victims_by_subnet

    def patched(self, use_labels: bool, historical_window_hours: int):
        print('\n--- Identifying Reconnaissance Victims by Subnet (full corpus) ---')
        with self.driver.session(database=self.database) as session:
            if use_labels:
                query = (
                    "MATCH (a:IP)-[r:CONNECTS]->(v:IP)\n"
                    "WHERE r.is_attack = 1 AND r.tactic = 'Reconnaissance'\n"
                    "WITH DISTINCT v.subnet as victim_subnet, r.timestamp as recon_time\n"
                    "ORDER BY recon_time\n"
                    "RETURN victim_subnet, recon_time"
                )
            else:
                query = (
                    "MATCH (a:IP)-[r1:CONNECTS]->(v:IP)\n"
                    "WHERE exists { (v)-[:CONNECTS]->() }\n"
                    "WITH DISTINCT v.subnet as victim_subnet, r1.timestamp as recon_time\n"
                    "ORDER BY recon_time\n"
                    "RETURN victim_subnet, recon_time"
                )
                if LABEL_AGNOSTIC_LIMIT is not None:
                    query += f'\nLIMIT {int(LABEL_AGNOSTIC_LIMIT)}'
            result = session.run(query).data()
        print(f"  ✓ Found {len(result):,} reconnaissance events")
        return result

    analyzer.identify_reconnaissance_victims_by_subnet = patched.__get__(analyzer, analyzer.__class__)

    # Run experiments
    executed_prefixes = []
    overall_start = datetime.utcnow()
    
    try:
        # 1. Run Label Aware Prediction
        if RUN_LABEL_AWARE:
            print('\n' + '=' * 80)
            print(f"Executing Label Aware experiment")
            print('=' * 80)
            analyzer.run_full_analysis(
                mode='label_aware',
                historical_window_hours=HISTORICAL_WINDOW_HOURS,
                detection_window_hours=DETECTION_WINDOW_HOURS,
                embedding_dim=EMBEDDING_DIM,
                n_hops=N_HOPS, # Run all hops (analyzer will skip if exists)
                chain_output_format=CHAIN_OUTPUT_FORMAT,
                workers=WORKERS
            )
            executed_prefixes.append('label_aware')

        # 2. Run Label Agnostic (Full)
        if RUN_LABEL_AGNOSTIC:
            print('\n' + '=' * 80)
            print(f"Executing Label Agnostic experiment")
            print('=' * 80)
            analyzer.run_full_analysis(
                mode='label_agnostic',
                historical_window_hours=HISTORICAL_WINDOW_HOURS,
                detection_window_hours=DETECTION_WINDOW_HOURS,
                embedding_dim=EMBEDDING_DIM,
                n_hops=N_HOPS, # Run all hops (analyzer will skip if exists)
                chain_output_format=CHAIN_OUTPUT_FORMAT,
                workers=WORKERS
            )
            executed_prefixes.append('label_agnostic')

        if {'label_aware', 'label_agnostic'}.issubset(set(executed_prefixes)):
            analyzer.compare_analysis_modes()
            
    finally:
        analyzer.identify_reconnaissance_victims_by_subnet = original_fn
        overall_finish = datetime.utcnow()
        print(f"Total analysis runtime: {(overall_finish - overall_start).total_seconds():.1f} seconds")

    # Archive artifacts
    run_stamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    run_dir = OUTPUT_DIR / f'run_{run_stamp}_h{HISTORICAL_WINDOW_HOURS}_d{DETECTION_WINDOW_HOURS}'
    run_dir.mkdir(parents=True, exist_ok=True)

    def add_window_tag(name: str) -> str:
        if name.startswith('label_aware_'):
            return name.replace('label_aware_', f"label_aware_h{HISTORICAL_WINDOW_HOURS}_d{DETECTION_WINDOW_HOURS}_", 1)
        if name.startswith('label_agnostic_'):
            return name.replace('label_agnostic_', f"label_agnostic_h{HISTORICAL_WINDOW_HOURS}_d{DETECTION_WINDOW_HOURS}_", 1)
        return name

    patterns = [f"{prefix}_*" for prefix in executed_prefixes]
    patterns.append('mode_comparison.png')

    moved = []
    for pattern in patterns:
        for src_path in Path('.').glob(pattern):
            if not src_path.is_file():
                continue
            dest_name = add_window_tag(src_path.name)
            dest_path = run_dir / dest_name
            shutil.move(str(src_path), dest_path)
            moved.append(dest_path)
            print(f'Moved {src_path.name} -> {dest_path}')

    # Export CONNECTS edges
    CONNECTS_EXPORT_PATH = OUTPUT_DIR / 'connects_edges.csv'
    export_cypher = """
    CALL apoc.export.csv.query(
      "MATCH (a:IP)-[r:CONNECTS]->(b:IP)\n   RETURN a.address AS src,\n          b.address AS dst,\n          r.timestamp AS ts,\n          r.is_attack AS is_attack",
      'thesis_results/connects_edges.csv',
      {batchSize: 50000, delimiter: ',', quotes: false}
     )
    YIELD file, source, format, nodes, relationships, properties, time
    RETURN file, source, format, nodes, relationships, properties, time;
    """

    if REFRESH_CONNECTS_EXPORT or not CONNECTS_EXPORT_PATH.exists():
        print('Exporting IP→IP CONNECTS edges via APOC...')
        with analyzer.driver.session(database=analyzer.database) as session:
            summary_records = session.run(export_cypher).data()
            print('CSV export complete.')

    # Generate thesis artifacts
    if GENERATE_THESIS_ARTIFACTS:
        print('\n' + '=' * 80)
        print('GENERATING THESIS ARTIFACTS')
        print('=' * 80)
        import subprocess
        
        # Ensure generate_all_thesis_artifacts.py exists
        if not Path('generate_all_thesis_artifacts.py').exists():
            print("generate_all_thesis_artifacts.py not found. Please create it.")
        else:
            result = subprocess.run(
                [sys.executable, 'generate_all_thesis_artifacts.py', str(run_dir)],
                cwd=str(Path.cwd()),
                capture_output=True,
                text=True
            )
            print(result.stdout)
            if result.stderr:
                print("STDERR:", result.stderr)

    controller.close()

if __name__ == '__main__':
    main()
