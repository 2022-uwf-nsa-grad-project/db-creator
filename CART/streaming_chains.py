import os
import pickle
import tempfile
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Optional, Union

from .aggregate_chain_manifest import aggregate_manifest
from .incremental_chain_builder import build_chains_incremental


def _worker_process(
    adj_pickle_path: str,
    start_nodes: List[str],
    num_hops: int,
    mode: str,
    batch_size: int,
    output_dir: str,
    manifest_path: str,
    output_format: str,
):
    # worker process entrypoint: load adjacency and call builder for its partition
    with open(adj_pickle_path, 'rb') as f:
        adj = pickle.load(f)
    # Call builder with edge_index and start_nodes subset; do not combine batches
    _, count, manifest = build_chains_incremental(
        csv_path=None,
        num_hops=num_hops,
        mode=mode,
        batch_size=batch_size,
        combine_batches=False,
        edge_index=adj,
        start_nodes=start_nodes,
        output_dir=output_dir,
        manifest_path=manifest_path,
        output_format=output_format,
    )
    return count, str(manifest) if manifest else None


def run_streaming_with_workers(
    adj: Dict[str, List[dict]],
    num_hops: int,
    mode: str = 'label_aware',
    batch_size: int = 100000,
    workers: int = 1,
    output_dir: Optional[Union[str, Path]] = None,
    output_format: str = 'parquet',
):
    """Partition start nodes and run multiple worker processes that stream chains to disk.

    - adj: adjacency mapping (src -> list of edge dicts or tuples) — picklable
    - num_hops: number of hops to build
    - mode: labeling mode string
    - batch_size: per-worker batch size
    - workers: number of parallel worker processes

    Returns combined total chain count and path to manifest file (if produced)
    """
    tempdir = Path(output_dir or 'thesis_results/chain_temp')
    tempdir.mkdir(parents=True, exist_ok=True)

    # Serialize adjacency to temp file for workers to load
    adj_pickle = tempfile.NamedTemporaryFile(delete=False, suffix='.pkl', dir=str(tempdir))
    adj_pickle_path = adj_pickle.name
    adj_pickle.close()
    with open(adj_pickle_path, 'wb') as f:
        pickle.dump(adj, f)

    # Partition start nodes deterministically
    start_nodes = list(adj.keys())
    partitions = [[] for _ in range(workers)]
    for i, n in enumerate(start_nodes):
        partitions[i % workers].append(n)

    total = 0
    worker_manifests: List[str] = []
    futures = []
    with ProcessPoolExecutor(max_workers=workers) as exe:
        for idx, part in enumerate(partitions):
            if not part:
                continue
            worker_dir = tempdir / f'worker_{idx}'
            worker_dir.mkdir(parents=True, exist_ok=True)
            worker_manifest = tempdir / f'{mode}_{num_hops}hop_worker{idx}.manifest'
            futures.append(
                exe.submit(
                    _worker_process,
                    adj_pickle_path,
                    part,
                    num_hops,
                    mode,
                    batch_size,
                    str(worker_dir),
                    str(worker_manifest),
                    output_format,
                )
            )

        for fut in as_completed(futures):
            try:
                c, manifest_path = fut.result()
                total += c
                if manifest_path:
                    worker_manifests.append(manifest_path)
            except Exception as exc:
                print('Worker failed:', exc)

    # Remove adjacency pickle
    try:
        os.unlink(adj_pickle_path)
    except Exception:
        pass

    combined_manifest = tempdir / f'{mode}_{num_hops}hop_batches.manifest'
    if worker_manifests:
        with open(combined_manifest, 'w') as out:
            for manifest in worker_manifests:
                with open(manifest, 'r') as mf:
                    out.write(mf.read())
        manifest_path = combined_manifest
    else:
        manifest_path = None

    return total, manifest_path


def run_streaming_inproc(
    adj: Dict[str, List[dict]],
    num_hops: int,
    mode: str = 'label_aware',
    batch_size: int = 100000,
    output_dir: Optional[Union[str, Path]] = None,
    output_format: str = 'parquet',
):
    """Run a single-process streaming build using the provided adjacency mapping.

    Returns (total_count, manifest_path)
    """
    # Directly call builder in-process so we don't need to pickle adjacency.
    _, total, manifest = build_chains_incremental(
        csv_path=None,
        num_hops=num_hops,
        mode=mode,
        batch_size=batch_size,
        combine_batches=False,
        edge_index=adj,
        start_nodes=None,
        output_dir=output_dir,
        output_format=output_format,
    )
    manifest_path = str(manifest) if manifest else None
    return total, manifest_path


def aggregate_manifest_wrapper(manifest_path: str, output_summary: str):
    return aggregate_manifest(manifest_path, output_summary)
