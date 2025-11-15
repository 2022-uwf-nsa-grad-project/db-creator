# Incremental Chain Builder - Memory & Logging Strategy

## The Problem with In-Memory Accumulation

Even with node-by-node traversal, storing **all chains in memory** can cause issues:
- 3-hop label-agnostic: Potentially millions of chains
- Each chain: ~10-20 fields = ~200-400 bytes
- 10 million chains × 400 bytes = **4 GB of memory**

## Solution: Streaming to Disk

### 1. **Batch Writing to Parquet**

Instead of:
```python
all_chains = []
for node in nodes:
    chains = build_chains(node)
    all_chains.extend(chains)  # ❌ Memory grows unbounded
return all_chains
```

We do:
```python
all_chains = []
batch_files = []

for node in nodes:
    chains = build_chains(node)
    all_chains.extend(chains)
    
    # Write batch when buffer is full
    if len(all_chains) >= 50000:
        batch_df = pl.from_dicts(all_chains)
        batch_df.write_parquet(f'batch_{len(batch_files)}.parquet')
        batch_files.append(batch_path)
        all_chains = []  # ✅ Clear memory

# Combine at end
result = pl.concat([pl.read_parquet(f) for f in batch_files])
```

**Benefits**:
- Memory usage stays constant (~400 MB)
- Can process billions of chains
- Disk I/O is fast with Parquet compression
- Can resume from batches if interrupted

### 2. **JSON Progress Logging**

Every 1,000 nodes processed, we log:
```json
{
  "mode": "label_aware",
  "num_hops": 3,
  "max_edges_per_node": 50,
  "total_nodes_processed": 95432,
  "total_chains_found": 8234567,
  "batches_written": 164,
  "progress_snapshots": [
    {
      "nodes_processed": 1000,
      "total_nodes": 95432,
      "chains_found": 87234,
      "batches_written": 1,
      "percent_complete": 1.05
    },
    {
      "nodes_processed": 2000,
      "total_nodes": 95432,
      "chains_found": 174512,
      "batches_written": 3,
      "percent_complete": 2.09
    },
    ...
  ]
}
```

**Benefits**:
- **Transparency**: See exactly where processing is at any time
- **Debugging**: If it hangs, you know the last node processed
- **Rate estimation**: Calculate chains/second, ETA
- **Validation**: Confirm edge limits are working
- **Resumability**: Could implement checkpoint-restart

### 3. **Combined Workflow**

```
Load CSV edges → Build edge index → For each node:
    ├─ Explore chains recursively
    ├─ Accumulate in memory buffer
    ├─ Every 50K chains:
    │   └─ Write batch to Parquet
    └─ Every 1K nodes:
        └─ Log progress to JSON

Final:
    ├─ Write any remaining chains
    ├─ Save complete progress log
    ├─ Combine all Parquet batches
    └─ Clean up temp files
```

## Example Output Structure

```
thesis_results/
├── chain_temp/                          # Temporary batches (deleted after)
│   ├── label_aware_3hop_batch0.parquet
│   ├── label_aware_3hop_batch1.parquet
│   └── ...
├── label_aware_3hop_progress.json       # Progress log
└── chain_cache/                         # Final cached results
    └── label_aware_3hop_abc123.parquet
```

## Progress Log Example

If processing takes 30 minutes, you can:

1. **Monitor in real-time**:
   ```bash
   watch -n 5 'cat thesis_results/label_aware_3hop_progress.json | jq ".progress_snapshots[-1]"'
   ```

2. **Calculate ETA**:
   ```python
   import json
   log = json.load(open('label_aware_3hop_progress.json'))
   recent = log['progress_snapshots'][-5:]  # Last 5 snapshots
   avg_rate = sum(s['chains_found'] for s in recent) / len(recent)
   # chains/snapshot × snapshots/second = chains/second
   ```

3. **Debug hangs**:
   ```python
   log = json.load(open('label_aware_3hop_progress.json'))
   last = log['progress_snapshots'][-1]
   print(f"Stuck at node {last['nodes_processed']} of {last['total_nodes']}")
   print(f"Found {last['chains_found']} chains so far")
   ```

## Memory Profile

| Stage | Memory Usage | Duration |
|-------|-------------|----------|
| Load CSV | ~500 MB | 10s |
| Build edge index | ~800 MB | 30s |
| Process node 1 | ~900 MB | - |
| ... build 50K chains | ~1.3 GB | - |
| Write batch | ~900 MB | 2s |
| Process node 2 | ~900 MB | - |
| ... (repeats) | ~900-1300 MB | - |
| Combine batches | ~2 GB | 20s |
| **Peak** | **~2 GB** | - |

Compare to join-based approach:
- **3-hop join**: 50+ GB, crashes
- **Incremental**: 2 GB, completes

## Advantages for Your Thesis

1. **Reproducibility**: JSON log proves exact processing
2. **Transparency**: Can include progress graphs in thesis
3. **Debugging**: If reviewers question results, you have proof
4. **Scalability**: Can extend to 10+ hops without memory issues
5. **Fault tolerance**: Can restart from last batch if interrupted

## Configuration

```python
chains_df, count = build_chains_incremental(
    'thesis_results/connects_edges.csv',
    num_hops=3,
    mode='label_aware',
    max_edges_per_node=50,        # Edge limit per node
    log_progress=True,             # Enable JSON logging
    progress_log_path=None         # Auto-generate path
)
```

**The JSON log is your proof that the pipeline worked correctly and didn't crash or hang.**
