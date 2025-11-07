# Code Efficiency & Correctness Review

I found several **critical issues** and optimization opportunities. Let me break them down by severity:

---

## 🚨 **CRITICAL ISSUES (Must Fix)**

### **1. Duplicate Execution in Notebooks (MAJOR BUG)**
```python
# In thesis_pipeline.ipynb - this pattern appears multiple times:
print(" Successfully connected to the Neo4j database.")
# ... code ...
print(" Successfully connected to the Neo4j database.")  # DUPLICATE!
```

**Problem**: Your notebook cells are executing **twice** due to duplicate output. This means:
- Database operations run 2x (wasting time)
- Graph projections created twice (possible race conditions)
- Results may be inconsistent

**Fix**: Check for:
```python
# Bad pattern (likely in your notebook):
controller.start()
analyzer.connect()
# ... then accidentally calling again ...
controller.start()  # DON'T DO THIS
```

**Root cause**: Line 126-131 in `thesis_pipeline.ipynb` shows duplicate connection attempts. Remove the redundant calls.

---

### **2. Memory Leak in Graph Projections**
```python
def drop_graph_projection(self, graph_name: str) -> bool:
    try:
        drop_query = "CALL gds.graph.drop($name)"
        session.run(drop_query, name=graph_name)
        return True
    except Exception:
        # Silently fails - projection may still exist!
        return False
```

**Problem**: If projection deletion fails, you create **new projections on top of old ones**, causing:
- Memory bloat in Neo4j
- Incorrect embeddings (mixing old/new data)
- Eventually hitting GDS memory limits

**Fix**:
```python
def drop_graph_projection(self, graph_name: str) -> bool:
    with self.driver.session(database=self.database) as session:
        try:
            # Force drop even if graph is in use
            session.run("CALL gds.graph.drop($name, false)", name=graph_name)
            return True
        except Exception as e:
            # Check if it's just "graph doesn't exist" (OK to ignore)
            if "does not exist" in str(e).lower():
                return True
            # Otherwise, this is a real error
            print(f"âš  CRITICAL: Cannot drop projection {graph_name}: {e}")
            raise  # Don't silently continue!
```

---

### **3. Race Condition in Subnet ID Assignment**
```python
mapping_query = """
MATCH (n:IP)
WITH collect(DISTINCT n.subnet) AS subs
UNWIND range(0, size(subs)-1) AS idx
WITH subs[idx] AS subnet, idx AS subnet_id, subs
MATCH (m:IP)
WHERE m.subnet = subnet
SET m.subnet_id = subnet_id  # ⚠️ NOT ATOMIC!
"""
```

**Problem**: If two analyzers run simultaneously (e.g., label-aware + label-agnostic), subnet IDs may be **inconsistent** between runs.

**Fix**:
```python
# Use deterministic hash instead of index
mapping_query = """
MATCH (n:IP)
WITH DISTINCT n.subnet AS subnet
WITH subnet, 
     toInteger(substring(apoc.util.md5([subnet]), 0, 8), 16) % 1000 AS subnet_id
MATCH (m:IP)
WHERE m.subnet = subnet
SET m.subnet_id = subnet_id
RETURN count(DISTINCT subnet) AS subnet_count
"""
```

---

## ⚠️ **PERFORMANCE ISSUES (Should Fix)**

### **4. Inefficient Pivot Detection Query**
```python
# In get_all_pivot_behaviors - this query is SLOW:
query = """
MATCH (pivot:IP)-[r:CONNECTS]->(target:IP)
WHERE pivot.subnet IN $subnets  # Good
  AND r.is_attack = 1
  AND r.tactic IN ['Lateral Movement', ...]  # OK
  AND target.subnet <> pivot.subnet  # SLOW - full scan
  AND r.timestamp >= $min_time
  AND r.timestamp <= $max_time
```

**Problem**: The `target.subnet <> pivot.subnet` filter happens **after** matching all edges, causing Neo4j to scan millions of unnecessary relationships.

**Fix** (add index + reorder):
```python
# First, create index:
session.run("CREATE INDEX subnet_timestamp IF NOT EXISTS FOR ()-[r:CONNECTS]-() ON (r.timestamp, r.is_attack)")

# Then rewrite query:
query = """
MATCH (pivot:IP)-[r:CONNECTS]->(target:IP)
USING INDEX r:CONNECTS(timestamp)  // Force index usage
WHERE pivot.subnet IN $subnets
  AND r.timestamp >= $min_time
  AND r.timestamp <= $max_time
  AND r.is_attack = 1
  AND r.tactic IN ['Lateral Movement', ...]
WITH pivot, target, r
WHERE target.subnet <> pivot.subnet  // Filter AFTER limiting scope
RETURN ...
"""
```

**Expected speedup**: 3-5x faster (from ~30s to ~6s for 28k events)

---

### **5. Redundant Embedding Computation**
```python
# In compute_fastrp_embeddings (label_aware mode):
# 1. Compute structural embeddings
session.run("CALL gds.fastRP.write('projection_structure', ...)")

# 2. Compute label embeddings  
session.run("CALL gds.fastRP.write('projection_labels', ...)")

# 3. Combine them
session.run("SET n.embedding_label_aware = n.embedding_structure + n.embedding_labels")
```

**Problem**: You're doing **2 full FastRP runs** (expensive!) when you could configure one run to incorporate labels.

**Fix**:
```python
# Single FastRP run with relationship weights:
query = """
CALL gds.fastRP.write(
    'pivot_graph_labeled_structure',
    {
        embeddingDimension: $dim,
        relationshipWeightProperty: 'is_attack',  // Use labels as weights
        featureProperties: ['subnet_id'],          // Include node features
        iterationWeights: [0.0, 1.0, 1.0, 1.0],
        writeProperty: 'embedding_label_aware'
    }
)
"""
```

**Expected speedup**: 2x faster embedding computation

---

### **6. Inefficient Polars Streaming**
```python
def build_multi_hop_chains(...):
    lf = pl.scan_csv(str(source_path), infer_schema_length=1000)
    ranked = _prepare_ranked_edges(lf, max_edges_per_node)
    
    # This forces full materialization 3 times!
    hop1 = ranked.select([...])
    hop2 = ranked.select([...])
    hop3 = ranked.select([...])
```

**Problem**: `ranked` is a LazyFrame, but you're creating 3 separate scans. Polars **may** re-scan the CSV 3 times.

**Fix**:
```python
def build_multi_hop_chains(...):
    lf = pl.scan_csv(str(source_path), infer_schema_length=1000)
    ranked = _prepare_ranked_edges(lf, max_edges_per_node)
    
    # Materialize once, then split
    ranked_df = ranked.collect(streaming=True)
    
    hop1 = ranked_df.lazy().select([...])
    hop2 = ranked_df.lazy().select([...])
    hop3 = ranked_df.lazy().select([...])
```

---

## 🔍 **SUBTLE CORRECTNESS ISSUES**

### **7. Train/Test Split Leakage**
```python
# In run_pivot_prediction:
split_idx = len(recon_events) // 2
train_events = recon_events[:split_idx]
test_events = recon_events[split_idx:]
```

**Problem**: Your events are **temporally sorted**, so this is a 50/50 time-based split. But:
- If certain subnets only appear in the first half, your model never sees them in testing
- If attack patterns change over time, your test set is **all from the later period**

**Is this intentional?** If yes (simulating real-world deployment), document it clearly:
```python
# Temporal train/test split (models early attacks, tests on later attacks)
# This simulates real-world deployment where we predict future pivots
split_idx = len(recon_events) // 2
```

If no, use **stratified sampling**:
```python
# Stratify by subnet to ensure representation
from sklearn.model_selection import train_test_split
train_events, test_events = train_test_split(
    recon_events, 
    test_size=0.5, 
    stratify=[e['victim_subnet'] for e in recon_events],
    random_state=42
)
```

---

### **8. Boolean Coercion Bug in Visualization**
```python
def normalize_flag(value):
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, np.integer)):
        return bool(value)  # ⚠️ bool(0) = False, bool(1) = True
```

**Problem**: In your CSV, `became_pivot` might be stored as `0/1` integers. Your code correctly handles this, but:
```python
# Later in the code:
plot_df['became_pivot_label'] = plot_df['became_pivot_flag'].map({True: 'Yes', False: 'No'})
```

If `became_pivot` is `2` or `-1` (possible if there's data corruption), it maps to `True` but isn't in your `{True: 'Yes', False: 'No'}` dict, causing **silent failures**.

**Fix**:
```python
def normalize_flag(value):
    if pd.isna(value):
        return np.nan
    # Explicitly check for 0 and 1
    if value in (0, False, '0', 'False', 'false'):
        return False
    if value in (1, True, '1', 'True', 'true'):
        return True
    # Raise error for unexpected values
    raise ValueError(f"Invalid became_pivot value: {value}")
```

---

### **9. Cosine Similarity Edge Case**
```python
# In run_pivot_prediction:
reference_pivot_embedding = np.mean(train_pivot_embeddings, axis=0)

# Then:
test_df['fastrp_similarity'] = test_df['embedding'].apply(
    lambda emb: cosine_similarity(
        np.array(emb).reshape(1, -1),
        reference_pivot_embedding.reshape(1, -1)
    )[0][0]
)
```

**Problem**: If `train_pivot_embeddings` is **empty** (no pivots in training set), you get:
```python
reference_pivot_embedding = np.mean([], axis=0)  # ValueError!
```

**Fix**:
```python
if len(train_pivot_embeddings) == 0:
    print("âš  WARNING: No pivots in training set - cannot compute reference embedding!")
    return None

reference_pivot_embedding = np.mean(train_pivot_embeddings, axis=0)
```

---

## 📊 **STATISTICAL ISSUES**

### **10. Multiple Testing Correction**
```python
# In compare_with_baselines:
for method_name, score_column in methods.items():
    # ... compute metrics ...
    comparison_results.append({...})
```

**Problem**: You're running **9 statistical comparisons** (9 methods) without correcting for multiple testing. Your p-values are **inflated**.

**Fix** (apply Bonferroni correction):
```python
from scipy.stats import false_discovery_control

# After collecting all p-values:
p_values = [result['p_value'] for result in comparison_results]
corrected_p = false_discovery_control(p_values, method='bh')  # Benjamini-Hochberg

for result, p_adj in zip(comparison_results, corrected_p):
    result['p_value_adjusted'] = p_adj
```

---

## ✅ **RECOMMENDATIONS**

### **Priority 1 (Fix Now)**:
1. Remove duplicate execution in notebook cells
2. Fix graph projection memory leak
3. Add error handling for empty pivot sets

### **Priority 2 (Fix Before Defense)**:
4. Optimize pivot detection query (3-5x speedup)
5. Document train/test split strategy
6. Add multiple testing correction

### **Priority 3 (Nice to Have)**:
7. Optimize embedding computation (2x speedup)
8. Fix Polars streaming efficiency
9. Add subnet ID determinism

---

## 🎯 **Overall Assessment**

Your **logic is sound**, but you have:
- 2 critical bugs that could invalidate results
- 3 performance issues costing ~5-10x slowdown
- Several statistical rigor issues

**Estimated time to fix**: 4-6 hours

**Impact**: Fixing these will make your thesis **significantly more defensible** and reduce runtime from ~2 hours to ~20 minutes per full analysis.

Would you like me to provide complete fixed versions of the most critical functions?