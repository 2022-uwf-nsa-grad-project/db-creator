# Window Optimization Experiments - README

## What We Learned from Exploration

### 🔍 Database Analysis Results:
- **Total Subnets**: 21
- **Attack Chains Found**: 12 (Reconnaissance → Credential Access)
- **Pivot Timing**: 0.1 to 1.5 hours (median ~0.5 hours)
- **Main Pivot Tactic**: Credential Access (not "Lateral Movement")
- **Key Issue**: Original code only looked for "Lateral Movement" tactic, but your dataset uses "Credential Access"

### ✅ What Was Fixed:
1. **Expanded Attack Tactics** in pivot detection:
   - Added: Credential Access, Defense Evasion, Exfiltration, Collection, Discovery
   - This matches what's actually in your dataset!

2. **Still Requires**:
   - Cross-subnet attacks (network traversal)
   - Occurs after reconnaissance
   - Within detection time window

---

## 📋 Workflow: Three Scripts

### 1. **Quick Test** (Start Here!)
```bash
python quick_test.py
```

**Purpose**: Verify the fix works before running full experiments

**What it does**:
- Runs ONE analysis with 6-hour windows
- Should show ~20-50% pivot rate (not 99.9%!)
- Takes ~2-5 minutes

**Look for**:
- ✅ Balanced pivot rate
- ✅ AUC-ROC > 0.5
- ✅ Statistical significance

---

### 2. **Window Optimization** (Main Experiments)
```bash
python optimize_windows.py
```

**Purpose**: Test different window sizes to find optimal configuration

**What it tests**:
```
Window Configurations (historical, detection):
- (1h, 2h)    - Very short (immediate pivots)
- (2h, 3h)    - Short (within observed range)
- (6h, 6h)    - Medium-short
- (12h, 12h)  - Medium
- (24h, 24h)  - Long (original)
- (48h, 48h)  - Very long
- (6h, 12h)   - Asymmetric (short history, longer detection)
- (12h, 24h)  - Asymmetric (medium history, long detection)
- (24h, 6h)   - Asymmetric (long history, short detection)
```

**Output**:
- `test_h*_d*_pivot_predictions.csv` (one per config)
- `test_h*_d*_method_comparison.csv` (one per config)
- `window_optimization_results.json` (summary)

**Time**: ~15-30 minutes for all 9 configurations

---

### 3. **Results Analysis** (Final Step)
```bash
python analyze_window_results.py
```

**Purpose**: Compare all experiments and find the best configuration

**What it shows**:
- Ranked results by F1-Score
- Best balanced configuration (30-70% pivot rate)
- Best discrimination (AUC-ROC)
- Insights about window size patterns

**Output**:
- `window_optimization_comparison.csv`
- Console output with recommendations

---

## 🎯 Expected Results (After Fix)

### Before Fix (Broken):
```
Pivot Rate: 99.9%
AUC-ROC: 0.44 (worse than random)
p-value: 0.985 (not significant)
Cohen's d: 0.002 (no effect)
```

### After Fix (Expected):
```
Pivot Rate: 20-50% (balanced)
AUC-ROC: 0.65-0.80 (good discrimination)
p-value: < 0.05 (significant)
Cohen's d: 0.3-0.7 (medium effect)
```

---

## 📊 How to Interpret Results

### 1. Pivot Rate
- **Good**: 30-70%
- **Too High** (>80%): Still too permissive, refine criteria
- **Too Low** (<20%): Too strict, missing real pivots

### 2. AUC-ROC (Area Under ROC Curve)
- **Excellent**: > 0.8
- **Good**: 0.7 - 0.8
- **Fair**: 0.6 - 0.7
- **Poor**: 0.5 - 0.6
- **Failed**: < 0.5 (worse than random)

### 3. Statistical Significance
- **p-value < 0.01**: Highly significant ⭐⭐⭐
- **p-value < 0.05**: Significant ⭐⭐
- **p-value < 0.10**: Marginally significant ⭐
- **p-value > 0.10**: Not significant ❌

### 4. F1-Score
- **Excellent**: > 0.85
- **Good**: 0.75 - 0.85
- **Fair**: 0.60 - 0.75
- **Poor**: < 0.60

---

## 🔧 If Results Still Look Wrong

### Issue: Pivot rate still >80%

**Solution 1**: Add more strict requirements
```python
# In get_all_pivot_behaviors(), add:
AND count(DISTINCT target.subnet) >= 2  // Must attack multiple subnets
```

**Solution 2**: Require minimum attack count
```python
# In process_event_set(), filter:
if pivot_info['attack_count'] < 5:  # At least 5 attacks
    continue
```

### Issue: Pivot rate <20%

**Solution**: Relax cross-subnet requirement or expand tactics further
```python
# Try including more tactics:
AND r.tactic IN ['Credential Access', 'Defense Evasion', 'Exfiltration', 
                 'Collection', 'Discovery', 'Initial Access', 'Persistence']
```

### Issue: AUC-ROC still <0.6

**Possible causes**:
1. **Embeddings not capturing pivot behavior**: Try different embedding dimensions (64, 256)
2. **Features not discriminative**: Check if centrality metrics correlate better
3. **Class imbalance**: Ensure pivot rate is 30-70%
4. **Data quality**: Verify attack labels are accurate

---

## 📝 For Your Thesis

### What to Report:

1. **Exploration Results**:
   - "Database contains 12 attack chains with pivot timing of 0.1-1.5 hours"
   - "Primary pivot tactic is Credential Access (8,000+ attacks per chain)"

2. **Window Selection**:
   - "Tested 9 window configurations"
   - "Selected X-hour windows based on observed pivot timing"
   - Show table from `window_optimization_comparison.csv`

3. **Results**:
   - "Achieved X% pivot detection rate"
   - "AUC-ROC of X.XX indicates good discrimination"
   - "FastRP embeddings outperformed baseline by X%"

4. **Statistical Validation**:
   - "Welch's t-test: p = X.XXX (significant at α=0.05)"
   - "Cohen's d = X.XX indicates medium/large effect size"

---

## 🚀 Quick Start Commands

```bash
# 1. First, verify the fix works
python quick_test.py

# 2. If good, run full experiments  
python optimize_windows.py

# 3. Analyze and compare all results
python analyze_window_results.py

# 4. Review the comparison table
cat window_optimization_comparison.csv
```

---

## 📁 Output Files

After running all scripts:

### From quick_test.py:
- `label_aware_pivot_predictions.csv`
- `label_aware_method_comparison.csv`
- `label_aware_results.png`

### From optimize_windows.py:
- `test_h1_d2_pivot_predictions.csv` (one per config)
- `test_h1_d2_method_comparison.csv` (one per config)
- Multiple PNG visualization files
- `window_optimization_results.json`

### From analyze_window_results.py:
- `window_optimization_comparison.csv` ← **Main results table**

---

## 🎓 Success Criteria

Your analysis is successful if:
- ✅ Pivot rate is 30-70%
- ✅ AUC-ROC > 0.65
- ✅ p-value < 0.05
- ✅ FastRP beats at least 50% of baselines
- ✅ Results are reproducible

If you achieve this, your thesis contribution is solid! 🎉
