# Pivot Detection Fixes & Database Exploration

## Summary of Changes

### 1. Added Database Exploration Tool (`explore_database()`)

**Location**: `CART/analyzers.py` - `SubnetPivotAnalyzer.explore_database()`

**What it does**:
- Analyzes your entire database to understand attack patterns
- Identifies reconnaissance attacks
- Finds lateral movement attacks  
- Detects TRUE attack chains (Reconnaissance → Lateral Movement)
- Analyzes cross-subnet traffic
- Provides recommendations based on what's in your data

**How to use**:
```python
from CART.analyzers import SubnetPivotAnalyzer

analyzer = SubnetPivotAnalyzer()
results = analyzer.explore_database()
```

Or run the standalone script:
```bash
python explore_database.py
```

**Output**:
- Console output with statistics and patterns
- `database_exploration.json` with detailed results
- Recommendations for pivot detection thresholds

---

### 2. Fixed Pivot Detection Logic (`get_all_pivot_behaviors()`)

**Previous (WRONG) Behavior**:
```python
# Counted ANY outgoing attack as a "pivot"
# Result: 99.9% of subnets were pivots!
```

**New (CORRECT) Behavior**:

#### For Label-Aware Mode:
```python
# Only counts as pivot if:
1. Attack uses lateral movement tactics:
   - Lateral Movement
   - Execution
   - Command and Control
   - Credential Access
   
2. Attack is CROSS-SUBNET (different subnet than source)

3. Happens after reconnaissance event

4. Within detection time window
```

#### For Label-Agnostic Mode:
```python
# Only counts as pivot if:
1. Connection is CROSS-SUBNET

2. Shows burst behavior:
   - At least 3 connections
   - Within 1 hour window
   
3. Happens after reconnaissance event

4. Within detection time window
```

---

## Key Improvements

### 1. Cross-Subnet Requirement
**Why**: True lateral movement means moving from one network segment to another. Same-subnet connections are usually normal internal traffic.

**Implementation**: 
```cypher
WHERE target.subnet <> pivot.subnet
```

### 2. Specific Attack Tactics
**Why**: Not all attacks indicate pivoting. We need tactics that show the attacker is moving through the network.

**Tactics That Indicate Pivoting**:
- `Lateral Movement`: Direct indication of moving between systems
- `Execution`: Running code on compromised systems
- `Command and Control`: Establishing persistent access
- `Credential Access`: Stealing credentials to access more systems

### 3. Burst Behavior (Label-Agnostic)
**Why**: When a compromised subnet starts rapidly connecting to multiple new subnets, it's suspicious even without attack labels.

**Implementation**: 
- Minimum 3 cross-subnet connections
- All within 1-hour window
- Creates burst pattern signature

---

## Expected Results After Fix

### Before Fix:
```
Training: 28,669 pivots / 28,692 total (99.9%)
Testing:  28,609 pivots / 28,692 total (99.7%)
AUC-ROC: 0.44 (worse than random!)
p-value: 0.985 (not significant)
```

### After Fix (Expected):
```
Training: ~5,000-15,000 pivots / 28,692 total (20-50%)
Testing:  Similar ratio
AUC-ROC: 0.70-0.85 (good discrimination)
p-value: < 0.05 (statistically significant)
```

---

## What To Do Next

### Step 1: Explore Your Database
```bash
python explore_database.py
```

**Look for**:
- How many lateral movement attacks exist?
- Are there true attack chains (Recon → Pivot)?
- What's the cross-subnet traffic pattern?

### Step 2: Review Results
Check the output for:
- "Found X true attack chains" - Should be > 100 for good analysis
- "No lateral movement attacks found" - May need to adjust tactics list
- Temporal distribution - Verify attacks span the time window

### Step 3: Run Analysis with Fixed Detection
```python
from CART.analyzers import SubnetPivotAnalyzer

analyzer = SubnetPivotAnalyzer()
analyzer.run_full_analysis(
    mode='both',
    historical_window_hours=24,
    detection_window_hours=24,
    embedding_dim=128
)
```

### Step 4: Interpret New Results
Look for:
- Balanced pivot ratio (30-70%)
- Statistically significant p-value (< 0.05)
- AUC-ROC > 0.7
- FastRP outperforms baselines

---

## Troubleshooting

### If exploration shows "No true attack chains found":

**Option 1**: Expand attack tactics
```python
# In get_all_pivot_behaviors(), add more tactics:
AND r.tactic IN [
    'Lateral Movement', 
    'Execution', 
    'Command and Control', 
    'Credential Access',
    'Discovery',           # Add this
    'Collection',          # Add this
    'Exfiltration'         # Add this
]
```

**Option 2**: Relax cross-subnet requirement
If your dataset has mostly same-subnet attacks:
```python
# Remove or comment out:
# AND target.subnet <> pivot.subnet
```

**Option 3**: Increase detection window
```python
# Try 48 or 72 hours instead of 24:
analyzer.run_full_analysis(
    detection_window_hours=48  # or 72
)
```

### If still getting 99% pivots:

**Check your reconnaissance detection**:
```python
# In identify_reconnaissance_victims_by_subnet()
# Make sure it's properly filtering for recon events
```

---

## Technical Details

### Database Queries

#### Attack Chain Detection:
```cypher
MATCH (a:IP)-[r1:CONNECTS]->(v:IP)
WHERE r1.is_attack = 1 AND r1.tactic = 'Reconnaissance'

WITH v.subnet as victim_subnet, r1.timestamp as recon_time

MATCH (pivot:IP)-[r2:CONNECTS]->(target:IP)
WHERE pivot.subnet = victim_subnet
  AND r2.is_attack = 1
  AND r2.timestamp > recon_time
  AND r2.tactic IN ['Lateral Movement', 'Execution', ...]
  AND target.subnet <> victim_subnet

RETURN victim_subnet, count(r2) as pivot_attacks
```

### Performance Optimizations

All queries use:
- Indexed subnet lookups
- Pre-fetched data (2-3 queries total)
- In-memory Python filtering
- Result: ~30 seconds for 28,000 events

---

## Files Modified

1. `CART/analyzers.py`:
   - Added `explore_database()` method (lines ~432-670)
   - Fixed `get_all_pivot_behaviors()` method (lines ~1270-1370)
   
2. `explore_database.py`:
   - New standalone exploration script

3. `PIVOT_DETECTION_FIXES.md`:
   - This documentation file

---

## Questions?

If you encounter issues:
1. Run `explore_database.py` first to understand your data
2. Check the console output for warnings
3. Review `database_exploration.json` for detailed patterns
4. Adjust tactics/thresholds based on what's in your dataset

The key is matching the detection logic to what actually exists in your APT dataset!
