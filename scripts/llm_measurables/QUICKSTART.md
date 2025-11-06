# LLM Measurables - Quick Start Guide

Get started with the LLM Measurables system in 5 minutes.

## What This Does

Systematically queries LLMs to answer ~14-21 questions per facility about:
- Current operational status (producing? suspended? care & maintenance?)
- Recent production output and trends
- Capacity and bottlenecks
- Maintenance schedules (planned/unplanned)
- Input constraints (power, water, acid)
- Logistics issues (roads, rail, ports)
- Incidents (safety, regulatory, security)
- Projects (expansions, debottlenecks)

**Smart routing:** High-consequence facilities (FCS > 80) get daily updates. Low-consequence facilities get monthly updates. Conditional questions (acid, power, rain, security) only go to relevant facilities.

## Prerequisites

```bash
# Python 3.8+
python --version

# Install requests (only external dependency)
pip install requests

# API key for LLM provider
export PERPLEXITY_API_KEY="pplx-..."
# OR
export OPENAI_API_KEY="sk-..."
# OR
export ANTHROPIC_API_KEY="sk-ant-..."
```

## 30-Second Test

```bash
cd /Users/willb/Github/GSMC/facilities

# Test without LLM queries (fast, no API key needed)
python scripts/llm_measurables/test_end_to_end.py \
  --facility facilities/ZAF/zaf-venetia-mine-fac.json

# Full test with 2 LLM queries (requires API key)
export PERPLEXITY_API_KEY="pplx-..."
python scripts/llm_measurables/test_end_to_end.py \
  --facility facilities/ZAF/zaf-venetia-mine-fac.json \
  --run-queries
```

**Expected output:**
```
[Step 1/5] Loading facility...
✓ Loaded: Venetia Mine (zaf-venetia-mine-fac)

[Step 2/5] Tagging with features...
✓ Tagged facility with features:
  - FCS: 20.00/100
  - Cadence: monthly

[Step 3/5] Routing measurables...
✓ Routed 14 measurables (Core Pack only)

[Step 4/5] Composing prompts...
✓ Composed 3 sample prompts

[Step 5/5] Executing LLM queries...
✓ Query Results: 2/2 accepted

✓ End-to-end test completed successfully!
```

## Production Workflow

### Step 1: Tag All Facilities

```bash
# Dry-run to preview FCS scores
python scripts/llm_measurables/feature_tagger.py --dry-run | head -50

# Tag all facilities (adds facility_features to each JSON)
python scripts/llm_measurables/feature_tagger.py
```

**Output:** Each facility JSON gains `facility_features` with FCS score.

### Step 2: Route Measurables

```bash
# Route all facilities and save to JSON
python scripts/llm_measurables/router.py \
  --all \
  --output output/routing_results.json
```

**Output:** `output/routing_results.json` with facility_id → [json_ids] mapping.

### Step 3: Execute Queries (Batch)

```bash
# Query top 10 facilities (highest FCS)
python -c "
import json
with open('output/routing_results.json') as f:
    routing = json.load(f)

# Load facilities, compute FCS, sort
facilities = []
for fac_id in routing.keys():
    country = fac_id[:3].upper()
    fac_path = f'facilities/{country}/{fac_id}.json'
    try:
        with open(fac_path) as f2:
            fac = json.load(f2)
            fcs = fac.get('facility_features', {}).get('consequentiality_score', 0)
            facilities.append((fac_id, fcs))
    except:
        pass

facilities.sort(key=lambda x: x[1], reverse=True)
top_10 = [fid for fid, _ in facilities[:10]]

print(' '.join(top_10))
" | while read -r fac_ids; do
    for fac_id in $fac_ids; do
        country=$(echo $fac_id | cut -c1-3 | tr '[:lower:]' '[:upper:]')
        echo "Querying $fac_id..."
        python scripts/llm_measurables/orchestrator.py \
          --facility facilities/$country/$fac_id.json \
          --output output/results/${fac_id}_results.json
    done
done
```

**Output:** One JSON file per facility in `output/results/`.

### Step 4: Aggregate Results

```python
import json
from pathlib import Path
import pandas as pd

# Load all result files
results = []
for result_file in Path("output/results").glob("*_results.json"):
    with open(result_file) as f:
        results.extend(json.load(f))

# Convert to DataFrame
df = pd.DataFrame(results)

# Filter accepted results only
accepted = df[df["accepted"] == True]

print(f"Total results: {len(df)}")
print(f"Accepted: {len(accepted)} ({100*len(accepted)/len(df):.1f}%)")

# Aggregate by metric
by_metric = accepted.groupby("json_id").agg({
    "confidence": "mean",
    "freshness_days": "mean",
    "result_id": "count"
}).rename(columns={"result_id": "count"})

print("\nResults by metric:")
print(by_metric.sort_values("count", ascending=False))

# Save to parquet
accepted.to_parquet("output/measurable_results.parquet", index=False)
print("\n✓ Saved to output/measurable_results.parquet")
```

## Query a Single Facility

```bash
# Auto-route and query
python scripts/llm_measurables/orchestrator.py \
  --facility facilities/CHL/chl-escondida-mine-fac.json \
  --output output/results/escondida_results.json

# Query specific measurables only
python scripts/llm_measurables/orchestrator.py \
  --facility facilities/CHL/chl-escondida-mine-fac.json \
  --json-ids supply.facility.status.current_operational_state \
             supply.facility.production.last_reported_output \
  --output output/results/escondida_status.json
```

## Python API Usage

```python
from scripts.llm_measurables import MeasurablesOrchestrator
import json
import os

# Load facility
with open("facilities/CHL/chl-escondida-mine-fac.json") as f:
    facility = json.load(f)

# Initialize orchestrator
orchestrator = MeasurablesOrchestrator(
    provider="perplexity",
    api_key=os.getenv("PERPLEXITY_API_KEY")
)

# Run queries (auto-routes based on features)
results = orchestrator.run_facility(facility)

# Filter accepted results
accepted = [r for r in results if r["accepted"]]

# Print summary
for result in accepted:
    print(f"\n{result['json_id']}:")
    print(f"  Value: {result['value']}")
    print(f"  Confidence: {result['confidence']}")
    print(f"  As of: {result['as_of_date']}")
    print(f"  Evidence: {len(result['evidence'])} sources")

# Save
orchestrator.save_results(results, "output/escondida_results.json")
```

## Troubleshooting

### "No module named 'scripts.llm_measurables'"

```bash
# Run from repository root
cd /Users/willb/Github/GSMC/facilities
python scripts/llm_measurables/test_end_to_end.py --facility ...
```

### "API key not found"

```bash
# Set environment variable
export PERPLEXITY_API_KEY="pplx-..."

# Or pass explicitly
python scripts/llm_measurables/orchestrator.py \
  --api-key "pplx-..." \
  --facility ...
```

### "Facility has no facility_features"

```bash
# Tag the facility first
python scripts/llm_measurables/feature_tagger.py

# Or tag a single country
python -c "
from scripts.llm_measurables import FacilityFeatureTagger
import json
from pathlib import Path

tagger = FacilityFeatureTagger()

for fac_file in Path('facilities/CHL').glob('*.json'):
    with open(fac_file) as f:
        fac = json.load(f)
    features = tagger.tag_facility(fac)
    fac['facility_features'] = features
    with open(fac_file, 'w') as f:
        json.dump(fac, f, indent=2, ensure_ascii=False)

print('✓ Tagged all Chile facilities')
"
```

### High rejection rate (>30%)

- Lower `min_confidence` threshold in `measurables_library.json` acceptance_rules
- Increase `max_freshness_days` for historical data
- Use `provisional=true` results for initial analysis
- Check prompt templates for clarity
- Try a different LLM provider (OpenAI gpt-4o often has higher confidence)

## Cost Control

**Estimate before running:**
```python
# Calculate query count
num_facilities = 100
avg_measurables = 16  # Core + 1-2 conditional packs
tokens_per_query = 1500
cost_per_1m_tokens = 5  # Perplexity pricing

total_queries = num_facilities * avg_measurables
total_tokens = total_queries * tokens_per_query
cost = total_tokens * cost_per_1m_tokens / 1_000_000

print(f"Queries: {total_queries}")
print(f"Tokens: {total_tokens:,}")
print(f"Cost: ${cost:.2f}")
```

**Reduce costs:**
- Start with top decile (1,000 facilities, ~$120/month)
- Use haiku/flash models for low-FCS facilities
- Cache facility context (reuse across measurables)
- Reduce cadence (weekly instead of daily)

## Next Steps

- **Pilot:** Run on 100 high-FCS facilities, validate acceptance rate > 80%
- **Scale:** Schedule daily/3x_week/weekly/monthly runs for all facilities
- **Integrate:** Connect to Gate-R for event-driven spawning
- **Enhance:** Add 20+ measurables, build monitoring dashboard

See [README.md](README.md) for full documentation.
