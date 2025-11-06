# LLM Measurables System

A production-ready framework for systematically querying LLMs about facility operational metrics using structured prompts, acceptance criteria, and evidenced results.

## Design Philosophy

**Problem:** 10,641 facilities × 100s of potential questions = infeasible to ask everything.

**Solution:** Smart routing based on facility features + consequentiality scoring + conditional question packs.

## Architecture

### Components

1. **Feature Tagger** (`feature_tagger.py`)
   - Derives `facility_features` from facility metadata
   - Computes process_type, mine_method, acid_dependency, power_intensity, climate_zone, etc.
   - Calculates Facility Consequentiality Score (FCS) for cadence prioritization

2. **Router** (`router.py`)
   - Selects measurables per facility based on features
   - Core Pack (14 questions) → all facilities
   - Conditional Packs (6 packs) → routed by features
   - Cadence assignment based on FCS

3. **Prompt Composer** (`prompt_composer.py`)
   - Injects facility context into prompt templates
   - Substitutes variables: {FACILITY_CANONICAL_NAME}, {PROCESS_TYPE}, {PRIMARY_METALS}, etc.
   - Computes SHA-256 prompt hash for auditability

4. **Orchestrator** (`orchestrator.py`)
   - Executes queries via LLM providers (Perplexity, OpenAI, Anthropic)
   - Validates responses against schema
   - Applies acceptance criteria (confidence, freshness thresholds)
   - Persists results with evidence chains

### Data Schemas

1. **facility.schema.json** (extended with `facility_features`)
   - Added: process_type, mine_method, acid_dependency, power_intensity, climate_zone, port_dependency, water_intensity, country_risk_bucket, consequentiality_score, single_point_failure

2. **measurables_library.json** (21 measurables across 6 packs)
   - Core Pack (14): status, production, capacity, maintenance, inputs, logistics, incidents, regulatory, projects
   - Acid-Dependent Pack (2): sulfuric acid constraints
   - Power-Intensive Pack (2): load-shedding, tariff changes
   - Rain-Sensitive Pack (1): rainfall disruptions
   - Smelter/Refiner Pack (1): feedstock constraints
   - Security-Risk Pack (1): blockades, security incidents

3. **measurable_result.schema.json**
   - Time-series results with evidence, confidence, freshness, acceptance status
   - Supports superseding, status change detection, validation errors

## Quick Start

### 1. Tag Facilities with Features

```bash
cd /Users/willb/Github/GSMC/facilities

# Dry-run to preview FCS scores
python scripts/llm_measurables/feature_tagger.py --dry-run

# Tag all facilities (writes facility_features to each JSON)
python scripts/llm_measurables/feature_tagger.py
```

**Output:** Each facility JSON gains a `facility_features` object with derived attributes and FCS score.

### 2. Route Measurables

```bash
# Route a single facility
python scripts/llm_measurables/router.py \
  --facility facilities/ZAF/zaf-venetia-mine-fac.json

# Route all facilities and save to JSON
python scripts/llm_measurables/router.py \
  --all \
  --output output/routing_results.json
```

**Output:** Shows selected packs, FCS, cadence, and list of json_ids to query.

### 3. Compose Prompts

```bash
# Compose a single prompt
python scripts/llm_measurables/prompt_composer.py \
  --facility facilities/ZAF/zaf-venetia-mine-fac.json \
  --json-id supply.facility.status.current_operational_state

# Save to file
python scripts/llm_measurables/prompt_composer.py \
  --facility facilities/ZAF/zaf-venetia-mine-fac.json \
  --json-id supply.facility.production.last_reported_output \
  --output /tmp/prompt.txt
```

**Output:** Full prompt with facility context injected + SHA-256 hash.

### 4. Execute Queries

```bash
# Set API key
export PERPLEXITY_API_KEY="pplx-..."

# Query a single facility (auto-routes measurables)
python scripts/llm_measurables/orchestrator.py \
  --facility facilities/ZAF/zaf-venetia-mine-fac.json \
  --provider perplexity \
  --output output/results/venetia_results.json

# Query specific measurables only
python scripts/llm_measurables/orchestrator.py \
  --facility facilities/ZAF/zaf-venetia-mine-fac.json \
  --json-ids supply.facility.status.current_operational_state \
             supply.facility.production.last_reported_output \
  --provider perplexity \
  --output output/results/venetia_status.json
```

**Output:** JSON array of results matching `measurable_result.schema.json`.

### 5. Python API Usage

```python
import os
import json
from scripts.llm_measurables import (
    FacilityFeatureTagger,
    MeasurablesRouter,
    PromptComposer,
    MeasurablesOrchestrator
)

# Step 1: Tag facilities
tagger = FacilityFeatureTagger()
stats = tagger.tag_all_facilities(input_dir="facilities/")
print(f"Tagged {stats['tagged']} facilities")

# Step 2: Route measurables
router = MeasurablesRouter()

with open("facilities/ZAF/zaf-venetia-mine-fac.json", "r") as f:
    facility = json.load(f)

json_ids = router.route_facility(facility)
cadence = router.get_cadence(facility)

print(f"Facility: {facility['canonical_name']}")
print(f"FCS: {facility['facility_features']['consequentiality_score']}")
print(f"Cadence: {cadence}")
print(f"Measurables: {len(json_ids)}")

# Step 3: Execute queries
orchestrator = MeasurablesOrchestrator(
    provider="perplexity",
    api_key=os.getenv("PERPLEXITY_API_KEY")
)

results = orchestrator.run_facility(facility, json_ids=json_ids[:3])  # Test with first 3

# Print summary
accepted = sum(1 for r in results if r["accepted"])
print(f"\nResults: {len(results)} queries, {accepted} accepted")

for result in results:
    print(f"\n{result['json_id']}:")
    print(f"  Value: {result['value']}")
    print(f"  Confidence: {result['confidence']}")
    print(f"  Freshness: {result['freshness_days']} days")
    print(f"  Accepted: {result['accepted']}")
    print(f"  Reason: {result['acceptance_reason']}")

# Save results
orchestrator.save_results(results, "output/results/venetia_test.json")
```

## Measurables Library (v0.1.0)

### Core Pack (14 measurables - all facilities)

1. `supply.facility.status.current_operational_state` - Operating status (producing|suspended|care_and_maintenance|commissioning|construction|closed)
2. `supply.facility.production.last_reported_output` - Most recent production figure
3. `supply.facility.production.monthly_run_rate_3m_trend` - Current run-rate and 3-month trend
4. `supply.facility.production.quarterly_forecast_range` - Low/base/high forecast for current/next quarter
5. `supply.facility.capacity.nameplate_and_bottleneck_stage` - Nameplate capacity and current bottleneck
6. `supply.facility.maintenance.unplanned_downtime_last_30d` - Recent unplanned outages
7. `supply.facility.maintenance.planned_downtime_next_90d` - Announced maintenance shutdowns
8. `supply.facility.inputs.power_reliability_status` - Grid power constraints
9. `supply.facility.inputs.water_availability_status` - Water availability constraints
10. `supply.facility.logistics.site_access_status` - Haul road/rail/port/border constraints
11. `supply.facility.incidents.safety_event_last_90d` - Safety incidents affecting production
12. `supply.facility.regulatory.adverse_action_last_180d` - Permit/regulatory/sanction actions
13. `supply.facility.projects.expansion_or_debottleneck_status` - Expansion project status
14. `supply.facility.status.restart_or_startup_timeline` - Restart timeline for suspended facilities

### Conditional Packs

- **Acid-Dependent** (2): SXEW/HPAL/heap leach facilities
- **Power-Intensive** (2): Al/FeSi/Zn smelters, high-risk grids
- **Rain-Sensitive** (1): Open-pit mines in tropical/monsoon zones
- **Smelter/Refiner** (1): Feedstock dependency
- **Security-Risk** (1): High country-risk regions

## Routing Rules

### Cadence (based on FCS)

| FCS Percentile | Cadence | Frequency |
|----------------|---------|-----------|
| ≥90th (FCS≥80) | daily | Every day |
| 70-90th (FCS 60-80) | 3x_week | Mon/Wed/Fri |
| 40-70th (FCS 30-60) | weekly | Monday |
| <40th (FCS<30) | monthly | 1st of month |

### Pack Selection (conditional packs)

| Pack | Trigger Condition |
|------|-------------------|
| acid_dependent | process_type in {sxew, hpal, heap_leach} OR acid_dependency in {high, medium} |
| power_intensive | power_intensity in {very_high, high} OR country_risk_bucket ≥ 3 |
| rain_sensitive | mine_method in {open_pit, both} AND climate_zone in {tropical, monsoon} |
| smelter_refiner | primary_type in {smelter, refinery} |
| security_risk | country_risk_bucket ≥ 3 |

## Acceptance Criteria

Results are accepted if they meet measurable-specific thresholds:

| Measurable Type | Min Confidence | Max Freshness | Dated Source Required |
|----------------|----------------|---------------|----------------------|
| Status changes | 60 | 365 days | Yes |
| Production numbers | 70 | 180 days | Yes |
| Forecasts | 60 | 90 days | Yes |
| Incidents | 70 | 90 days | Yes |
| Constraints (power/water/acid) | 65 | 60 days | Yes |

**Provisional results:** Accepted but flagged if:
- Confidence is within 5 points of threshold
- Freshness is within 1.5× of threshold
- Status change override applies

## FCS (Facility Consequentiality Score)

```
FCS = 0.45 × global_supply_share
    + 0.20 × metal_criticality
    + 0.15 × supply_concentration
    + 0.10 × single_point_failure
    + 0.10 × recent_volatility
```

**Scale:** 0-100

**Components:**
- `global_supply_share`: Facility's % of global supply for primary commodity
- `metal_criticality`: Metal importance (Li/Co/REE=95, Cu=70, Au=40, coal=20)
- `supply_concentration`: Country-level HHI proxy (higher risk → higher concentration)
- `single_point_failure`: 100 if facility >5% global share, else 0
- `recent_volatility`: Historical measurables variance (placeholder for now)

## LLM Provider Support

| Provider | Model Default | JSON Mode | Rate Limit |
|----------|---------------|-----------|------------|
| Perplexity | sonar-pro | No (manual) | 1s delay |
| OpenAI | gpt-4o | Yes | 1s delay |
| Anthropic | claude-3-5-sonnet | No (manual) | 1s delay |

**Recommendation:** Use Perplexity `sonar-pro` for freshness (prioritizes recent sources).

## File Structure

```
scripts/llm_measurables/
├── __init__.py                 # Package exports
├── README.md                   # This file
├── feature_tagger.py           # Derive facility_features
├── router.py                   # Route measurables to facilities
├── prompt_composer.py          # Build prompts with context
└── orchestrator.py             # Execute queries with LLM providers

schemas/
├── facility.schema.json        # Extended with facility_features
├── measurables_library.json    # 21 measurables across 6 packs
└── measurable_result.schema.json  # Time-series results schema

output/
├── routing_results.json        # facility_id → [json_ids] mapping
└── results/
    └── *.json                  # Query results by facility
```

## Next Steps (Production Roadmap)

### Phase 1: Bootstrap (Current)
- ✅ Schema design
- ✅ Feature tagger
- ✅ Router with conditional packs
- ✅ Prompt composer
- ✅ Orchestrator with Perplexity/OpenAI/Anthropic
- ⬜ Tag all 10,641 facilities with features
- ⬜ Validate routing (spot-check 20 facilities across FCS deciles)

### Phase 2: Pilot (100 facilities)
- ⬜ Select 100 high-FCS facilities (top decile)
- ⬜ Run Core Pack queries (14 × 100 = 1,400 queries)
- ⬜ Validate acceptance rate (target: >80%)
- ⬜ Spot-check 20 results for accuracy
- ⬜ Tune confidence/freshness thresholds

### Phase 3: Scale (All facilities)
- ⬜ Schedule daily/3x_week/weekly/monthly runs
- ⬜ Implement status change detection (trigger re-routing)
- ⬜ Build result aggregation (parquet table)
- ⬜ Integrate with Gate-R (event-driven spawning)
- ⬜ Build monitoring dashboard (acceptance rate, latency, cost)

### Phase 4: Enhancements
- ⬜ Add 20+ measurables (see design doc for candidates)
- ⬜ Implement result superseding (time-series management)
- ⬜ Add global supply data ingestion (for accurate FCS)
- ⬜ Build feedback loop (flag low-confidence results for human review)
- ⬜ Optimize prompts (A/B test prompt variations)

## Cost Estimation

**Assumptions:**
- 10,641 facilities
- Average 16 measurables/facility (Core + 1-2 conditional packs)
- Perplexity API: $5/1M tokens
- ~1,500 tokens/query (prompt + response)

**Monthly Cost (steady-state with cadence):**
- Top decile (1,064 facilities): 1,064 × 16 × 30 = 510,720 queries
- D2-D5 (4,256 facilities): 4,256 × 16 × 12 = 816,768 queries
- D6-D10 (5,321 facilities): 5,321 × 16 × 4 = 340,544 queries
- **Total:** 1,668,032 queries/month
- **Tokens:** 1,668,032 × 1,500 = 2.5B tokens/month
- **Cost:** 2.5B × $5/1M = **$12,500/month**

**Optimization levers:**
- Use shorter prompts for high-frequency queries (-20% cost)
- Cache facility context (reuse across measurables) (-10% cost)
- Use cheaper models for low-FCS facilities (-30% cost for tail)

**Optimized cost:** ~$8,000/month

## Questions & Design Clarifiers

Per your original ask, here are my answers:

1. **Facility features available:** ✅ Implemented in schema + feature_tagger.py
2. **Consequentiality (FCS):** ✅ Implemented; needs global supply data ingestion for accuracy
3. **Cadence:** ✅ Daily/3x_week/weekly/monthly based on FCS deciles
4. **Schema:** ✅ Two new schemas: measurables_library.json + measurable_result.schema.json
5. **Query engine:** ✅ Perplexity (sonar-pro) primary; OpenAI/Anthropic fallback
6. **Confidence thresholds:** ✅ Implemented per-measurable acceptance_rules

## Contact

For questions or enhancements, see main repository CLAUDE.md.
