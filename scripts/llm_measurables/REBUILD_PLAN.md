# LLM Measurables System - Production Rebuild Plan

**Status:** Scaffolding complete, production system pending
**Blocking Issues:** 9 critical items from managerial review
**Estimated Effort:** 40-60 hours for full green-light readiness

---

## Current State Assessment

### ✅ What Works
- Schema foundation (facility.schema.json with corrected facility_features)
- Snowflake DDL (MEASURABLE, MEASURABLE_RESULT, FACILITY_TIER_PLAN with proper constraints)
- Basic Python framework (feature_tagger, router, prompt_composer, orchestrator)
- End-to-end test harness (validates component integration, not production quality)

### ❌ What's Blocking Production
1. **Library depth:** 21 measurables vs. required 300+
2. **Tiering undefined:** No deterministic 5/50/100/500 fill logic
3. **Validation missing:** No per-metric validators, no unit conversion
4. **Cost unproven:** No measured tokens, no rate sheet
5. **Quality unproven:** No golden set, no acceptance metrics
6. **Observability missing:** No structured logs, no metrics dashboard
7. **Data model gaps:** facility_key vs facility_id, unit normalization, idempotency rules
8. **Integration undefined:** No Gate-R connector, no Snowflake upsert logic
9. **Live testing:** No real LLM queries validated (only mocked tests)

---

## Rebuild Roadmap (Priority Order)

### Phase 1: Data Model & Catalog Foundation (12-16 hours)

**Goal:** Fix schema issues and build a credible 300+ measurable catalog.

#### Task 1.1: Fix Facility Features Schema ✅ DONE
- [x] Split process_chain (array) from mine_method (single)
- [x] Remove null from enums
- [x] Add metals[] array
- [x] Add tier field (1/2/3/4)

#### Task 1.2: Build Measurable Catalog (300+ items) - 8-10 hours
**Deliverable:** `schemas/measurables_catalog_v2.json` + CSV index

**Pack Structure** (target counts):
```
Core (20):                    Status, production, capacity, maintenance, inputs, logistics, incidents, regulatory, projects
Power (25):                   Tariffs, curtailments, load-shedding, captive power, fuel supply, grid stability
Feedstock/Market (30):        Concentrate availability, TCRC, offtake, inventory, feedstock sourcing
Acid/Consumables (25):        Acid availability, onsite plant uptime, reagent constraints
Maintenance/Equipment (30):   Planned shutdowns, spare parts, mill/crusher/hoist/ventilation availability
Climate/Weather (25):         Rainfall, floods, drought, wildfire, cyclones, seasonal risk
Security/Community (20):      Strikes, blockades, theft, protests, regional incidents
Regulatory/Permitting (20):   Inspections, environmental orders, taxes, royalties, sanctions
Logistics/Ports (20):         Port throughput, berth allocation, trucking/rail, border closures
Tailings/Water/ESG (25):      TSF capacity, inspections, water allocations, discharge permits
Projects/Construction (15):   EPC status, % complete, critical path, slippage
Metal-specific:
  - Cu/Ni/Co (20)
  - PGM (15)
  - Al smelter (20)
  - Zn/Pb (15)
  - Rare Earth (15)
  - Sn/W (10)
Total: ~335 measurables
```

**Per-measurable fields:**
```json
{
  "json_id": "supply.facility.power.tariff_change_last_180d",
  "prompt_template": "...",
  "unit_canonical": "USD/MWh",
  "value_type": "object",
  "pack": "power",
  "priority": 15,
  "applicability_tags": ["power_intensive", "Al", "FeSi", "Zn"],
  "acceptance_rule_id": "tariff_change_rule",
  "acceptance_min_confidence": 70,
  "acceptance_max_freshness_days": 180,
  "acceptance_require_dated_source": true,
  "rationale": "Power tariff spikes can make smelters uneconomic, triggering curtailments"
}
```

**Build Strategy:**
1. Start with Core (20) - hand-craft high-quality prompts
2. Duplicate and modify for conditional packs (power → 25, feedstock → 30, etc.)
3. Use templates for metal-specific packs (parameterize by metal)
4. Export to JSON + CSV index (for easy review/edit in spreadsheet)

**Acceptance:** Review CSV with 20 random samples; verify pack counts match targets ±10%.

#### Task 1.3: Unit Normalization & Canonical Mapping - 2 hours
**Deliverable:** `utils/unit_converter.py` using `pint` library

**Canonical units by category:**
```
Mass: t (metric tonnes)
Mass rate: tpm (tonnes per month), tpa (tonnes per annum)
Power: MW (megawatts), MWh (megawatt-hours)
Volume: m3 (cubic meters)
Volume rate: m3/d (cubic meters per day)
Currency: USD (convert all to USD at spot rate)
Percentage: % (0-100 scale)
Enum/Boolean: N/A
```

**Functions:**
- `convert_to_canonical(value, raw_unit, canonical_unit) -> (canonical_value, conversion_success)`
- `validate_unit_compatibility(raw_unit, expected_unit) -> bool`

**Tests:** Unit tests with ≥80% coverage.

---

### Phase 2: Tiering & Routing Logic (6-8 hours)

**Goal:** Implement exact 5/50/100/500 deterministic fill logic.

#### Task 2.1: FCS → Tier Mapping - 1 hour
**Deliverable:** `utils/tiering.py`

```python
def compute_tier(fcs: float) -> int:
    """
    Map FCS to tier:
    - FCS ≥ 90: Tier 4 (Platinum, 500 measurables, daily)
    - FCS 70-89: Tier 3 (Gold, 100 measurables, 3×/week)
    - FCS 40-69: Tier 2 (Silver, 50 measurables, weekly)
    - FCS < 40: Tier 1 (Bronze, 5 measurables, monthly)
    """
    if fcs >= 90:
        return 4
    elif fcs >= 70:
        return 3
    elif fcs >= 40:
        return 2
    else:
        return 1

TIER_TARGETS = {1: 5, 2: 50, 3: 100, 4: 500}
TIER_CADENCE = {1: "monthly", 2: "weekly", 3: "3x_week", 4: "daily"}
```

#### Task 2.2: Pack Priority Ordering - 2 hours
**Deliverable:** `config/pack_priorities.yaml`

```yaml
pack_priorities:
  - name: core
    priority: 1
    always_include: true
    applicability: all

  - name: power
    priority: 2
    applicability:
      process_chain: [electrolytic_refining, electric_arc_furnace]
      OR:
        power_intensity: [very_high, high]
      OR:
        metals: [Al, FeSi, Si, Zn]

  - name: feedstock
    priority: 3
    applicability:
      primary_type: [smelter, refinery]

  - name: acid
    priority: 4
    applicability:
      process_chain: [sxew, hpal, heap_leach]
      OR:
        acid_dependency: [high, medium]

  # ... continue for all packs ...
```

**Exclusion rules:**
- Skip pack if `acid_dependency == 'none'` for acid pack
- Skip pack if `mine_method == 'not_applicable'` for underground/rain packs

#### Task 2.3: Deterministic Fill Algorithm - 3-4 hours
**Deliverable:** `router_v2.py` with `assemble_measurables(facility, tier) -> [json_ids]`

**Algorithm:**
```python
def assemble_measurables(facility, tier):
    target_count = TIER_TARGETS[tier]
    selected = []

    # Step 1: Always include Core pack
    core_measurables = get_pack_measurables("core", facility)
    selected.extend(core_measurables)

    # Step 2: Add conditional packs in priority order
    for pack_def in PACK_PRIORITIES:
        if len(selected) >= target_count:
            break
        if pack_def.name == "core":
            continue  # Already added
        if not check_applicability(facility, pack_def.applicability):
            continue  # Skip non-applicable packs
        if check_exclusion(facility, pack_def):
            continue  # Explicit exclusion

        pack_measurables = get_pack_measurables(pack_def.name, facility)
        pack_measurables.sort(key=lambda m: m['priority'])  # Highest priority first

        # Take up to remaining needed
        remaining = target_count - len(selected)
        selected.extend(pack_measurables[:remaining])

    # Step 3: Backfill from metal-specific packs if still short
    if len(selected) < target_count:
        metals = facility['facility_features']['metals']
        for metal in metals:
            metal_pack = f"metal_{metal.lower()}"
            if metal_pack in available_packs:
                pack_measurables = get_pack_measurables(metal_pack, facility)
                pack_measurables.sort(key=lambda m: m['priority'])
                remaining = target_count - len(selected)
                selected.extend(pack_measurables[:remaining])
            if len(selected) >= target_count:
                break

    # Step 4: Final backfill from general pool if still short
    if len(selected) < target_count:
        # Take from all remaining packs in priority order
        all_measurables = get_all_measurables_sorted()
        selected_ids = set(m['json_id'] for m in selected)
        for measurable in all_measurables:
            if measurable['json_id'] in selected_ids:
                continue
            selected.append(measurable)
            if len(selected) >= target_count:
                break

    return [m['json_id'] for m in selected[:target_count]]
```

**Tests:**
- Test facility with FCS=95 → exactly 500 measurables
- Test facility with FCS=25 → exactly 5 measurables (Core subset)
- Test smelter (feedstock pack) vs mine (no feedstock pack)
- Test SXEW mine (acid pack) vs flotation mine (no acid pack)
- Verify determinism: same facility → same list every time

---

### Phase 3: Validation & Unit Conversion (4-6 hours)

**Goal:** Strict response validation and canonical unit conversion.

#### Task 3.1: JSON Schema Validator - 2 hours
**Deliverable:** `validation/response_validator.py`

**Functions:**
- `validate_response_structure(response, json_id) -> (parsed, errors)`
- `validate_required_fields(parsed, json_id) -> errors`
- `validate_value_type(value, expected_type) -> errors`
- `validate_enum(value, allowed_values) -> errors`
- `validate_range(value, min_val, max_val) -> errors`

#### Task 3.2: Per-Metric Validators - 2-3 hours
**Deliverable:** `validation/metric_validators.py`

**Example validators:**
```python
def validate_production_output(value, unit):
    """Validate production output is positive and reasonable."""
    if not isinstance(value, (int, float)):
        return ["Production output must be numeric"]
    if value < 0:
        return ["Production output cannot be negative"]
    if value > 1e9:  # 1 billion tonnes is unreasonable
        return ["Production output exceeds plausible range"]
    return []

def validate_operational_state(value):
    """Validate status enum."""
    allowed = ["producing", "temporarily_suspended", "care_and_maintenance",
               "commissioning", "construction", "closed", "unknown"]
    if value not in allowed:
        return [f"Invalid operational state: {value}"]
    return []

def validate_trend(value):
    """Validate trend object structure."""
    if not isinstance(value, dict):
        return ["Trend must be an object"]
    required = ["run_rate_tpm", "trend", "trend_magnitude"]
    missing = [k for k in required if k not in value]
    if missing:
        return [f"Trend missing fields: {missing}"]
    return []
```

**Validator registry:** Map json_id → validation function.

#### Task 3.3: Unit Converter Integration - 1 hour
Integrate `pint` into orchestrator:
```python
import pint
ureg = pint.UnitRegistry()

def convert_value(value, raw_unit, canonical_unit):
    try:
        quantity = ureg.Quantity(value, raw_unit)
        converted = quantity.to(canonical_unit)
        return converted.magnitude, True
    except:
        return value, False  # Conversion failed, store raw
```

---

### Phase 4: Cost Worksheet & Observability (4-6 hours)

**Goal:** Reproducible cost projection and structured logging.

#### Task 4.1: Cost Calculator - 2 hours
**Deliverable:** `utils/cost_calculator.py` + `docs/COST_PROJECTION.md`

**Inputs:**
```python
PROVIDER_RATES = {
    "perplexity": {"model": "sonar-pro", "rate_per_1k_tokens": 0.005, "rpm": 60},
    "openai": {"model": "gpt-4o", "rate_per_1k_tokens": 0.015, "rpm": 500},
    "anthropic": {"model": "claude-3-5-sonnet", "rate_per_1k_tokens": 0.015, "rpm": 50}
}

MEASURED_TOKENS = {
    "avg_prompt_tokens": 450,
    "avg_response_tokens": 350,
    "total_per_query": 800
}

FACILITY_DISTRIBUTION = {
    1: 5321,  # Bronze tier
    2: 4256,  # Silver tier
    3: 1064,  # Gold tier (placeholder, needs actual FCS distribution)
    4: 0      # Platinum tier (placeholder)
}
```

**Calculations:**
```python
def calculate_monthly_cost(provider="perplexity"):
    total_queries = 0
    for tier, count in FACILITY_DISTRIBUTION.items():
        measurables = TIER_TARGETS[tier]
        cadence = TIER_CADENCE[tier]
        queries_per_month = {
            "daily": 30,
            "3x_week": 12,
            "weekly": 4,
            "monthly": 1
        }[cadence]
        total_queries += count * measurables * queries_per_month

    total_tokens = total_queries * MEASURED_TOKENS["total_per_query"]
    cost = total_tokens * PROVIDER_RATES[provider]["rate_per_1k_tokens"] / 1000

    return {
        "queries_per_month": total_queries,
        "tokens_per_month": total_tokens,
        "cost_usd_per_month": cost,
        "breakdown_by_tier": ...
    }
```

**Output:** Markdown table + CSV for spreadsheet import.

#### Task 4.2: Structured Logging - 2 hours
**Deliverable:** `utils/logger.py` with JSON line format

**Log events:**
```python
logger.info("query_started", extra={
    "facility_key": fac_key,
    "json_id": json_id,
    "run_id": run_id,
    "provider": provider,
    "model": model
})

logger.info("query_completed", extra={
    "facility_key": fac_key,
    "json_id": json_id,
    "latency_ms": latency,
    "tokens": tokens,
    "cost_usd": cost,
    "accepted": accepted,
    "confidence": confidence,
    "freshness_days": freshness
})

logger.error("query_failed", extra={
    "facility_key": fac_key,
    "json_id": json_id,
    "error_type": error_type,
    "error_message": str(e)
})
```

**Metrics aggregation:** Script to parse logs → summary stats (acceptance rate, avg latency, total cost).

#### Task 4.3: Budget Guardrails - 1-2 hours
**Deliverable:** Environment variables + kill switch

```python
MAX_COST_PER_RUN_USD = float(os.getenv("LLM_MAX_COST_PER_RUN", "100.0"))
MAX_QUERIES_PER_DAY = int(os.getenv("LLM_MAX_QUERIES_PER_DAY", "10000"))
KILL_SWITCH = os.getenv("LLM_KILL_SWITCH", "false").lower() == "true"

# In orchestrator
if KILL_SWITCH:
    raise RuntimeError("LLM queries disabled by KILL_SWITCH")

if estimated_cost > MAX_COST_PER_RUN_USD:
    raise RuntimeError(f"Estimated cost ${estimated_cost} exceeds limit ${MAX_COST_PER_RUN_USD}")
```

---

### Phase 5: Quality Harness (6-8 hours)

**Goal:** Validate accuracy against known ground truth.

#### Task 5.1: Build Golden Set - 3-4 hours
**Deliverable:** `test_data/golden_set.json` (50 facilities)

**Selection criteria:**
- 10 facilities per tier (mix Bronze → Platinum)
- Geographic diversity (5 continents)
- Facility type diversity (mines, smelters, refiners)
- Process diversity (flotation, SXEW, EAF, etc.)

**For each facility, document:**
```json
{
  "facility_key": "zaf-venetia-mine-fac",
  "ground_truth": {
    "supply.facility.status.current_operational_state": {
      "value": "producing",
      "as_of_date": "2025-10-15",
      "source": "Company Q3 2025 report",
      "url": "https://..."
    },
    "supply.facility.production.last_reported_output": {
      "value": 42500,
      "unit": "t",
      "as_of_date": "2025-09-30",
      "source": "Company Q3 2025 report"
    }
    // ... 3-5 key metrics per facility
  }
}
```

**Build process:**
1. Select 50 facilities
2. Manual research (company reports, news, regulatory filings)
3. Document 3-5 key metrics with sources
4. Peer review for accuracy

#### Task 5.2: Golden Set Test Runner - 2-3 hours
**Deliverable:** `test_golden_set.py`

**Metrics:**
- **Acceptance rate:** % of queries accepted by validation
- **Accuracy:** % of accepted values matching ground truth (±10% tolerance for numbers, exact for enums)
- **Average freshness:** Mean freshness_days across accepted results
- **Evidence coverage:** % of results with ≥1 dated source
- **Hallucination rate:** % of results with incorrect facts vs ground truth

**Output:** Markdown report + CSV with per-facility results.

**Green-light threshold:** ≥70% acceptance, ≥60% accuracy, <10% hallucination.

---

### Phase 6: Integration & Live Testing (4-6 hours)

**Goal:** Snowflake upsert logic, live queries, end-to-end validation.

#### Task 6.1: Snowflake Upsert Logic - 2 hours
**Deliverable:** `integrations/snowflake_writer.py`

```python
def upsert_results(results: List[Dict], connection):
    """
    Upsert results into MEASURABLE_RESULT table.
    Conflict resolution: (FACILITY_KEY, JSON_ID, AS_OF_DATE) unique constraint.
    On conflict: update if newer QUERY_TIMESTAMP or higher CONFIDENCE.
    """
    for result in results:
        # Check for existing
        existing = fetch_existing(result['facility_key'], result['json_id'], result['as_of_date'])
        if existing:
            # Tie-break: newer query_timestamp, then higher confidence
            if result['query_timestamp'] > existing['query_timestamp']:
                update_result(result)
            elif result['query_timestamp'] == existing['query_timestamp'] and result['confidence'] > existing['confidence']:
                update_result(result)
            # Else: skip
        else:
            insert_result(result)

    # Maintain IS_LATEST flags
    connection.execute("CALL MAINTAIN_IS_LATEST()")
```

#### Task 6.2: Live Integration Test (10 heterogeneous facilities) - 2-3 hours
**Selection:**
- 2 mines (flotation, SXEW)
- 2 smelters (Cu flash, Al electrolytic)
- 2 refiners (Zn, Ni)
- 1 integrated (mine + concentrator)
- 1 care & maintenance
- 1 construction
- 1 closed

**Run:**
- Tag features
- Route measurables
- Execute 5-10 queries per facility (sample from tier plan)
- Upsert to Snowflake
- Validate IS_LATEST flags
- Check logs, metrics, cost

**Acceptance:** ≥70% acceptance rate, no crashes, logs parseable, cost within ±20% of estimate.

---

## Green-Light Criteria Checklist

Before pilot approval:

- [ ] Library ≥300 measurables with tags + priorities
- [ ] Router produces exact 5/50/100/500 counts deterministically
- [ ] Snowflake DDL deployed, upserts work, uniqueness enforced, IS_LATEST maintained
- [ ] Live test on 10 heterogeneous facilities: ≥70% acceptance, credible evidence
- [ ] Cost worksheet shows ≤ agreed budget with measured tokens
- [ ] Logs/metrics visible and parseable
- [ ] Golden set test report: ≥70% acceptance, ≥60% accuracy, <10% hallucination
- [ ] Unit conversion works for top 10 unit types
- [ ] Validation layer catches common errors (missing fields, out-of-range, wrong enum)

---

## Estimated Effort Summary

| Phase | Tasks | Hours |
|-------|-------|-------|
| 1. Data Model & Catalog | 1.1 ✅, 1.2, 1.3 | 12-16 |
| 2. Tiering & Routing | 2.1, 2.2, 2.3 | 6-8 |
| 3. Validation | 3.1, 3.2, 3.3 | 4-6 |
| 4. Cost & Observability | 4.1, 4.2, 4.3 | 4-6 |
| 5. Quality Harness | 5.1, 5.2 | 6-8 |
| 6. Integration & Testing | 6.1, 6.2 | 4-6 |
| **Total** | | **36-50 hours** |

Add 20% buffer for unknowns → **43-60 hours total**.

---

## Next Steps (Your Decision)

**Option A: Full Rebuild (43-60 hours)**
- I execute the full plan above
- Deliverables: 300+ catalog, validation layer, cost worksheet, golden set, live tests
- Timeline: ~1.5-2 weeks full-time
- Outcome: Production-ready system meeting all green-light criteria

**Option B: Phased Delivery (prioritize critical path)**
- Phase 1 only (catalog + data model) → 12-16 hours
- Validate catalog quality with you
- Then decide whether to continue to Phases 2-6

**Option C: Minimal Viable Pilot (20-25 hours)**
- Build 100-measurable catalog (Core + Power + Feedstock + Acid)
- Implement Tier 1/2 routing only (5/50 counts)
- Run on 20 facilities (golden set subset)
- Prove concept before scaling to 300+

**Option D: Pause & Reassess**
- Current scaffolding is archived for reference
- We revisit this when GSMC has clearer requirements or higher priority needs

---

## Recommendation

I recommend **Option B (Phased Delivery)** or **Option C (Minimal Viable Pilot)**.

**Rationale:**
- Building a 300+ catalog is labor-intensive and needs your domain review to ensure quality
- A 100-measurable MVP proves the concept and gets real data sooner
- You can validate LLM quality/cost on a subset before committing to full scale
- Phased approach reduces risk of building the wrong thing

**My ask:**
1. Which option do you prefer?
2. If Option B/C: Which packs should I prioritize for the 100-measurable catalog? (Core + Power + ?)
3. Do you have any existing measurable questions documented that I should incorporate?
4. What's the actual budget constraint for pilot testing? (e.g., "$500 total" helps me size the test)

Once you choose, I'll execute with proper production discipline: tests, docs, and validated deliverables at each checkpoint.
