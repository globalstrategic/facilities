# Phase 2 Alternate Path: Direct-EntityIdentity + External Relationships

**Date**: 2025-10-20
**Status**: Implemented
**Decision**: Use Direct-EntityIdentity integration with external relationships file instead of embedded links in facility JSONs

## Executive Summary

We have chosen to implement Phase 2 company resolution using an **external relationships file** (parquet/CSV) as the source of truth for facility-company links, rather than embedding `owner_links` and `operator_link` directly in facility JSON files.

This decision enables:
- ✅ Batch reprocessing without modifying thousands of facility JSON files
- ✅ Quality gate tuning without data migration
- ✅ Multiple relationship sources/versions
- ✅ Efficient querying via pandas/SQL
- ✅ Separation of concerns (facility data vs relationships)

## Architecture

### Where Data Lives

1. **Facility JSONs** (`facilities/{ISO3}/*.json`):
   - `company_mentions[]`: Raw company mentions from sources (Phase 1 extraction)
   - NO `owner_links[]` or `operator_link` (deprecated, removed via pre-commit hook)

2. **Relationships File** (`tables/facilities/facility_company_relationships.csv`):
   - **Canonical source of truth** for all facility-company relationships
   - Parquet files are transient job outputs, merged into CSV after each enrichment run
   - Includes legacy relationships (migrated from old owner_links/operator_link)
   - Includes new relationships (resolved via CompanyResolver from company_mentions)

3. **Quality Gates** (`config/gates.json`):
   - Thresholds for auto_accept (≥0.90), review (0.75-0.89), pending (<0.75)
   - Boost values for registry matches, dual sources, parent matches

### Data Flow

```
Facility JSON
  ↓
company_mentions[] (Phase 1: Extraction)
  ↓
CompanyResolver.resolve_mentions() (Phase 2: Resolution)
  ↓
Quality Gates (auto_accept / review / pending)
  ↓
Write to Parquet (transient job output)
  ↓
Merge to facility_company_relationships.csv (canonical storage)
```

### Relationships Schema

**Columns**:
- `relationship_id`: UUID
- `facility_id`: Links to facility JSON
- `company_id`: Canonical company ID (e.g., `cmp-lei-378900F238434B74D281`)
- `company_name`: Resolved company name
- `role`: operator, owner, majority_owner, minority_owner
- `confidence`: 0.0-1.0 score
- `base_confidence`: Pre-gate confidence score
- `match_method`: resolver, auto+gleif, legacy, manual
- `provenance`: Source of mention (e.g., mines_csv, gemini_research)
- `evidence`: Supporting text/context
- `gate`: auto_accept, review, pending
- `gates_applied`: JSON with penalties applied
- `created_at`: Timestamp
- `created_by`: automation, manual, migration
- Additional fields: percentage, valid_from, valid_to, lei, etc.

## Why Not Embedded Links?

### Rejected Approach: Embedded owner_links/operator_link in Facility JSONs

**Problems**:
1. **Brittleness**: Modifying thousands of JSON files for each resolution pass is risky
2. **Version control noise**: Massive diffs for minor confidence adjustments
3. **Inflexibility**: Can't easily maintain multiple relationship versions
4. **Reprocessing cost**: Must read/write all files to tune quality gates
5. **Audit trail**: Difficult to track relationship provenance and changes
6. **Schema coupling**: Facility schema becomes complex with relationship details

### Chosen Approach: External Relationships File

**Benefits**:
1. **Separation of concerns**: Facility data vs relationships are independent
2. **Batch operations**: Query/filter relationships without touching facility files
3. **Gate tuning**: Adjust thresholds and rerun resolver without migration
4. **Multiple sources**: Combine legacy, auto-resolved, and manual relationships
5. **Audit trail**: Full history in single file with provenance tracking
6. **Query performance**: Pandas/SQL queries faster than JSON traversal
7. **Flexibility**: Easy to export/import, version, and compare

## Quality Gates

Configured in `config/gates.json`:

```json
{
  "auto_accept_threshold": 0.90,    // Auto-write relationships
  "review_min_threshold": 0.75,      // Manual review queue
  "prefer_registry_boost": 0.05,     // LEI/Wikidata match bonus
  "dual_source_boost": 0.03,         // Multiple source confirmation
  "parent_match_boost": 0.02         // Parent company match
}
```

**Three Buckets**:
1. **auto_accept** (confidence ≥ 0.90): High-quality, write immediately
2. **review** (0.75-0.89): Medium confidence, needs human validation
3. **pending** (< 0.75): No match or low confidence, track for research

## Guardrails

### Pre-commit Hook

Blocks commits containing `owner_links` or `operator_link` in facility JSONs:

```bash
# .git/hooks/pre-commit
if git diff --cached --name-only | grep -q 'facilities/.*\.json'; then
  if git diff --cached | grep -q '"owner_links"\|"operator_link"'; then
    echo "ERROR: Facility JSON contains deprecated owner_links/operator_link"
    echo "Use tables/facilities/facility_company_relationships.parquet instead"
    exit 1
  fi
fi
```

### Schema Validation

Facility schema does NOT include `owner_links` or `operator_link` definitions (removed Oct 15, 2025).

## Review Workflow

For relationships in the **review** bucket:

1. **Export review pack**:
   ```bash
   # Filter relationships with gate='review'
   python -c "import pandas as pd; df=pd.read_parquet('tables/facilities/facility_company_relationships.parquet'); df[df['gate']=='review'].to_csv('review_pack_2025-10-20.csv', index=False)"
   ```

2. **Manual review**:
   - Check each relationship: company name match, context evidence
   - Approve: Add to accepted list
   - Reject: Add to rejected list
   - Research: Add to pending companies tracker

3. **Batch update**:
   - Approved: Update gate to 'auto_accept', boost confidence
   - Rejected: Remove from relationships file
   - Research: Track in PendingCompanyTracker for later resolution

## File Locations

### Primary Files (tracked in git)
- **Relationships (canonical)**: `tables/facilities/facility_company_relationships.csv`
- **Gates config**: `config/gates.json`

### Generated Files (gitignored)
- **Job outputs (transient)**: `tables/facilities/facility_company_relationships.parquet`
- **Accepted subset**: `tables/facilities/relationships.accepted.csv`
- **Review packs**: `review_pack_YYYY-MM-DD.csv` (generated at repo root)
- **Pending tracking**: Handled by EntityIdentity PendingCompanyTracker

## Migration Status

### Legacy Relationships (Pre-Oct 15)
- Facilities with embedded `owner_links[]` / `operator_link` were migrated
- **Migrated** to `facility_company_relationships.csv` (Oct 15)
- All marked with `provenance: legacy_owner_links/legacy_operator_link`
- Confidence: 1.0 (assumed accurate, manually entered)

### New Relationships (Post-Oct 15)
- **Wave-1 Countries**: BRA, IND, RUS, CHL, PER (1,341 facilities total)
- **Resolution complete** (as of Oct 20):
  - All countries processed with strict gates (≥0.90 threshold)
  - 3 auto-accepted relationships from Wave-1 enrichment
  - 37 review items logged (0.75-0.89 confidence range)
  - See `docs/implementation_history/COVERAGE_2025-10-20.md` for detailed stats

### Strict Gates Impact
- With `auto_accept_threshold: 0.90`, very few auto-accept (high precision)
- Most fall into review or pending buckets (requires manual validation)
- This is **intentional** to maintain data quality
- **Confidence distribution** from Wave-1:
  - 0.85-0.90: 8 items (21.6% of review bucket) — near-misses
  - 0.80-0.85: 19 items (51.4%) — moderate confidence
  - 0.75-0.80: 10 items (27.0%) — lower confidence

## Success Metrics

### Coverage (auto-generated)
To regenerate current stats:
```bash
# Total facilities by country
find facilities -name '*.json' | wc -l
for c in BRA IND RUS CHL PER; do
  echo "$c: $(find facilities/$c -name '*.json' | wc -l)"
done

# Accepted relationships
python -c "import csv; print('Accepted:', sum(1 for _ in csv.DictReader(open('tables/facilities/relationships.accepted.csv'))))"

# Coverage by country
python - <<'PY'
import csv, glob
from collections import defaultdict
countries = ["BRA","IND","RUS","CHL","PER"]
fac_counts = {c: len(glob.glob(f"facilities/{c}/*.json")) for c in countries}
covered = defaultdict(set)
with open("tables/facilities/relationships.accepted.csv") as f:
    for row in csv.DictReader(f):
        cc = row["facility_id"].split("-")[0].upper()
        if cc in countries: covered[cc].add(row["facility_id"])
for c in countries:
    total, cov = fac_counts.get(c,0), len(covered[c])
    print(f"{c}: {cov}/{total} ({100*cov/total if total else 0:.1f}%)")
PY
```

**Current snapshot** (2025-10-20):
- Wave-1: 3/1,341 facilities with auto-accepted relationships (0.2%)
- Target: Validate 37 review items to increase coverage

### Quality
- **Auto-accept threshold**: 0.90 (strict, high precision)
- **Review queue**: 37 items (0.75-0.89 confidence range)
- **Pending tracking**: All unresolved companies logged in enrichment logs

### Performance
- **Batch resolution**: Multiple facilities/second (depends on EntityIdentity database size)
- **Reprocessing**: Can re-run full Wave-1 resolution in minutes
- **Query time**: Fast CSV/pandas operations for relationship lookups

## Comparison to Original Plan

### Original (Embedded Links)
- Store `owner_links[]` and `operator_link` in facility JSON
- Modify facilities during resolution
- Higher risk, lower flexibility

### Implemented (External Relationships)
- Store relationships in separate parquet/CSV
- Facility JSONs remain clean with only `company_mentions[]`
- Lower risk, higher flexibility, better audit trail

## Next Steps

1. ✅ **Complete Wave-1 resolution** (All 5 countries processed: BRA, IND, RUS, CHL, PER)
2. **Process review queue** (37 items with 0.75-0.89 confidence need validation)
3. **Add company aliases** (optional: create `config/company_aliases.json` for near-misses)
4. **Wave-2 expansion** (next 5 countries based on facility count)
5. **Automated review tools** (build UI for batch review)
6. **Integration with downstream systems** (query relationships for analytics)

## References

- **Workflow guide**: `docs/guides/RESOLUTION_WORKFLOW.md`
- **Coverage snapshot**: `docs/implementation_history/COVERAGE_2025-10-20.md`
- **Enrichment script**: `scripts/enrich_companies.py`
- **Enrichment wrapper**: `scripts/run_enrichment.sh` (recommended, sets PYTHONPATH)
- **Resolver implementation**: `scripts/utils/company_resolver.py`
- **Quality gates config**: `config/gates.json`

---

**Approved by**: Automated decision (Oct 20, 2025)
**Implementation**: Complete
**Status**: Production-ready with strict quality gates
