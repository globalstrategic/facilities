# Company Resolution Workflow

**Version**: 2.0 (External Relationships Model)
**Last Updated**: 2025-10-20

## Overview

This guide documents the end-to-end workflow for resolving company names to canonical IDs using the Phase 2 Direct-EntityIdentity approach with external relationships storage.

## Workflow Steps

### 1. Backfill Mentions

**Goal**: Extract company mentions from facility data into `company_mentions[]` arrays.

**Command**:
```bash
# Single country
python scripts/backfill_mentions.py --country IND

# Dry-run (preview without saving)
python scripts/backfill_mentions.py --country IND --dry-run

# All countries
python scripts/backfill_mentions.py --all
```

**What it does**:
- Reads facility JSONs
- Extracts company names from source data (Mines.csv, research reports)
- Writes to `company_mentions[]` array with metadata:
  - `name`: Company name string
  - `role`: operator, owner, unknown
  - `source`: Origin of mention (e.g., mines_csv_row_2535)
  - `confidence`: 0.5 (default for extracted mentions)
  - `first_seen`: Timestamp
  - `evidence`: Context text

**Output**:
- Modified facility JSONs with `company_mentions[]` populated
- No relationships created yet (Phase 1 only)

### 2. Resolve Mentions

**Goal**: Match company mentions to canonical company IDs using EntityIdentity.

**Command**:
```bash
# RECOMMENDED: Use wrapper (sets PYTHONPATH automatically)
scripts/run_enrichment.sh --country IND

# Or run directly (legacy, requires manual PYTHONPATH)
python scripts/enrich_companies.py --country IND

# Dry-run (preview without saving)
scripts/run_enrichment.sh --dry-run --country IND

# Custom confidence threshold
scripts/run_enrichment.sh --country IND --min-confidence 0.75
```

**What it does**:
- Reads `company_mentions[]` from facility JSONs
- Uses `CompanyResolver` with quality gates (strict profile)
- Matches against EntityIdentity company registry (3,687 companies)
- Applies quality gates based on `config/gates.json`
- Writes relationships to `tables/facilities/facility_company_relationships.parquet`

**Quality Gates** (`config/gates.json`):
```json
{
  "auto_accept_threshold": 0.90,    // Auto-accept: write immediately
  "review_min_threshold": 0.75,      // Review queue: needs validation
  "prefer_registry_boost": 0.05,     // LEI/Wikidata match bonus
  "dual_source_boost": 0.03,         // Multiple sources agree
  "parent_match_boost": 0.02         // Parent company fuzzy match
}
```

**Three Buckets**:
1. **auto_accept** (≥0.90): High confidence, written to relationships file
2. **review** (0.75-0.89): Medium confidence, needs human review
3. **pending** (<0.75): No match or low confidence, tracked separately

**Output**:
- `tables/facilities/facility_company_relationships.parquet` (updated)
- `tables/facilities/facility_company_relationships.csv` (for inspection)
- Pending companies tracked in EntityIdentity PendingCompanyTracker

### 3. Apply Gates & Generate Buckets

**Goal**: Separate relationships into auto-accept, review, and pending categories.

**Commands**:
```bash
# Extract accepted relationships (confidence ≥ 0.90)
python -c "import pandas as pd; df=pd.read_parquet('tables/facilities/facility_company_relationships.parquet'); df[df['gate']=='auto_accept'].to_csv('tables/facilities/relationships.accepted.csv', index=False)"

# Extract review queue (0.75-0.89)
python -c "import pandas as pd; df=pd.read_parquet('tables/facilities/facility_company_relationships.parquet'); df[df['gate']=='review'].to_csv('review_pack_$(date +%Y-%m-%d).csv', index=False)"

# Count pending (tracked separately)
python -c "import pandas as pd; df=pd.read_parquet('tables/facilities/facility_company_relationships.parquet'); print(f\"Pending: {(df['gate']=='pending').sum()}\")"
```

**Acceptance Checks**:
```bash
# File exists & non-empty
wc -l tables/facilities/facility_company_relationships.csv

# Unique facilities covered
cut -d, -f2 tables/facilities/facility_company_relationships.csv | tail -n +2 | sort -u | wc -l

# Confidence distribution
awk -F, 'NR>1{c=$8; bucket=int(c*10)/10; dist[bucket]++} END{for (k in dist) print k"-"k+0.09": "dist[k]}' tables/facilities/facility_company_relationships.csv | sort -n
```

### 4. Review Queue Processing

**Goal**: Manually validate borderline matches (review bucket).

**Export review pack**:
```bash
# Generate CSV for review (includes top candidate fields)
python -c "
import pandas as pd
df = pd.read_parquet('tables/facilities/facility_company_relationships.parquet')
review_df = df[df['gate'] == 'review'][['facility_id', 'company_id', 'company_name', 'role', 'confidence', 'evidence', 'match_method']]
review_df.to_csv('review_pack_2025-10-20.csv', index=False)
print(f'Exported {len(review_df)} relationships for review')
"
```

**Manual review process**:
1. Open `review_pack_YYYY-MM-DD.csv` in spreadsheet
2. For each relationship:
   - Check company name vs evidence text
   - Verify role (operator/owner) makes sense
   - Look up facility if needed
3. Mark decision:
   - **Approve**: Company match is correct
   - **Reject**: Company match is wrong
   - **Research**: Need more investigation

**Batch update** (after review):
```python
import pandas as pd

# Load relationships
df = pd.read_parquet('tables/facilities/facility_company_relationships.parquet')

# Load review decisions (CSV with approved/rejected IDs)
decisions = pd.read_csv('review_decisions.csv')

# Approve: Update gate to auto_accept
approved_ids = decisions[decisions['decision'] == 'approve']['relationship_id']
df.loc[df['relationship_id'].isin(approved_ids), 'gate'] = 'auto_accept'
df.loc[df['relationship_id'].isin(approved_ids), 'confidence'] += 0.05  # Boost

# Reject: Remove relationships
rejected_ids = decisions[decisions['decision'] == 'reject']['relationship_id']
df = df[~df['relationship_id'].isin(rejected_ids)]

# Save updated relationships
df.to_parquet('tables/facilities/facility_company_relationships.parquet', index=False)
```

### 5. Coverage & Metrics

**Goal**: Track progress and quality metrics.

**Coverage by country**:
```bash
for c in BRA IND RUS CHL PER; do
  total=$(ls facilities/$c/*.json | wc -l)
  covered=$(awk -F, 'NR>1 && $8>=0.90 && $2 ~ /^'"$c"'-/ {print $2}' tables/facilities/facility_company_relationships.csv | sort -u | wc -l)
  printf "%s  total=%d  covered=%d  coverage=%.1f%%\n" "$c" "$total" "$covered" $(echo "100*$covered/$total" | bc -l)
done
```

**Quality metrics**:
```bash
# Total relationships by gate
awk -F, 'NR>1 {gates[$NF]++} END {for (g in gates) print g": "gates[g]}' tables/facilities/facility_company_relationships.csv

# Average confidence by gate
awk -F, 'NR>1 {sum[$NF]+=$8; count[$NF]++} END {for (g in sum) printf "%s: avg_conf=%.3f (n=%d)\n", g, sum[g]/count[g], count[g]}' tables/facilities/facility_company_relationships.csv
```

**Save snapshot**:
```bash
cat > docs/implementation_history/COVERAGE_$(date +%Y-%m-%d).md <<EOF
# Coverage Snapshot $(date +%Y-%m-%d)

## Wave-1 Countries (BRA, IND, RUS, CHL, PER)

$(for c in BRA IND RUS CHL PER; do
  total=$(ls facilities/$c/*.json | wc -l)
  covered=$(awk -F, 'NR>1 && $8>=0.90 && $2 ~ /^'"$c"'-/ {print $2}' tables/facilities/facility_company_relationships.csv | sort -u | wc -l)
  printf "- **%s**: %d total, %d covered (%.1f%%)\n" "$c" "$total" "$covered" $(echo "100*$covered/$total" | bc -l)
done)

## Relationship Quality

$(awk -F, 'NR>1 {gates[$NF]++} END {for (g in gates) printf "- **%s**: %d relationships\n", g, gates[g]}' tables/facilities/facility_company_relationships.csv)

## Pending Queue

- Tracked in EntityIdentity PendingCompanyTracker
- See enrichment logs for pending company names
EOF
```

## Quick Reference

### File Locations

| File | Purpose | Format |
|------|---------|--------|
| `facilities/{ISO3}/*.json` | Facility data with `company_mentions[]` | JSON |
| `tables/facilities/facility_company_relationships.parquet` | Source of truth for relationships | Parquet |
| `tables/facilities/facility_company_relationships.csv` | Same as parquet, for inspection | CSV |
| `tables/facilities/relationships.accepted.csv` | Auto-accepted relationships only | CSV |
| `review_pack_YYYY-MM-DD.csv` | Relationships needing review | CSV |
| `config/gates.json` | Quality gate thresholds | JSON |

### Key Commands

```bash
# 1. Backfill mentions
python scripts/backfill_mentions.py --country IND

# 2. Resolve mentions (use wrapper for automatic PYTHONPATH)
scripts/run_enrichment.sh --country IND

# 3. Generate review pack
python -c "import pandas as pd; df=pd.read_parquet('tables/facilities/facility_company_relationships.parquet'); df[df['gate']=='review'].to_csv('review_pack_$(date +%Y-%m-%d).csv', index=False)"

# 4. Check coverage
for c in BRA IND RUS CHL PER; do
  total=$(ls facilities/$c/*.json | wc -l)
  covered=$(awk -F, 'NR>1 && $8>=0.90 && $2 ~ /^'"$c"'-/ {print $2}' tables/facilities/facility_company_relationships.csv | sort -u | wc -l)
  echo "$c: $covered/$total"
done
```

### Confidence Levels

| Range | Gate | Meaning | Action |
|-------|------|---------|--------|
| ≥0.90 | auto_accept | Very high confidence | Auto-write |
| 0.75-0.89 | review | Medium confidence | Manual review |
| <0.75 | pending | Low confidence / no match | Research |

### Guardrails

1. **Pre-commit hook**: Blocks `owner_links` / `operator_link` in facility JSONs
2. **Schema validation**: Facility schema excludes deprecated relationship fields
3. **Quality gates**: Strict thresholds prevent low-quality auto-accepts
4. **Audit trail**: Full provenance tracking in relationships file

## Troubleshooting

### Issue: No relationships created

**Symptom**: Enrichment runs but relationships file is empty or unchanged.

**Cause**: Strict gates (0.90 threshold) reject most matches.

**Solution**:
- Lower threshold temporarily: `--min-confidence 0.75`
- Process review queue manually
- Check pending companies for patterns

### Issue: Too many pending

**Symptom**: Most mentions fall into pending bucket.

**Cause**: Company names not in EntityIdentity registry (3,687 companies).

**Solution**:
- Review pending company names
- Add missing companies to EntityIdentity registry
- Use PendingCompanyTracker to prioritize research

### Issue: Review queue too large

**Symptom**: Hundreds of relationships need manual review.

**Cause**: Many matches in 0.75-0.89 confidence range.

**Solution**:
- Batch review by country or industry
- Build review UI for efficiency
- Adjust gate thresholds after sampling

## Next Steps

1. **Complete Wave-1** (5 countries, 1,341 facilities)
2. **Process review queue** (validate borderline matches)
3. **Tune gates** (adjust thresholds based on accuracy)
4. **Wave-2 expansion** (next 5 countries by facility count)
5. **Automated tools** (build review UI, bulk operations)

## References

- **Architecture decision**: `docs/implementation_history/PHASE_2_ALTERNATE_PATH.md`
- **Enrichment script**: `scripts/enrich_companies.py`
- **Resolver implementation**: `scripts/utils/company_resolver.py`
- **Quality gates config**: `config/gates.json`
