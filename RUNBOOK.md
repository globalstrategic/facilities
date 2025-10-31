# Canonical Naming System - Production Runbook

**Version**: 2.1.0
**Last Updated**: 2025-10-31
**Status**: Production-Ready

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Environment Setup](#environment-setup)
3. [Testing Workflow](#testing-workflow)
4. [Production Deployment](#production-deployment)
5. [Monitoring & QC](#monitoring--qc)
6. [Troubleshooting](#troubleshooting)
7. [Command Reference](#command-reference)

---

## Quick Start

```bash
# 1. Set environment variables
export OSM_CONTACT_EMAIL="your.email@company.com"
export NOMINATIM_DELAY_S="1.0"

# 2. Test on small country (Monaco - 2 facilities)
python scripts/backfill.py canonical_names --country MCO --dry-run

# 3. Run QC report
python scripts/reporting/facility_qc_report.py

# 4. If satisfied, run live
python scripts/backfill.py all --country MCO
```

---

## Environment Setup

### Required Environment Variables

```bash
# OSM Nominatim Usage Policy compliance
export OSM_CONTACT_EMAIL="you@company.com"

# Optional: Adjust API rate limit (default: 1.0 seconds)
export NOMINATIM_DELAY_S="1.0"
```

### Optional: Add to `.bashrc` or `.zshrc`

```bash
echo 'export OSM_CONTACT_EMAIL="your.email@company.com"' >> ~/.bashrc
echo 'export NOMINATIM_DELAY_S="1.0"' >> ~/.bashrc
source ~/.bashrc
```

---

## Testing Workflow

### Phase 1: Proof on Test Countries

Test on progressively larger and more diverse countries to validate edge cases.

#### 1.1 Monaco (2 facilities) - Minimal Test

```bash
# Dry-run with global dedupe
python scripts/backfill.py canonical_names --country MCO --global-dedupe --dry-run

# Check output for:
# - Canonical names generated
# - Slugs (operator-excluded)
# - Confidence scores
# - No collisions
```

**Expected Output**:
```
[1/2] Baken Diamond Mine (mco-baken-diamond-mine-fac)
  [DRY RUN] → Baken Diamond  | slug=baken-diamond  | conf=0.32

[2/2] Aurum Monaco Refinery (mco-aurum-monaco-refinery-fac)
  [DRY RUN] → Aurum Monaco Mine  | slug=aurum-monaco-mine  | conf=0.60
```

#### 1.2 Luxembourg (9 facilities) - Small Production Test

```bash
# Offline mode (cache + heuristics only)
python scripts/backfill.py towns --country LUX --offline --dry-run

# Full dry-run
python scripts/backfill.py all --country LUX --dry-run

# If satisfied, run live
python scripts/backfill.py all --country LUX
```

#### 1.3 South Africa (628 facilities) - Diverse Types

```bash
# Test towns with offline mode first (validate cache)
python scripts/backfill.py towns --country ZAF --offline --dry-run | tee logs/zaf_towns_dryrun.log

# Test canonical names with global dedupe
python scripts/backfill.py canonical_names --country ZAF --global-dedupe --dry-run | tee logs/zaf_names_dryrun.log
```

**What "Good" Looks Like**:
- Canonical lines show `slug=...` and `conf=0.6+` for most facilities
- Cache stats show growing size and rising hit rate on re-runs
- Tail of `<0.5` confidence is expected where type/town are missing

#### 1.4 Australia (613 facilities) - Remote Locations

```bash
# Many facilities in remote areas - test Nominatim fallback
python scripts/backfill.py towns --country AUS --nominatim-delay 1.2 --dry-run | tee logs/aus_towns_dryrun.log
```

#### 1.5 China (1,837 facilities) - Non-Latin Names

```bash
# Unicode handling test (Cyrillic, Chinese, Japanese)
python scripts/backfill.py canonical_names --country CHN --global-dedupe --dry-run | tee logs/chn_names_dryrun.log
```

#### 1.6 Russia - High Diversity (Cyrillic Names)

```bash
python scripts/backfill.py canonical_names --country RUS --global-dedupe --dry-run | tee logs/rus_names_dryrun.log
```

---

### Phase 2: Quality Control Reporting

#### 2.1 Baseline QC Report (Before Backfill)

```bash
# Generate baseline report
python scripts/reporting/facility_qc_report.py > reports/baseline.txt
```

**Expected Baseline Output** (before backfill):
```
======================================================================
FACILITY QC REPORT (OVERALL)
======================================================================
Total facilities:           10632

Field Coverage:
  canonical_name:               0 (  0.0%)
  canonical_slug:               0 (  0.0%)
  town:                         0 (  0.0%)
  geohash:                      0 (  0.0%)
  primary_type:                 0 (  0.0%)
  display_name:                 0 (  0.0%)
  operator_display:             0 (  0.0%)

Data Quality Flags:
  town_missing:                 0 (  0.0%)
  operator_unresolved:          0 (  0.0%)
  canonical_incomplete:         0 (  0.0%)

Type Confidence Distribution:
  none    :  10632 (100.0%)
  <0.5    :      0 (  0.0%)
  0.5–0.8 :      0 (  0.0%)
  ≥0.8    :      0 (  0.0%)
```

#### 2.2 Post-Test QC Report

```bash
# After running backfill on test countries
python scripts/reporting/facility_qc_report.py > reports/after_tests.txt

# Compare
diff reports/baseline.txt reports/after_tests.txt
```

**What to Look For**:
- Coverage increases for canonical_name, canonical_slug, geohash
- No unexpected slug collisions
- Type confidence distribution shows reasonable spread
- CSV report (`data/reports/facility_qc_*.csv`) shows per-country breakdown

---

## Production Deployment

### Recommended: Country-by-Country Rollout

Process high-priority countries first, then batch remaining countries.

#### Priority 1: High-Value Countries (Large Datasets)

```bash
# Top countries by facility count
for country in CHN USA ZAF AUS IDN IND; do
    echo "============================================================"
    echo "Processing: $country"
    echo "============================================================"
    python scripts/backfill.py all --country "$country" --nominatim-delay 1.2
    echo ""
done
```

#### Priority 2: Batch Remaining Countries

```bash
# Get list of all countries
ls facilities/ > countries.txt

# Process batch with conservative rate limiting
while read country; do
    echo "Processing: $country"
    python scripts/backfill.py all --country "$country" --nominatim-delay 1.5
done < countries.txt
```

#### Alternative: Mass Backfill (Use with Caution)

```bash
# Process all countries (will take several hours)
python scripts/backfill.py all --all --nominatim-delay 1.5

# Or with parallelization (requires GNU parallel)
ls facilities/ | parallel -j 4 --delay 2 \
    "python scripts/backfill.py all --country {} --nominatim-delay 2.0"
```

---

## Monitoring & QC

### Cache Performance Monitoring

```bash
# Cache stats are printed at end of each towns backfill
# Look for:
# - Cache size growing
# - Hit rate increasing on re-runs
# - Few/no expired entries

# Example output:
============================================================
GEOCODE CACHE STATISTICS
============================================================
Cache size: 245 entries
Cache hits: 189
Cache misses: 56
Hit rate: 77.1%
Loads: 1
Saves: 1
Pruned: 0 expired entries
Backend: parquet
TTL: 365 days
Path: data/geocode_cache.parquet
============================================================
```

### Post-Deployment QC Report

```bash
# Final QC report
python scripts/reporting/facility_qc_report.py > reports/production.txt

# Compare to baseline
diff reports/baseline.txt reports/production.txt
```

**Target Metrics** (after full backfill):
- canonical_name coverage: >95%
- canonical_slug coverage: >95%
- town coverage: >80%
- geohash coverage: >98% (auto-generated from coords)
- type_confidence ≥0.8: >70%
- Slug collisions: <10 groups

### Collision Detection

If QC report shows collisions, investigate:

```bash
# QC report will show top collisions like:
# Top slug collisions (country, slug) → count, examples:
#   (ZAF, central-mine) → 3  e.g., ['zaf-central-mine-fac', 'zaf-central-1-mine-fac', 'zaf-central-2-mine-fac']

# Collisions are automatically resolved with region/geohash/coordinate-hash suffixes
# Review collision groups to ensure proper disambiguation
```

---

## Troubleshooting

### Common Issues

#### 1. OSM Nominatim Rate Limiting

**Symptom**: 429 Too Many Requests errors

**Solution**:
```bash
# Increase delay between calls
export NOMINATIM_DELAY_S="2.0"
python scripts/backfill.py towns --country ZAF --nominatim-delay 2.0
```

#### 2. Low Type Confidence

**Symptom**: QC report shows high percentage of type_confidence <0.5

**Solution**: Update TYPE_MAP in `scripts/utils/name_canonicalizer.py`

```python
TYPE_MAP = {
    "sx-ew": "hydromet_plant",
    "your-new-type": "canonical-type",
    # Add more mappings...
}
```

#### 3. Slug Collisions

**Symptom**: QC report shows collisions in same country

**Root Cause**: Two facilities with same town-core-type combination

**Solution**: Collision resolver automatically appends region/geohash/hash suffixes

**Manual Override** (if needed):
```bash
# Use global-dedupe to ensure uniqueness across all countries
python scripts/backfill.py canonical_names --country ZAF --global-dedupe
```

#### 4. Missing Towns

**Symptom**: High `town_missing` flag count

**Solutions**:
1. **Use interactive mode**:
   ```bash
   python scripts/backfill.py towns --country ZAF --interactive
   ```

2. **Check industrial zones database** (if applicable):
   - Add zones to `scripts/utils/geocoding.py`

3. **Verify Nominatim results**:
   ```bash
   # Check cache for specific coordinates
   python -c "
   import sys; sys.path.insert(0, 'scripts')
   from utils.geocode_cache import GeocodeCache
   with GeocodeCache() as cache:
       result = cache.get(-25.7479, 28.2293)
       print(result)
   "
   ```

#### 5. Geohash Not Generated

**Symptom**: Facilities missing geohash field

**Root Cause**: Missing coordinates (lat/lon)

**Solution**: Geohash auto-generates when coordinates present. Fix missing coordinates first:
```bash
python scripts/backfill.py geocode --country ZAF
```

---

## Command Reference

### Main Commands

#### `canonical_names` - Generate Canonical Names & Slugs

```bash
# Basic usage
python scripts/backfill.py canonical_names --country ZAF --dry-run

# With global deduplication (recommended for production)
python scripts/backfill.py canonical_names --country ZAF --global-dedupe

# Custom scan root
python scripts/backfill.py canonical_names --country ZAF --global-dedupe --global-scan-root /path/to/facilities
```

**Flags**:
- `--global-dedupe`: Seed slug map with all existing slugs (ensures global uniqueness)
- `--global-scan-root`: Directory to scan for existing slugs (default: `facilities`)
- `--dry-run`: Preview changes without saving

---

#### `towns` - Backfill Town/City Names

```bash
# Basic usage
python scripts/backfill.py towns --country ZAF --dry-run

# Offline mode (cache + heuristics only, no API calls)
python scripts/backfill.py towns --country ZAF --offline

# Custom geohash precision (default: 7)
python scripts/backfill.py towns --country ZAF --geohash-precision 8

# Custom Nominatim delay (OSM policy compliance)
python scripts/backfill.py towns --country ZAF --nominatim-delay 1.5

# Interactive prompting for missing towns
python scripts/backfill.py towns --country ZAF --interactive
```

**Flags**:
- `--offline`: No Nominatim API calls (cache + heuristics only)
- `--geohash-precision INT`: Geohash precision (4-8, default: 7)
- `--nominatim-delay FLOAT`: Delay between API calls in seconds (default: 1.0 or `$NOMINATIM_DELAY_S`)
- `--interactive`: Prompt for missing towns
- `--dry-run`: Preview changes

---

#### `all` - Run All Backfill Operations

```bash
# Complete backfill (geocoding + metals + companies + towns + canonical names)
python scripts/backfill.py all --country ZAF

# With interactive mode
python scripts/backfill.py all --country ZAF --interactive

# Dry-run
python scripts/backfill.py all --country ZAF --dry-run
```

---

#### QC Report

```bash
# Generate QC report
python scripts/reporting/facility_qc_report.py

# Output: Console summary + CSV report in data/reports/
```

---

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `OSM_CONTACT_EMAIL` | Contact email for Nominatim User-Agent (required for production) | `ops@gsmc.example` |
| `NOMINATIM_DELAY_S` | Default delay between Nominatim calls (seconds) | `1.0` |

---

### File Locations

| File | Purpose |
|------|---------|
| `data/geocode_cache.parquet` | Nominatim results cache (TTL: 365 days) |
| `data/reports/facility_qc_YYYYMMDD_HHMM.csv` | Per-country QC reports |
| `logs/` | Dry-run output logs (create manually) |
| `reports/` | QC report archives (create manually) |

---

## Success Criteria

### Phase 1: Testing Complete

- [x] Dry-runs pass on MCO, LUX, ZAF, AUS, CHN, RUS
- [x] No unexpected errors or crashes
- [x] Cache hit rate >50% on re-runs
- [x] Confidence scores reasonable (median >0.6)
- [x] QC report shows expected coverage increases

### Phase 2: Production Deployment Complete

- [x] All 10,632 facilities processed
- [x] canonical_name coverage >95%
- [x] canonical_slug coverage >95%
- [x] geohash coverage >98%
- [x] <10 slug collision groups
- [x] Final QC report generated and archived

---

## Support & Maintenance

### Routine Maintenance

**Weekly**:
- Monitor `data/geocode_cache.parquet` size (prune if >50MB)
- Review QC reports for data quality trends

**Monthly**:
- Update TYPE_MAP based on new facility types encountered
- Re-run QC report and compare to previous month

**Quarterly**:
- Re-backfill towns (new OSM data)
- Refresh canonical names (operator changes)

### Cache Management

```bash
# Check cache size
ls -lh data/geocode_cache.parquet

# Clear cache (if needed)
rm data/geocode_cache.parquet

# Or prune expired entries (automatic on next run)
```

---

## Appendix: Example Workflows

### Example 1: New Country Addition

```bash
# 1. Add facilities for new country (via import)
python scripts/import_from_report.py new_country.txt --country NEW

# 2. Run full backfill
python scripts/backfill.py all --country NEW --interactive

# 3. Verify with QC report
python scripts/reporting/facility_qc_report.py | grep NEW
```

### Example 2: Fix Low-Confidence Facilities

```bash
# 1. Find low-confidence facilities
python scripts/reporting/facility_qc_report.py > report.txt
grep "type_conf_<0.5" report.txt

# 2. Update TYPE_MAP in name_canonicalizer.py

# 3. Re-run canonical names
python scripts/backfill.py canonical_names --country ZAF --global-dedupe

# 4. Verify improvement
python scripts/reporting/facility_qc_report.py | grep ZAF
```

### Example 3: Bulk Refresh After Schema Change

```bash
# Re-backfill everything (use with caution!)
python scripts/backfill.py all --all --nominatim-delay 1.5 --global-dedupe
```

---

**End of Runbook**
