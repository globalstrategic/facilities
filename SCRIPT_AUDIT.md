# Scripts Audit & Duplication Analysis

**Date**: 2025-10-20
**Status**: Analysis Complete

## Executive Summary

The `scripts/` directory contains **11 Python scripts** with some functionality duplication and documentation inconsistencies. Several scripts reference non-existent dependencies, and the documentation mentions scripts that don't exist.

### Key Findings

1. **Missing Scripts**: Documentation references scripts that don't exist
2. **Duplicate Functionality**: Import and enrichment logic is split across multiple scripts
3. **Pipeline Inconsistencies**: `pipeline_ingest.py` calls non-existent scripts
4. **Migration Scripts**: Legacy migration scripts may be obsolete
5. **Documentation Drift**: CLAUDE.md and README don't match actual implementation

---

## Scripts Inventory

### Core Production Scripts (5)

#### 1. **import_from_report.py** (1,393 lines)
**Purpose**: Main facility import pipeline with Phase 1 extraction
**Status**: ✅ Production-ready

**Key Features:**
- Parse CSV, markdown tables, and text formats
- Extract company mentions (Phase 1) - NO resolution
- Metal normalization with chemical formulas (via `metal_identifier`)
- Per-row country detection (for multi-country CSVs like Mines.csv)
- Duplicate detection (name + location matching)
- Writes facility JSONs with `company_mentions[]` array

**Usage:**
```bash
# Standard import
python scripts/import_from_report.py report.txt --country DZA

# Auto-detect country from filename
python scripts/import_from_report.py albania.txt

# Multi-country CSV
python scripts/import_from_report.py gt/Mines.csv --source "Mines.csv"
```

**Dependencies:**
- `entityidentity.metal_identifier` (optional, fallback available)
- `utils.country_utils`

**Output:** Facility JSONs with `company_mentions` ready for Phase 2

---

#### 2. **enrich_companies.py** (470 lines)
**Purpose**: Phase 2 company resolution and relationship creation
**Status**: ✅ Production-ready (actively used)

**Key Features:**
- Batch company resolution using `CompanyResolver`
- Quality gates (auto_accept / review / pending)
- Writes relationships to `facility_company_relationships.parquet`
- Does NOT modify facility JSONs (Phase 2 design)

**Usage:**
```bash
# Enrich all facilities
python scripts/enrich_companies.py

# Enrich specific country
python scripts/enrich_companies.py --country IND

# Preview without saving
python scripts/enrich_companies.py --dry-run
```

**Dependencies:**
- `utils.company_resolver.CompanyResolver`
- `utils.id_utils.to_canonical`
- `entityidentity.companies.PendingCompanyTracker` (optional)

**Output:** `tables/facilities/facility_company_relationships.parquet`

**Current Activity:** Running for IND, RUS, BRA, CHL, PER (background processes)

---

#### 3. **deep_research_integration.py** (606 lines)
**Purpose**: Gemini Deep Research integration for facility enrichment
**Status**: ✅ Production-ready

**Key Features:**
- Generate research prompts from facility data
- Process Gemini research outputs (JSON/JSONL)
- Resolve companies using `CompanyResolver`
- Merge status, owners, operators, products into facility JSONs
- Update verification metadata

**Usage:**
```bash
# Generate research prompt
python scripts/deep_research_integration.py \
    --generate-prompt --country ZAF --metal platinum --limit 50

# Process research output
python scripts/deep_research_integration.py \
    --process research_output.json --country ZAF --metal platinum
```

**Dependencies:**
- `scripts.utils.company_resolver.CompanyResolver`

**Output:** Updated facility JSONs + raw research backups

---

#### 4. **facilities.py** (456 lines)
**Purpose**: Unified CLI wrapper for common operations
**Status**: ✅ Production-ready

**Subcommands:**
1. `import` - Wraps `import_from_report.py`
2. `research` - Wraps `deep_research_integration.py`
3. `test` - Runs test suites
4. `sync` - Parquet export/import (via `FacilitySyncManager`)
5. `resolve` - Test entity resolution (country/metal/company)

**Usage:**
```bash
# Import facilities
python scripts/facilities.py import report.txt --country DZ

# Test resolution
python scripts/facilities.py resolve company "BHP"

# Export to parquet
python scripts/facilities.py sync --export
```

**Note:** Limited compared to calling scripts directly. For advanced features, use scripts directly.

---

#### 5. **audit_facilities.py** (312 lines)
**Purpose**: Data quality checks and reporting
**Status**: ✅ Production-ready

**Usage:**
```bash
python scripts/audit_facilities.py
```

---

### Pipeline Script (1)

#### 6. **pipeline_ingest.py** (177 lines)
**Purpose**: Unified ingest pipeline chaining multiple scripts
**Status**: ⚠️ **BROKEN** - Calls non-existent scripts

**Intended Flow:**
```
TXT → Parse → Normalize → Resolve → Review Pack → Metrics
```

**Problems:**
1. Calls `scripts/normalize_mentions.py` - **DOES NOT EXIST**
2. Calls `scripts/export_review_pack.py` - **DOES NOT EXIST**
3. Calls `migration/wave_metrics.py` - **DOES NOT EXIST**

**Recommendation:**
- Fix missing scripts OR
- Remove this script and document manual pipeline workflow OR
- Rewrite to use existing scripts only

---

### Utility/Maintenance Scripts (5)

#### 7. **backfill_mentions.py** (440 lines)
**Purpose**: Extract company mentions from existing facilities
**Status**: ✅ Utility script

Useful for migrating old facilities without `company_mentions[]` to Phase 2 format.

---

#### 8. **full_migration.py** (381 lines)
**Purpose**: Legacy CSV → JSON migration
**Status**: ⚠️ Possibly obsolete

**Question:** Is initial migration complete? If yes, can this be archived?

---

#### 9. **migrate_legacy_fields.py** (235 lines)
**Purpose**: Migrate old field names to new schema
**Status**: ⚠️ Possibly obsolete

**Question:** Is schema migration complete? If yes, can this be archived?

---

#### 10. **verify_backfill.py** (185 lines)
**Purpose**: Verify backfill results
**Status**: ✅ Utility script

---

#### 11. **__init__.py** (0 lines)
**Purpose**: Make scripts/ a Python package
**Status**: ✅ Standard

---

## Duplication Analysis

### 1. Company Resolution Logic

**Duplicated across:**
- `import_from_report.py` - Phase 1 extraction only (✅ correct)
- `enrich_companies.py` - Phase 2 resolution (✅ correct)
- `deep_research_integration.py` - Uses `CompanyResolver` for research enrichment

**Analysis:**
- ✅ **Not true duplication** - different phases of pipeline
- `import_from_report.py` extracts mentions, `enrich_companies.py` resolves them
- `deep_research_integration.py` uses `CompanyResolver` for research context

**Recommendation:** No changes needed, this is by design.

---

### 2. Facility Loading/Saving

**Duplicated across:**
- `import_from_report.py` - Writes new facilities
- `enrich_companies.py` - Reads facilities (doesn't write, only parquet)
- `deep_research_integration.py` - Loads and saves facilities
- `backfill_mentions.py` - Loads and saves facilities

**Analysis:**
- ⚠️ **Moderate duplication** - basic file I/O logic repeated
- Could be extracted to `utils/facility_io.py`

**Recommendation:**
- Low priority - not causing issues
- Could refactor if adding more scripts

---

### 3. Duplicate Detection

**Location:** Only in `import_from_report.py` (check_duplicate function)

**Analysis:**
- ✅ **No duplication** - only one place
- Enhanced matcher available via `utils/facility_matcher.py` (not used in scripts yet)

---

### 4. Country Normalization

**Locations:**
- `import_from_report.py` - Uses `utils.country_utils`
- `facilities.py` - Uses `entityidentity.country_identifier`

**Analysis:**
- ✅ **No duplication** - both use centralized utilities

---

## Documentation Issues

### CLAUDE.md Issues

#### Issue 1: Non-existent Scripts Referenced

**CLAUDE.md says:**
```bash
# Enhanced import with entity resolution
python scripts/import_from_report_enhanced.py report.txt --country DZ --enhanced
```

**Reality:** `import_from_report_enhanced.py` **DOES NOT EXIST**

**Actual Usage:**
```bash
# Enhanced mode is BUILT-IN to import_from_report.py
python scripts/import_from_report.py report.txt --country DZ
# (enhanced mode is automatic via metal_identifier)
```

**Fix:** Remove all references to `import_from_report_enhanced.py`

---

#### Issue 2: Vague References

**CLAUDE.md says:**
> "pipeline_ingest, run_enrichment, whatever"

**Reality:**
- `pipeline_ingest.py` exists but is broken
- `run_enrichment` or `run_enrichment.py` **DOES NOT EXIST**

**Fix:** Document actual scripts only

---

#### Issue 3: Outdated Workflow

**CLAUDE.md shows:**
```bash
# With custom source
python scripts/import_from_report_enhanced.py report.txt --country AFG --enhanced --source "Afghanistan Minerals Report 2025"
```

**Should be:**
```bash
# Actual import (enhanced by default)
python scripts/import_from_report.py report.txt --country AFG --source "Afghanistan Minerals Report 2025"
```

---

### README_FACILITIES.md Issues

#### Issue 1: Same as CLAUDE.md

References non-existent `import_from_report_enhanced.py` throughout.

---

#### Issue 2: Workflow Confusion

Shows both:
- Direct script calls: `python scripts/import_from_report_enhanced.py`
- CLI calls: `python scripts/facilities.py import`

**Recommendation:**
- Clarify when to use CLI vs direct scripts
- CLI = simple operations
- Direct scripts = advanced features

---

## Recommendations

### Priority 1: Fix Documentation

1. **Remove all references to:**
   - `import_from_report_enhanced.py` (doesn't exist)
   - `run_enrichment.py` (doesn't exist)

2. **Update import examples to:**
   ```bash
   # Standard import (with automatic entity resolution)
   python scripts/import_from_report.py report.txt --country DZA
   ```

3. **Add Phase 2 workflow:**
   ```bash
   # Phase 1: Extract facilities + mentions
   python scripts/import_from_report.py report.txt --country DZA

   # Phase 2: Resolve companies
   python scripts/enrich_companies.py --country DZA
   ```

---

### Priority 2: Fix or Remove pipeline_ingest.py

**Option A: Fix It**

Create missing scripts:
- `scripts/normalize_mentions.py`
- `scripts/export_review_pack.py`
- `migration/wave_metrics.py`

**Option B: Remove It**

Delete `pipeline_ingest.py` and document manual pipeline in CLAUDE.md:

```bash
# Manual Pipeline
# 1. Import facilities
python scripts/import_from_report.py report.txt --country DZA

# 2. Enrich with companies
python scripts/enrich_companies.py --country DZA

# 3. (Optional) Deep research enrichment
python scripts/deep_research_integration.py --process output.json --country DZA

# 4. Audit
python scripts/audit_facilities.py
```

**Recommendation:** Option B (remove) - simpler and more transparent

---

### Priority 3: Archive Legacy Scripts

Move to `scripts/legacy/`:
- `full_migration.py` (if initial migration complete)
- `migrate_legacy_fields.py` (if schema migration complete)

Keep:
- `backfill_mentions.py` (still useful for old facilities)
- `verify_backfill.py` (still useful)

---

### Priority 4: Update CLAUDE.md Script Reference

Replace current "Common Development Commands" section with:

```markdown
## Core Scripts

### 1. Import Facilities

**Script:** `scripts/import_from_report.py`

```bash
# Import from text/CSV/markdown
python scripts/import_from_report.py report.txt --country DZA

# Auto-detect country from filename
python scripts/import_from_report.py algeria_report.txt

# Multi-country CSV (per-row detection)
python scripts/import_from_report.py Mines.csv --source "Mines Database"

# From stdin
cat report.txt | python scripts/import_from_report.py --country DZA
```

**Features:**
- Automatic entity resolution (metals → chemical formulas)
- Company mention extraction (Phase 1)
- Duplicate detection
- Per-row country support

---

### 2. Enrich with Companies (Phase 2)

**Script:** `scripts/enrich_companies.py`

```bash
# Enrich all facilities
python scripts/enrich_companies.py

# Enrich specific country
python scripts/enrich_companies.py --country IND

# Preview only
python scripts/enrich_companies.py --dry-run
```

**Features:**
- Batch company resolution
- Quality gates (auto_accept / review / pending)
- Writes to parquet (not facility JSONs)

---

### 3. Deep Research Integration

**Script:** `scripts/deep_research_integration.py`

```bash
# Generate prompt
python scripts/deep_research_integration.py \
    --generate-prompt --country ZAF --metal platinum

# Process research output
python scripts/deep_research_integration.py \
    --process output.json --country ZAF
```

---

### 4. Unified CLI

**Script:** `scripts/facilities.py`

```bash
# Import (basic wrapper)
python scripts/facilities.py import report.txt --country DZ

# Test entity resolution
python scripts/facilities.py resolve country "Algeria"
python scripts/facilities.py resolve metal "Cu"
python scripts/facilities.py resolve company "BHP"

# Export/import parquet
python scripts/facilities.py sync --export
python scripts/facilities.py sync --status
```

**Note:** For advanced features, use scripts directly.
```

---

## Summary

### Scripts to Keep (6)
1. ✅ `import_from_report.py` - Core import
2. ✅ `enrich_companies.py` - Company resolution
3. ✅ `deep_research_integration.py` - Research integration
4. ✅ `facilities.py` - Unified CLI
5. ✅ `audit_facilities.py` - Data quality
6. ✅ `backfill_mentions.py` - Utility

### Scripts to Fix (1)
1. ⚠️ `pipeline_ingest.py` - Missing dependencies

### Scripts to Archive (2-3)
1. ⚠️ `full_migration.py` - Legacy migration
2. ⚠️ `migrate_legacy_fields.py` - Schema migration
3. ❓ `verify_backfill.py` - Keep or archive?

### Documentation to Update (2)
1. ❌ `CLAUDE.md` - Remove non-existent scripts
2. ❌ `docs/README_FACILITIES.md` - Same issues

---

## Next Steps

1. Update CLAUDE.md with accurate script info
2. Fix or remove `pipeline_ingest.py`
3. Archive legacy migration scripts (if appropriate)
4. Update README_FACILITIES.md
5. Add "Scripts Reference" section to CLAUDE.md
