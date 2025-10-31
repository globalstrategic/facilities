# CLAUDE.md

Developer guidance for Claude Code when working with the facilities repository.

## Quick Reference

**Database**: 10,641 facilities across 135 countries
**Package**: `talloy` | **Version**: 2.1.1 | **Schema**: v2.0.0
**Main Docs**: See [README.md](README.md) for comprehensive documentation

## Common Commands

```bash
# Import facilities
python scripts/import_from_report.py report.txt --country DZ

# Backfill missing data
python scripts/backfill.py geocode --country ARE --interactive
python scripts/backfill.py companies --country IND
python scripts/backfill.py metals --all

# Clean duplicates
python scripts/tools/deduplicate_facilities.py --country ZAF --dry-run

# Export data
python scripts/facilities.py sync --export
```

## Key Architecture Points

### 1. File Structure

```
facilities/{ISO3}/{iso3}-{slug}-fac.json  # Individual facility JSONs
scripts/import_from_report.py             # Main import pipeline (1,771 lines)
scripts/backfill.py                       # Unified enrichment system
scripts/utils/                            # Shared libraries
scripts/tools/                            # Standalone utilities
schemas/facility.schema.json              # JSON Schema v2.0.0
```

### 2. Import Pipeline (import_from_report.py)

**What it does:**
- Parses markdown tables, CSV, TSV formats
- Auto-detects country from filename or content
- Extracts company mentions (Phase 1 - NOT resolution)
- Normalizes metals with chemical formulas via EntityIdentity
- Detects duplicates (4-priority strategy)
- Writes facility JSONs

**Table validation** (v2.1.1):
- Requires 3+ indicator keywords in headers
- Keywords: `facility`, `mine`, `name`, `operator`, `owner`, `location`, `province`, `region`, `commodity`, `commodities`, `metal`, `metals`
- Recognizes plural forms and multiple indicators per header

**Duplicate detection priorities:**
1. Coordinate-based (two-tier: 0.01°/0.1° with name matching)
2. Exact name match
3. Fuzzy name match (>85% similarity OR >80% word overlap)
4. Alias match

### 3. Two-Phase Company Resolution

**Phase 1** (during import): Extract mentions → `company_mentions[]` array
**Phase 2** (enrich_companies.py): Resolve mentions → canonical IDs → parquet table

**DO NOT** modify facility JSONs for company links. Relationships go in `tables/facilities/facility_company_relationships.parquet`.

### 4. Backfill System (backfill.py)

Unified enrichment for existing facilities:

- `geocode`: Add coordinates (industrial zones → Nominatim → interactive)
- `companies`: Resolve company_mentions to canonical IDs
- `metals`: Add chemical formulas and categories
- `all`: Run everything

### 5. Entity Resolution (EntityIdentity)

**Location:** `../entityidentity/` (sibling repo)

**Integration points:**
- `country_utils.py`: ISO3 normalization
- `metal_identifier()`: Chemical formulas + categories (use `.get('chemical_formula')` not `.get('formula')`)
- `CompanyResolver`: Canonical company matching with quality gates

**Important API changes:**
- EntityIdentity changed key from `'formula'` to `'chemical_formula'` - code updated in v2.1.1
- Always use `result.get('chemical_formula')` when calling `metal_identifier()`

## Critical Development Patterns

### 1. DO NOT Create Duplicates

Import pipeline has automatic duplicate prevention. If you need to clean existing duplicates:

```bash
# ALWAYS dry-run first
python scripts/tools/deduplicate_facilities.py --country ZAF --dry-run
python scripts/tools/deduplicate_facilities.py --country ZAF
```

### 2. DO NOT Modify Facility JSONs for Phase 2 Company Links

Phase 2 relationships go in parquet, not JSON:

```python
# ✗ Wrong: Don't modify facility JSON
facility['operator_link'] = 'cmp-549300HX3DJC74TG4332'

# ✓ Right: Use enrich_companies.py to write to parquet
python scripts/enrich_companies.py --country IND
```

### 3. Always Use ISO3 Country Codes

All directories and `country_iso3` fields use 3-letter codes:

```python
from scripts.utils.country_utils import normalize_country_to_iso3

iso3 = normalize_country_to_iso3("Algeria")  # → "DZA"
```

### 4. Facility ID Pattern

Pattern: `{iso3}-{slug}-fac`

```python
def slugify(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r'\([^)]*\)', '', text)  # Remove parentheticals
    text = re.sub(r'[^a-z0-9]+', '-', text)
    return text.strip('-')

# Example: "Karee Mine (Rustenburg)" → "zaf-karee-mine-fac"
```

## Script Responsibilities

### Main Scripts

| Script | Lines | Purpose |
|--------|-------|---------|
| `import_from_report.py` | 1,771 | Import pipeline with entity resolution |
| `backfill.py` | (TBD) | Unified enrichment system |
| `facilities.py` | 466 | Unified CLI wrapper (limited functionality) |
| `enrich_companies.py` | (TBD) | Phase 2 company resolution |
| `deep_research_integration.py` | (TBD) | Gemini research integration |

### Utility Modules (scripts/utils/)

| Module | Purpose |
|--------|---------|
| `company_resolver.py` | CompanyResolver with quality gates (strict/moderate/permissive) |
| `country_utils.py` | ISO3 normalization |
| `deduplication.py` | Shared duplicate detection logic |
| `geocoding.py` | Multi-strategy geocoding service |
| `id_utils.py` | Canonical ID mapping |
| `name_canonicalizer.py` | Name normalization |
| `facility_sync.py` | Parquet export/import |

### Standalone Tools (scripts/tools/)

| Tool | Purpose |
|------|---------|
| `deduplicate_facilities.py` | Batch duplicate cleanup |
| `geocode_facilities.py` | Standalone geocoding utility |
| `audit_facilities.py` | Data quality checks |
| `verify_backfill.py` | Verify enrichment results |

## Common Gotchas

1. **EntityIdentity API change**: Use `result.get('chemical_formula')` not `result.get('formula')` (fixed in v2.1.1)

2. **Country codes**: All storage uses ISO3 (DZA, USA, ZAF), but EntityIdentity returns ISO2 - auto-converted

3. **Company mentions vs links**:
   - Phase 1: `company_mentions[]` (raw strings in facility JSON)
   - Phase 2: `owner_links[]` / `operator_link` (canonical IDs in parquet)

4. **Table validation** (v2.1.1): Headers need 3+ indicator keywords. If import fails with "No facility tables found":
   - Check headers contain words like: facility, mine, operator, commodity, location
   - Use plural forms work: commodities, metals
   - Combine indicators: "Facility Name" = 2 matches (facility + name)

5. **Geocoding strategies**: Industrial zones → Nominatim → interactive (in that order)

6. **Parquet loading**: EntityIdentity loads ~50MB on first use (2-3s startup delay)

## Testing

```bash
# Run all tests
pytest scripts/tests/ -v

# Run specific test
pytest scripts/tests/test_import_enhanced.py -v

# Via CLI wrapper
python scripts/facilities.py test
python scripts/facilities.py test --suite dedup
```

## Version History (Recent)

- **v2.1.1** (2025-10-27): Enhanced table detection, EntityIdentity API fix
- **v2.1.0** (2025-10-21): Geocoding & backfill system
- **v2.0.1** (2025-10-21): Deduplication system
- **v2.0.0** (2025-10-20): EntityIdentity integration

See [CHANGELOG.md](CHANGELOG.md) for complete history.

## Where to Find Information

- **User guide**: [README.md](README.md) - Comprehensive 1,300+ line all-in-one documentation
- **Developer guide**: This file (CLAUDE.md)
- **Version history**: [CHANGELOG.md](CHANGELOG.md)
- **Schema reference**: `schemas/facility.schema.json`
- **Import logs**: `output/import_logs/`
- **Scripts docs**: `scripts/README.md` (focused on import workflow)

## External Dependencies

**Required:**
- EntityIdentity library (in `../entityidentity/` or via pip)

**Optional:**
- geopy (for geocoding)
- Anthropic API (for LLM-based enrichment)

```bash
# Ensure EntityIdentity accessible
export PYTHONPATH="/Users/willb/Github/GSMC/entityidentity:$PYTHONPATH"

# Or install as package
pip install git+https://github.com/microprediction/entityidentity.git
```

## Performance Characteristics

- **Import**: ~50 facilities/second (standard), ~10 facilities/second (with entity resolution)
- **Company enrichment**: ~5-10 facilities/second (batch resolution)
- **Database loading**: 10,641 facilities in ~0.5s
- **Parquet export**: All facilities in <5s
- **Memory usage**: ~150MB (with all resolvers loaded)

## Statistics (Current)

- **Facilities**: 10,641
- **Countries**: 135 (ISO3 codes)
- **Top Countries**: CHN (1,837), USA (1,623), AUS (613), ZAF (628), IDN (461), IND (424)
- **With Coordinates**: ~99%
- **Operating Facilities**: ~45%
- **Average Confidence**: 0.64
