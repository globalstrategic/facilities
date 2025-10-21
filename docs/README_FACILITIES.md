# Facilities Data Management System

**Quick Navigation**: This is the primary user guide for the facilities database. See [Documentation Index](#documentation-index) below for other guides.

---

## Overview

This system manages 8,606 mining and processing facilities across 129 countries, featuring structured JSON-based architecture with comprehensive entity resolution, company linking, and research pipeline integration powered by the **EntityIdentity library**.

**Version**: 2.0.1 (Deduplication Complete)
**Database Size**: ~8,455 facilities (after deduplication)
**Last Updated**: 2025-10-21

## Quick Start

```bash
# View database status
python scripts/facilities.py sync --status

# Test entity resolution
python scripts/facilities.py resolve country "Algeria"
python scripts/facilities.py resolve metal "Cu"
python scripts/facilities.py resolve company "BHP"

# Import facilities with entity resolution
python scripts/import_from_report_enhanced.py report.txt --country DZ --enhanced

# Export to parquet format
python scripts/facilities.py sync --export
```

## Architecture

```
facilities/
├── facilities/                   # 8,606 facility JSONs by ISO3 country
│   ├── USA/ (1,623 facilities)
│   ├── CHN/ (1,837 facilities)
│   ├── AUS/ (578 facilities)
│   └── ... (126 more countries)
│
├── scripts/
│   ├── facilities.py             # Unified CLI with sync/resolve commands
│   ├── import_from_report.py     # Standard import pipeline
│   ├── import_from_report_enhanced.py  # Enhanced with entity resolution
│   ├── deep_research_integration.py    # Gemini Deep Research integration
│   │
│   ├── utils/                    # Entity resolution utilities (NEW in v2.0)
│   │   ├── country_detection.py  # Auto-detect ISO codes
│   │   ├── metal_normalizer.py   # Chemical formulas & categories
│   │   ├── company_resolver.py   # Company matching with LEI codes
│   │   ├── facility_matcher.py   # Multi-strategy duplicate detection
│   │   ├── facility_sync.py      # Parquet export/import
│   │   └── migrate_schema.py     # Schema v1→v2 migration
│   │
│   └── tests/                    # 156 comprehensive tests (98.7% passing)
│       ├── test_entity_resolution.py
│       ├── test_company_resolution.py
│       ├── test_facility_matching.py
│       ├── test_facility_sync.py
│       ├── test_import_enhanced.py
│       └── test_schema.py
│
├── schemas/
│   └── facility.schema.json      # JSON Schema v2.0.0 with EI fields
│
├── docs/
│   ├── README_FACILITIES.md (this file)
│   ├── ENTITYIDENTITY_INTEGRATION_PLAN.md  # Complete integration architecture
│   ├── SCHEMA_CHANGES_V2.md               # Schema v2.0 documentation
│   ├── DEEP_RESEARCH_WORKFLOW.md          # Research enrichment guide
│   └── FACILITIES_MIGRATION_PLAN.md       # Legacy migration docs
│
└── output/
    ├── import_logs/              # Import reports with statistics
    ├── research_raw/             # Gemini Deep Research outputs
    ├── research_prompts/         # Generated research prompts
    └── entityidentity_export/    # Parquet exports for EntityIdentity
```

## Data Model

### Facility JSON Structure (Schema v2.0.0)

```json
{
  "facility_id": "zaf-rustenburg-karee-fac",
  "ei_facility_id": "karee_52f2f3d6",
  "name": "Karee Mine",
  "aliases": ["Karee", "Rustenburg Karee"],
  "country_iso3": "ZAF",
  "location": {
    "lat": -25.666,
    "lon": 27.202,
    "precision": "site"
  },
  "types": ["mine", "concentrator"],
  "commodities": [
    {
      "metal": "platinum",
      "primary": true,
      "chemical_formula": "Pt",
      "category": "precious_metal"
    },
    {
      "metal": "palladium",
      "primary": false,
      "chemical_formula": "Pd",
      "category": "precious_metal"
    }
  ],
  "status": "operating",
  "owner_links": [
    {
      "company_id": "cmp-378900F238434B74D281",
      "role": "owner",
      "percentage": 74.0,
      "confidence": 0.95
    }
  ],
  "operator_link": {
    "company_id": "cmp-378900F238434B74D281",
    "confidence": 0.95
  },
  "products": [
    {
      "stream": "PGM concentrate",
      "capacity": 250000,
      "unit": "oz 6E",
      "year": 2024
    }
  ],
  "sources": [
    {
      "type": "gemini_research",
      "id": "South Africa Platinum Study 2025",
      "date": "2025-10-14T00:00:00"
    }
  ],
  "verification": {
    "status": "llm_suggested",
    "confidence": 0.85,
    "last_checked": "2025-10-14T10:00:00",
    "checked_by": "import_pipeline_enhanced",
    "notes": "Enhanced with company resolution"
  }
}
```

### Schema v2.0.0 Enhancements

**New Fields:**
1. **`ei_facility_id`** (optional, string): Links to EntityIdentity database
2. **`chemical_formula`** (optional, in commodities): Chemical formula (e.g., "Cu", "Fe2O3")
3. **`category`** (optional, in commodities): Metal classification (base_metal, precious_metal, rare_earth, etc.)

**100% Backward Compatible**: All existing facilities validate without modification.

## EntityIdentity Integration

The system leverages the **entityidentity library** for comprehensive entity resolution:

### 1. Country Resolution

Auto-detect and normalize country codes:

```python
from scripts.utils.country_detection import detect_country_from_facility, iso2_to_iso3

# Auto-detect from facility data
country = detect_country_from_facility({"country": "Algeria"})  # → "DZA"

# Convert between formats
iso3 = iso2_to_iso3("DZ")  # → "DZA"
iso2 = iso3_to_iso2("DZA")  # → "DZ"
```

**CLI Usage:**
```bash
python scripts/facilities.py resolve country "Algeria"
# Result: DZ / DZA / People's Democratic Republic of Algeria
```

### 2. Metal Normalization

Standardize commodity names with chemical formulas:

```python
from scripts.utils.metal_normalizer import normalize_commodity

result = normalize_commodity("Cu")
# Returns: {
#   "metal": "copper",
#   "chemical_formula": "Cu",
#   "category": "base_metal"
# }
```

**CLI Usage:**
```bash
python scripts/facilities.py resolve metal "Cu"
python scripts/facilities.py resolve metal "lithium carbonate"
```

**Coverage**: 95%+ of common metals, alloys, and compounds

### 3. Company Resolution

Match company names to canonical LEI-based IDs:

```python
from scripts.utils.company_resolver import FacilityCompanyResolver

resolver = FacilityCompanyResolver()
result = resolver.resolve_operator(
    "BHP",
    country_hint="AUS",
    facility_coords=(-25.666, 27.202)
)
# Returns: {
#   "company_id": "cmp-549300HX3DJC74TG4332",
#   "confidence": 1.0,
#   "match_explanation": "Exact name match"
# }
```

**CLI Usage:**
```bash
python scripts/facilities.py resolve company "BHP"
python scripts/facilities.py resolve company "Sibanye-Stillwater" --country ZAF
```

**Database**: 3,687 companies with LEI codes and Wikidata links

### 4. Enhanced Facility Matching

Multi-strategy duplicate detection:

```python
from scripts.utils.facility_matcher import FacilityMatcher

matcher = FacilityMatcher()
duplicates = matcher.find_duplicates(facility_data)
# Strategies: name, location (5km), alias, company+commodity, entityidentity
```

**Performance**: Processes 8,606 facilities in ~0.5s using vectorized operations

### 5. Facility Synchronization

Export/import parquet format for EntityIdentity integration:

```python
from scripts.utils.facility_sync import FacilitySyncManager

manager = FacilitySyncManager()

# Export to parquet
parquet_file = manager.export_to_entityidentity_format(output_path)
# Output: 8,606 facilities → 0.70 MB parquet

# Import from parquet
stats = manager.import_from_entityidentity(parquet_file)
```

**CLI Usage:**
```bash
# Export all facilities
python scripts/facilities.py sync --export

# Import from EntityIdentity
python scripts/facilities.py sync --import facilities.parquet

# Check database status
python scripts/facilities.py sync --status
```

## Workflows

### 1. Import Facilities from Research Report

**Standard Import** (basic normalization):
```bash
python scripts/import_from_report.py report.txt --country DZ --source "Algeria Report 2025"
```

**Enhanced Import** (with entity resolution):
```bash
python scripts/import_from_report_enhanced.py report.txt --country DZ --enhanced --source "Algeria Report 2025"
```

**Enhanced mode features:**
- Auto-resolves company names to canonical IDs
- Adds chemical formulas to commodities
- Multi-strategy duplicate detection
- Confidence boosting for resolved entities
- Backward compatible (same output without --enhanced)

**Import Statistics:**
```
IMPORT COMPLETE (ENHANCED MODE)
============================================================
Country: DZ
Source: Algeria Report 2025
New facilities: 42
Duplicates skipped: 3
Files written: 42

Entity Resolution Stats:
  Metals with formulas: 84 (100%)
  Companies resolved: 28 (66%)
  Confidence boosts: 28
============================================================
```

### 2. Deep Research Integration

Enrich facilities using Gemini Deep Research (see [DEEP_RESEARCH_WORKFLOW.md](DEEP_RESEARCH_WORKFLOW.md)):

```bash
# Generate research prompt
python scripts/deep_research_integration.py \
    --generate-prompt \
    --country ZAF \
    --metal platinum \
    --limit 50

# Process research results
python scripts/deep_research_integration.py \
    --process research_output.json \
    --country ZAF \
    --metal platinum
```

**Company resolution** is automatically applied during research processing.

### 3. Facility Synchronization

```bash
# Export to parquet (for EntityIdentity integration)
python scripts/facilities.py sync --export --output /path/to/output

# Import from EntityIdentity parquet
python scripts/facilities.py sync --import entityidentity/tables/facilities/facilities_*.parquet

# Check database status
python scripts/facilities.py sync --status
```

### 4. Query Facilities

**By Country:**
```python
import json
from pathlib import Path

# Load all facilities in South Africa
for facility_file in Path('facilities/ZAF').glob('*.json'):
    with open(facility_file) as f:
        facility = json.load(f)
        print(f"{facility['name']}: {facility['status']}")
```

**By Metal:**
```python
import json
from pathlib import Path

def find_facilities_by_metal(metal_name):
    facilities = []
    for facility_file in Path('facilities').glob('**/*.json'):
        with open(facility_file) as f:
            facility = json.load(f)
            for commodity in facility.get('commodities', []):
                if commodity['metal'].lower() == metal_name.lower():
                    facilities.append(facility)
                    break
    return facilities

platinum_facilities = find_facilities_by_metal("platinum")
print(f"Found {len(platinum_facilities)} platinum facilities")
```

**By Company:**
```python
import json
from pathlib import Path

def find_facilities_by_company(company_id):
    facilities = []
    for facility_file in Path('facilities').glob('**/*.json'):
        with open(facility_file) as f:
            facility = json.load(f)

            # Check owners
            for owner in facility.get('owner_links', []):
                if owner['company_id'] == company_id:
                    facilities.append(facility)
                    break

            # Check operator
            operator = facility.get('operator_link', {})
            if operator and operator.get('company_id') == company_id:
                if facility not in facilities:
                    facilities.append(facility)

    return facilities

# Find all BHP facilities
bhp_facilities = find_facilities_by_company("cmp-549300HX3DJC74TG4332")
```

### 5. Deduplication

The system includes advanced duplicate detection and cleanup capabilities to maintain data quality.

#### Automatic Duplicate Prevention (Import Time)

The import pipeline (`scripts/import_from_report.py`) automatically detects duplicates using a **4-priority matching strategy**:

**Priority 1: Coordinate-Based Matching (Primary)**
- **Tier 1**: Very close coordinates (0.01° ~1km) + name similarity >60% OR name containment
- **Tier 2**: Close coordinates (0.1° ~11km) + name similarity >85% OR name containment
- Prevents false positives (e.g., nearby but distinct mines)

**Priority 2: Exact Name Match**
- Case-insensitive exact name matching
- Coordinates checked if available for confirmation

**Priority 3: Fuzzy Name Match**
- Name similarity >85% OR word overlap >80%
- Catches variations like "Two Rivers Mine" vs "Two Rivers Platinum Mine"

**Priority 4: Alias Match**
- Checks if name appears in existing facility's aliases

**Example Detection:**
```
Input: "Two Rivers Platinum Mine" at (-24.893, 30.124)
Existing: "Two Rivers" at (-24.893, 30.124)

✓ Tier 1 match: Exact coords + "Two Rivers" contained in "Two Rivers Platinum Mine"
→ Duplicate detected, import skipped
```

#### Deduplication Script (Cleanup Existing Duplicates)

For cleaning up duplicates that already exist in the database:

```bash
# Preview duplicates (dry run - no changes)
python scripts/deduplicate_facilities.py --country ZAF --dry-run

# Clean up duplicates in South Africa
python scripts/deduplicate_facilities.py --country ZAF

# Clean up all countries
python scripts/deduplicate_facilities.py --all
```

**What the script does:**

1. **Identifies duplicate groups** using same logic as import-time detection
2. **Scores facilities** by data completeness:
   - Coordinates (+10 points)
   - Commodities (+2 per commodity)
   - Company mentions (+3 per mention)
   - Products (+2 per product)
   - Aliases (+1 per alias)
   - Known status vs "unknown" (+5)
   - Higher verification confidence (+10)
   - Verification status (human_verified +20, csv_imported +10, llm_verified +5)

3. **Merges data** from duplicates into best facility:
   - Combines aliases (duplicate names become aliases)
   - Merges sources (tracks all import origins)
   - Consolidates commodities (prefers entries with chemical formulas)
   - Combines company mentions (keeps highest confidence per company)
   - Adds merge notes to verification

4. **Deletes** inferior duplicate files

**Example Output:**
```
=== Deduplication LIVE MODE ===

Processing ZAF...
  Loaded 779 facilities
  Found 147 duplicate groups

  Group 1 (2 facilities):
    [38.0] zaf-two-rivers-platinum-mine-fac: Two Rivers Platinum Mine
    [32.0] zaf-two-rivers-fac: Two Rivers
    → Keeping: zaf-two-rivers-platinum-mine-fac
    → Deleted: zaf-two-rivers-fac

  Group 2 (2 facilities):
    [32.0] zaf-new-denmark-fac: New Denmark
    [30.0] zaf-new-denmark-colliery-fac: New Denmark Colliery
    → Keeping: zaf-new-denmark-fac
    → Deleted: zaf-new-denmark-colliery-fac

=== SUMMARY ===
ZAF: 147 groups, 150 removed, 147 kept

Total: 147 duplicate groups, 150 facilities removed
```

**Real-world results (South Africa case study):**
- **Before**: 779 facilities (168 coordinate-based duplicate pairs)
- **After**: 628 facilities
- **Removed**: 151 duplicates (19.4% reduction)

**Merged Facility Example:**
```json
{
  "facility_id": "zaf-two-rivers-platinum-mine-fac",
  "name": "Two Rivers Platinum Mine",
  "aliases": [
    "Two Rivers",
    "Two Rivers Mine",
    "Tweefontein"
  ],
  "location": {
    "lat": -24.893,
    "lon": 30.124,
    "precision": "site"
  },
  "commodities": [
    {"metal": "Platinum", "primary": true},
    {"metal": "Rhodium", "primary": false},
    {"metal": "Gold", "primary": false},
    {"metal": "Ruthenium", "primary": false},
    {"metal": "Iridium", "primary": false}
  ],
  "sources": [
    {"type": "csv_import", "id": "Research Import ZAF 2025-10-20"},
    {"type": "csv_import", "id": "Mines.csv Run 1"},
    {"type": "text_extraction", "id": "Research Import ZAF 2025-10-20"}
  ],
  "verification": {
    "notes": "Merged from: zaf-two-rivers-fac, zaf-two-rivers-mine-fac"
  }
}
```

#### Best Practices

**When importing new data:**
- Always review import logs for duplicate detection statistics
- Check `duplicates_skipped` count in import summary
- Verify duplicates are legitimate (not distinct nearby facilities)

**When running deduplication:**
- **Always** run with `--dry-run` first to preview changes
- Review duplicate groups to ensure correct merging
- Check facility scores to verify best facility selection
- Consider country-by-country deduplication for large databases

**Avoiding false positives:**
- Coordinate thresholds are tuned for mining facilities (~1-11km)
- Name similarity requires high overlap to avoid matching unrelated facilities
- Use aliases to explicitly link facilities with different names

## Data Quality

### Confidence Levels

- **0.95**: Very High - Human verified with multiple sources
- **0.85**: High - EntityIdentity match or reliable source
- **0.75**: Moderate-High - LLM research with entity resolution
- **0.65**: Moderate - CSV import with good data quality
- **0.40**: Low - Partial data or uncertain matching
- **0.20**: Very Low - Minimal data, needs research

### Verification Status

- **`csv_imported`**: Initial import from source data
- **`llm_suggested`**: Enhanced by AI research (Gemini/GPT)
- **`llm_verified`**: Cross-referenced by multiple LLM sources
- **`human_verified`**: Manually reviewed and confirmed
- **`conflicting`**: Contradictory information found

### Confidence Boosting

Enhanced import automatically boosts confidence when:
- Company operator is successfully resolved: +0.10
- Multiple commodities with chemical formulas: +0.05
- Coordinates with site-level precision: +0.05

## Statistics

Current database (as of 2025-10-14):

- **Total Facilities**: 8,606
- **Countries**: 129 (ISO3 codes)
- **Top Countries**: CHN (1,837), USA (1,623), AUS (578), IDN (461), IND (424)
- **Metals/Commodities**: 50+ types
- **With Coordinates**: 99.3% (8,544 facilities)
- **With Company Links**: ~35% (growing via entity resolution)
- **Operating Facilities**: ~45%
- **Average Confidence**: 0.641

### Test Coverage

- **Total Tests**: 156
- **Pass Rate**: 98.7% (154/156 passing)
- **Modules Tested**: All entity resolution, import, sync, and schema validation

## Schema Validation

### Validate Facility Against Schema

```python
import json
import jsonschema

# Load schema v2.0.0
with open('schemas/facility.schema.json') as f:
    schema = json.load(f)

# Validate facility
with open('facilities/USA/usa-stillwater-east-fac.json') as f:
    facility = json.load(f)

try:
    jsonschema.validate(facility, schema)
    print("✓ Facility is valid")
except jsonschema.ValidationError as e:
    print(f"✗ Validation error: {e.message}")
```

### Migrate Facilities to Schema v2.0.0

```bash
# Preview migration (dry run)
python scripts/utils/migrate_schema.py --dry-run

# Migrate all facilities
python scripts/utils/migrate_schema.py

# Migrate single facility
python scripts/utils/migrate_schema.py --facility-id usa-stillwater-east-fac
```

**Migration adds:**
- `ei_facility_id` field (null initially)
- `chemical_formula` to all commodities (null initially)
- `category` to all commodities (null initially)

**Backups created automatically** before modification.

## CLI Commands Reference

### Import Commands

```bash
# Standard import
python scripts/import_from_report.py report.txt --country DZ

# Enhanced import (with entity resolution)
python scripts/import_from_report_enhanced.py report.txt --country DZ --enhanced

# With custom source
python scripts/import_from_report_enhanced.py report.txt --country AFG --enhanced --source "Afghanistan Minerals Report 2025"
```

### Sync Commands

```bash
# Export to parquet
python scripts/facilities.py sync --export
python scripts/facilities.py sync --export --output /custom/path

# Import from parquet
python scripts/facilities.py sync --import facilities.parquet
python scripts/facilities.py sync --import facilities.parquet --overwrite

# Database status
python scripts/facilities.py sync --status
```

### Resolve Commands

```bash
# Test country resolution
python scripts/facilities.py resolve country "Algeria"
python scripts/facilities.py resolve country "DZ"

# Test metal normalization
python scripts/facilities.py resolve metal "Cu"
python scripts/facilities.py resolve metal "platinum"
python scripts/facilities.py resolve metal "lithium carbonate"

# Test company resolution
python scripts/facilities.py resolve company "BHP"
python scripts/facilities.py resolve company "Sibanye-Stillwater" --country ZAF
```

### Research Commands

```bash
# Generate research prompt
python scripts/facilities.py research --generate-prompt --country ZAF --metal platinum

# Process research results
python scripts/facilities.py research --process output.json --country ZAF --metal platinum
```

### Test Commands

```bash
# Run all tests
python scripts/facilities.py test

# Run specific test suite
python scripts/facilities.py test --suite dedup
python scripts/facilities.py test --suite migration
```

## Maintenance

### Update Facility Data

```python
import json
from datetime import datetime

# Load facility
with open('facilities/USA/usa-stillwater-east-fac.json') as f:
    facility = json.load(f)

# Update fields
facility['status'] = 'operating'
facility['verification']['status'] = 'human_verified'
facility['verification']['last_checked'] = datetime.now().isoformat()
facility['verification']['checked_by'] = 'manual_review'

# Save with backup
import shutil
backup_path = f'facilities/USA/usa-stillwater-east-fac.backup_{int(datetime.now().timestamp())}.json'
shutil.copy('facilities/USA/usa-stillwater-east-fac.json', backup_path)

with open('facilities/USA/usa-stillwater-east-fac.json', 'w') as f:
    json.dump(facility, f, indent=2, ensure_ascii=False)
```

### Data Quality Checks

```bash
# Find facilities without status
grep -r '"status": "unknown"' facilities/

# Count facilities by status
for status in operating closed suspended; do
  echo "$status: $(grep -r "\"status\": \"$status\"" facilities | wc -l)"
done

# Find facilities without coordinates
grep -r '"lat": null' facilities/ | wc -l

# Check facilities with low confidence
find facilities -name "*.json" -exec grep -l '"confidence": 0\.[0-4]' {} \;
```

## Performance Characteristics

### Import Performance
- **Standard import**: ~50 facilities/second
- **Enhanced import**: ~10 facilities/second (entity resolution overhead)
- **Memory usage**: ~150MB (with all resolvers loaded)

### Query Performance
- **Database loading**: 8,606 facilities in ~0.5s
- **Company resolution**: First query 2-3s, cached queries <10ms
- **Facility matching**: ~106ms for all 5 strategies (vectorized)
- **Parquet export**: 8,606 facilities in <5s

### Storage
- **JSON database**: ~35 MB (8,606 files)
- **Parquet export**: 0.70 MB (compressed)
- **With backups**: ~70 MB (2x for safety)

## Troubleshooting

### Common Issues

1. **EntityIdentity not found**
   ```bash
   # Ensure entityidentity is in PYTHONPATH
   export PYTHONPATH="/Users/willb/Github/GSMC/entityidentity:$PYTHONPATH"

   # Or install it
   pip install git+https://github.com/microprediction/entityidentity.git
   ```

2. **Country not resolved**
   - Use `resolve country` command to test
   - Check that country name is spelled correctly
   - Try ISO2 or ISO3 code directly

3. **Company resolution failed**
   - Company may not be in EntityIdentity database (3,687 companies)
   - Try variations of company name
   - Check if company has LEI code
   - Add to manual mappings if needed

4. **Duplicate facilities**
   - Enhanced matcher checks: name, location, aliases, company+commodity, EntityIdentity
   - Review duplicates in import logs
   - Merge manually if needed

5. **Schema validation fails**
   - Check that all required fields are present
   - Verify chemical_formula pattern (if provided)
   - Verify category enum values
   - Run: `python scripts/tests/test_schema.py`

6. **Import hangs or is slow**
   - EntityIdentity loads ~50MB parquet on first use
   - Company resolution caches results (subsequent imports faster)
   - Use standard import if speed is critical

## Related Documentation

- **[EntityIdentity Integration Plan](./ENTITYIDENTITY_INTEGRATION_PLAN.md)** - Complete architecture
- **[Schema Changes v2.0](./SCHEMA_CHANGES_V2.md)** - Schema v2.0.0 documentation
- **[Deep Research Workflow](./DEEP_RESEARCH_WORKFLOW.md)** - Research enrichment guide
- **[Facilities Migration Plan](./FACILITIES_MIGRATION_PLAN.md)** - Legacy migration

## Future Enhancements

### Planned Features

1. **Automatic Entity Linking**
   - Background job to resolve unlinked companies
   - Auto-populate chemical formulas for existing commodities
   - Link facilities to EntityIdentity parquet database

2. **Real-time Updates**
   - WebSocket feed for status changes
   - Production data integration
   - Price impact correlation

3. **Advanced Analytics**
   - Supply chain mapping
   - Geographic clustering
   - Capacity utilization trends
   - Risk analysis (geopolitical, environmental)

4. **External Integration**
   - S&P Global Market Intelligence
   - Wood Mackenzie data
   - Government mining registries
   - Satellite imagery analysis

5. **Research Automation**
   - Scheduled Gemini refreshes
   - News monitoring for changes
   - Automated verification updates

## Version History

- **v2.0.0** (2025-10-14): EntityIdentity integration complete
  - Added country, metal, company entity resolution
  - Enhanced facility matching with 5 strategies
  - Parquet export/import for EntityIdentity sync
  - Schema enhancements (ei_facility_id, chemical_formula, category)
  - 156 comprehensive tests (98.7% passing)
  - CLI commands: sync, resolve

- **v1.0.0** (2025-10-12): Initial structured database
  - Migrated 8,443 facilities from CSV
  - JSON schema validation
  - Basic duplicate detection
  - Deep Research integration

## Support

For questions or issues:
1. Check this documentation
2. Review integration plan: `docs/ENTITYIDENTITY_INTEGRATION_PLAN.md`
3. Check test files for usage examples
4. Review facility schema: `schemas/facility.schema.json`

---

**Database Status**: Production-ready | **Test Coverage**: 98.7% | **Facilities**: ~8,455 | **Countries**: 129

---

# Documentation Index

**Complete guide to all facilities documentation** - Find the right doc for your needs.

---

## 📚 Primary Documentation

### This Document (README_FACILITIES.md)
**User Guide & API Reference** - You are here!

- Data model and JSON schema
- Import workflows and examples
- Entity resolution (countries, metals, companies)
- Deduplication system
- Query patterns and examples
- Data quality guidelines

**Best for**: New users, import workflows, querying facilities, deduplication

---

### [CLAUDE.md](../CLAUDE.md)
**Developer Guide** - For developers working with the codebase via Claude Code

- Code architecture and patterns
- Development commands (import, dedup, enrich)
- Duplicate detection strategy (4-priority system)
- Testing patterns
- Common workflows (adding countries, debugging, deduplication)
- Scripts reference (all production scripts)
- Performance characteristics

**Best for**: Development, architecture understanding, workflow automation, troubleshooting

---

## 🏗️ Architecture & Integration

### [ENTITYIDENTITY_INTEGRATION_PLAN.md](ENTITYIDENTITY_INTEGRATION_PLAN.md)
**EntityIdentity Integration Architecture** (920 lines)

- Complete integration design
- Company resolution with quality gates
- Metal normalization with chemical formulas
- Country auto-detection
- Facility matching strategies
- Two-phase company resolution pattern
- Testing framework

**Best for**: Understanding entity resolution system, integration architecture

---

### [SCHEMA_CHANGES_V2.md](SCHEMA_CHANGES_V2.md)
**Schema v2.0.0 Documentation** (453 lines)

- Schema evolution from v1 to v2
- New fields: `chemical_formula`, `category`, `company_mentions[]`
- Phase 2 company resolution fields
- Migration guide
- Backward compatibility notes

**Best for**: Schema reference, migration planning, field definitions

---

## 🔄 Specialized Workflows

### [DEEP_RESEARCH_WORKFLOW.md](DEEP_RESEARCH_WORKFLOW.md)
**Gemini Deep Research Integration** (396 lines)

- Research prompt generation
- Processing research outputs
- Company resolution during enrichment
- Batch processing workflows
- Output formats

**Best for**: Enriching facilities with AI research, batch updates

---

### [guides/RESOLUTION_WORKFLOW.md](guides/RESOLUTION_WORKFLOW.md)
**Entity Resolution Workflow** (~11KB)

- Detailed resolution workflows
- Country, metal, and company resolution
- Confidence scoring
- Quality gates
- Edge cases and troubleshooting

**Best for**: Deep dive into resolution logic, quality gate tuning

---

## 📖 Historical Documentation

### [implementation_history/](implementation_history/)

Historical design decisions, migration plans, and point-in-time analyses:

- `COVERAGE_2025-10-20.md` - Test coverage snapshot
- `PHASE_2_ALTERNATE_PATH.md` - Company resolution design decision

**Best for**: Understanding design decisions, historical context

---

## 📝 Version History

### [../CHANGELOG.md](../CHANGELOG.md)
**Complete version history and release notes**

- Version 2.0.1 (2025-10-21): Deduplication system
- Version 2.0.0 (2025-10-20): EntityIdentity integration
- Earlier versions and migration notes
- Known issues and deprecations

**Best for**: What's new, migration guides, version compatibility

---

## 🗺️ Documentation Map by Use Case

### I want to...

**Import facilities from a CSV/report:**
→ [Workflows](#workflows) (this document)

**Clean up duplicate facilities:**
→ [Deduplication](#5-deduplication) (this document)
→ [CLAUDE.md - Deduplicating Facilities](../CLAUDE.md#deduplicating-facilities)

**Understand the code architecture:**
→ [CLAUDE.md - Code Architecture](../CLAUDE.md#code-architecture)

**Resolve company names to canonical IDs:**
→ [Company Resolution](#3-company-resolution) (this document)
→ [ENTITYIDENTITY_INTEGRATION_PLAN.md](ENTITYIDENTITY_INTEGRATION_PLAN.md)

**Understand the JSON schema:**
→ [Data Model](#data-model) (this document)
→ [SCHEMA_CHANGES_V2.md](SCHEMA_CHANGES_V2.md)

**Run tests:**
→ [CLAUDE.md - Testing Patterns](../CLAUDE.md#7-testing-patterns)

**Enrich facilities with research:**
→ [DEEP_RESEARCH_WORKFLOW.md](DEEP_RESEARCH_WORKFLOW.md)

**Contribute to the codebase:**
→ [CLAUDE.md](../CLAUDE.md)
→ [ENTITYIDENTITY_INTEGRATION_PLAN.md](ENTITYIDENTITY_INTEGRATION_PLAN.md)

**Check what's new:**
→ [CHANGELOG.md](../CHANGELOG.md)

---

## 📊 Quick Stats

**Current Database** (2025-10-21):
- ~8,455 facilities across 129 countries
- 99.3% with coordinates
- 50+ metals/commodities
- Schema v2.0.1

**Documentation Files**: 9 active documents
- 2 primary guides (README_FACILITIES.md, CLAUDE.md)
- 4 specialized references
- 2 historical docs
- 1 changelog

---

## 🔗 External Links

- [EntityIdentity Library](https://github.com/globalstrategic/entityidentity) - Entity resolution backbone
- [Gemini Deep Research](https://deepmind.google/technologies/gemini/) - AI research integration
- [JSON Schema Specification](https://json-schema.org/) - Schema validation reference

---

## 📞 Support

For questions or issues:
1. Check this INDEX for relevant documentation
2. Review [CHANGELOG.md](../CHANGELOG.md) for recent changes
3. Check [CLAUDE.md - Known Issues](../CLAUDE.md#known-issues--gotchas)
4. Review import logs in `output/import_logs/`

---

**Last Updated**: 2025-10-21
