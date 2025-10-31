# Facilities Scripts

Command-line tools for managing the facilities database.

## Quick Start

```bash
# Import facilities from research reports
python import_from_report.py report.txt --country DZ

# Backfill missing data
python backfill.py geocode --country ARE --interactive
python backfill.py companies --country IND
python backfill.py all --country ARE

# Clean up duplicates
python tools/deduplicate_facilities.py --country ZAF --dry-run

# Export to parquet
python facilities.py sync --export
```

## Main Scripts

### import_from_report.py

**Purpose**: Import facilities from markdown tables, CSV, or TSV files

**Supported formats:**
- Markdown tables (`| header | header |`)
- CSV files (comma-separated)
- Tab-separated tables (TSV)

**Features:**
- Auto-detects country from filename or content
- Extracts company mentions (Phase 1)
- Normalizes metals with chemical formulas
- Detects duplicates automatically
- Writes schema-compliant JSON files

**Examples:**

```bash
# From text file
python import_from_report.py report.txt --country DZ --source "Algeria Report 2025"

# Auto-detect country from filename
python import_from_report.py bulgaria.txt

# From CSV
python import_from_report.py facilities.csv --country DZ

# From stdin
cat report.txt | python import_from_report.py --country DZ
```

**Table requirements:**
- Headers must contain 3+ indicator keywords
- Keywords: `facility`, `mine`, `name`, `operator`, `owner`, `location`, `province`, `region`, `commodity`, `commodities`, `metal`, `metals`
- Plural forms recognized (commodities, metals)

**Output:**
- Facility JSONs in `../facilities/{COUNTRY}/`
- Import log in `../output/import_logs/`

### backfill.py

**Purpose**: Enrich existing facilities with missing data

**Operations:**
- `geocode`: Add coordinates (industrial zones → Nominatim → interactive)
- `companies`: Resolve company mentions to canonical IDs
- `metals`: Add chemical formulas and categories
- `all`: Run all enrichment operations

**Examples:**

```bash
# Geocode facilities
python backfill.py geocode --country ARE
python backfill.py geocode --country ARE --interactive

# Resolve companies
python backfill.py companies --country IND --profile moderate

# Add metal formulas
python backfill.py metals --all

# Do everything
python backfill.py all --country ARE --interactive

# Dry run (preview changes)
python backfill.py all --country ARE --dry-run

# Multiple countries
python backfill.py geocode --countries ARE,IND,CHN
```

### enrich_companies.py

**Purpose**: Phase 2 company resolution (batch processing)

**Examples:**

```bash
# Enrich all facilities
python enrich_companies.py

# Specific country
python enrich_companies.py --country IND

# Dry run
python enrich_companies.py --dry-run

# Set confidence threshold
python enrich_companies.py --min-confidence 0.75
```

**Output**: `../tables/facilities/facility_company_relationships.parquet`

### facilities.py

**Purpose**: Unified CLI wrapper (limited functionality)

**Examples:**

```bash
# Test entity resolution
python facilities.py resolve country "Algeria"
python facilities.py resolve metal "Cu"
python facilities.py resolve company "BHP"

# Export/import
python facilities.py sync --export
python facilities.py sync --import facilities.parquet

# Database status
python facilities.py sync --status

# Run tests
python facilities.py test
python facilities.py test --suite dedup
```

## Utility Tools (tools/)

### deduplicate_facilities.py

**Purpose**: Batch cleanup of existing duplicates

**Examples:**

```bash
# Preview duplicates (always do this first)
python tools/deduplicate_facilities.py --country ZAF --dry-run

# Clean up duplicates
python tools/deduplicate_facilities.py --country ZAF

# All countries (use with caution)
python tools/deduplicate_facilities.py --all
```

**What it does:**
- Finds duplicate groups using 4-priority matching
- Scores facilities by data completeness
- Merges data (aliases, sources, commodities, company mentions)
- Deletes inferior duplicates
- Tracks merge history in verification notes

### geocode_facilities.py

**Purpose**: Standalone geocoding utility

**Examples:**

```bash
# Geocode all facilities in a country
python tools/geocode_facilities.py --country ARE

# Interactive mode
python tools/geocode_facilities.py --country ARE --interactive

# Dry run
python tools/geocode_facilities.py --country ARE --dry-run

# Single facility
python tools/geocode_facilities.py --facility-id are-union-cement-company-fac
```

### audit_facilities.py

**Purpose**: Data quality checks and reporting

```bash
python tools/audit_facilities.py
```

### verify_backfill.py

**Purpose**: Verify backfill results

```bash
python tools/verify_backfill.py
```

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run specific test
pytest tests/test_import_enhanced.py -v

# Via CLI wrapper
python facilities.py test
```

## Common Workflows

### Adding a New Country

```bash
# Import facilities
python import_from_report.py country_report.txt --country DZ

# Enrich with geocoding
python backfill.py geocode --country DZ --interactive

# Resolve companies
python enrich_companies.py --country DZ
```

### Cleaning Up Duplicates

```bash
# Preview what will be merged
python tools/deduplicate_facilities.py --country ZAF --dry-run

# Review the output, then run for real
python tools/deduplicate_facilities.py --country ZAF
```

### Enriching Existing Data

```bash
# Add missing coordinates
python backfill.py geocode --country ARE --interactive

# Add chemical formulas to commodities
python backfill.py metals --all

# Resolve company mentions to canonical IDs
python backfill.py companies --country IND
```

## Troubleshooting

**"No facility tables found in report"**
- Ensure headers contain 3+ indicator keywords (facility, mine, name, operator, commodity, location)
- Check table format (must be pipe-separated `|` or CSV)
- Use plural forms: commodities, metals (recognized in v2.1.1+)

**"Too many duplicates detected"**
- This is working correctly - preventing duplicate entries
- Review import log in `output/import_logs/` for details

**"Coordinates not parsing"**
- Use decimal degrees: `34.267` not `34° 16' N`

**"EntityIdentity not found"**
```bash
export PYTHONPATH="/Users/willb/Github/GSMC/entityidentity:$PYTHONPATH"
# or
pip install git+https://github.com/microprediction/entityidentity.git
```

**"Metal formulas not being extracted"**
- Ensure EntityIdentity is installed
- Check that code uses `result.get('chemical_formula')` not `result.get('formula')` (fixed in v2.1.1)

## Documentation

- **Developer guide**: [../CLAUDE.md](../CLAUDE.md)
- **User guide**: [../README.md](../README.md)
- **Version history**: [../CHANGELOG.md](../CHANGELOG.md)
- **Schema reference**: `../schemas/facility.schema.json`

## Directory Structure

```
scripts/
├── import_from_report.py            # Main import pipeline (1,771 lines)
├── backfill.py                      # Unified enrichment system
├── enrich_companies.py              # Phase 2 company resolution
├── facilities.py                    # CLI wrapper
├── deep_research_integration.py     # Gemini research integration
│
├── tools/                           # Standalone utilities
│   ├── deduplicate_facilities.py
│   ├── geocode_facilities.py
│   ├── audit_facilities.py
│   └── verify_backfill.py
│
├── utils/                           # Shared libraries
│   ├── company_resolver.py
│   ├── country_utils.py
│   ├── deduplication.py
│   ├── geocoding.py
│   ├── name_canonicalizer.py
│   └── facility_sync.py
│
└── tests/                           # Test suites
    ├── test_import_enhanced.py
    ├── test_dedup.py
    ├── test_schema.py
    └── test_facility_sync.py
```
