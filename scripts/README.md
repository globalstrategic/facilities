# Facilities Scripts

Unified CLI for managing the facilities database.

## Quick Start

All operations now use a single `facilities.py` CLI:

```bash
# Import facilities from research reports
python facilities.py import report.txt --country DZ

# Enrich with Gemini Deep Research
python facilities.py research --generate-prompt --country ZAF --metal platinum

# Run tests
python facilities.py test
```

## Import Research Reports

Import facilities from text reports in two simple steps:

```bash
# Step 1: Save your report to a file
cat > report.txt
[Paste your full report text, then press Ctrl+D]

# Step 2: Import
python facilities.py import report.txt --country DZ
```

**That's it!** The script will:
- Auto-detect the correct country code (DZA → DZ)
- Extract facility tables from your report
- Check for duplicates against existing facilities
- Generate properly formatted JSON files

**Alternative methods:**

```bash
# If report is already saved
python facilities.py import report.txt --country AF

# With optional custom source name
python facilities.py import report.txt --country DZ --source "Algeria Mining Report 2025"

# From clipboard (Mac)
pbpaste > report.txt
python facilities.py import report.txt --country DZ

# From clipboard (Linux with xclip)
xclip -o > report.txt
python facilities.py import report.txt --country AF
```

### What it does

1. **Auto-detects country code** - use DZA or DZ, both work
2. **Extracts facility tables** from markdown text (supports both `|` and tab-separated)
3. **Normalizes** metals, facility types, and operational status
4. **Checks for duplicates** by name, location, and aliases
5. **Generates** schema-compliant JSON files
6. **Creates** detailed import report with statistics

### Output

Files created in: `../facilities/{COUNTRY}/`

Example: `../facilities/DZ/dz-gara-djebilet-fac.json`

Import report: `../output/import_logs/import_report_{COUNTRY}_{timestamp}.json`

### Example output

```
INFO: Found existing directory 'DZ' for input 'DZA'
INFO: Processing report for DZ...
INFO: Found 1 facility tables
INFO: Loaded 22 existing facilities for duplicate detection
INFO: Found 41 new facilities

============================================================
IMPORT COMPLETE
============================================================
Country: DZ
Source: Algeria Mining Report 2025
New facilities: 41
Duplicates skipped: 1
Files written: 41
============================================================

Duplicates found (skipped 1 existing facilities):
  - 'El Abed Oued Zaunder Mines' (exists as dz-el-abed-oued-zaunder-mines-fac)
```

### Duplicate Detection

Won't create duplicates if:
- Exact facility ID match
- Same name + location within ~1km (0.01°)
- Same name when no coordinates (conservatively assumes duplicate)
- Name matches an existing facility's alias

**Tested and verified:** Run `python test_dedup.py` to verify duplicate detection is working.

### Country Codes

**The script auto-detects existing country directories.** You can use any ISO code (2 or 3 letter) and it will find the correct directory:

- `DZA` → `DZ` (Algeria)
- `AFG` → `AF` (Afghanistan)
- `USA` → `USA` (United States)
- `ARG` → `ARG` (Argentina)
- `AUS` → `AUS` (Australia)

**No need to check existing directories** - just use any valid ISO code and the script handles the rest.

### Input Format

The script automatically extracts tables from your report text. Supports both:
- **Pipe-separated tables**: `| Name | Location | ... |`
- **Tab-separated tables**: `Name\tLocation\t...` (like from Excel/Sheets)

**Column names** (flexible - script normalizes these):
- **Site/Mine Name** (required) - Facility name
- **Latitude, Longitude** OR **Coordinates (Lat, Lon)** (recommended) - Decimal coordinates
- **Primary Commodity** - Main metal/mineral
- **Other Commodities** - Secondary metals/minerals
- **Asset Type** - Mine, Smelter, Plant, etc.
- **Operational Status** - Operating, Construction, Planned, Closed, etc.
- **Operator(s)** - Company names
- **Synonyms/Aliases** - Alternative names
- **Notes** - Additional information

### Normalization

**Metals:** `Cu` → `copper`, `Au` → `gold`, `REE` → `rare earths`, `Fe` → `iron`

**Types:** `Open Pit Mine` → `mine`, `Cement Factory` → `plant`, `Copper Smelter` → `smelter`

**Status:** `Operational` → `operating`, `In Development` → `construction`, `Proposed` → `planned`

## Company Enrichment (Phase 2)

After importing facilities, enrich them with resolved company links:

```bash
# Enrich all facilities
python enrich_companies.py

# Enrich specific country
python enrich_companies.py --country IND

# Preview without saving
python enrich_companies.py --dry-run

# Set confidence threshold
python enrich_companies.py --min-confidence 0.75
```

**What it does:**
- Batch resolves company mentions to canonical company IDs
- Uses quality gates (auto_accept / review / pending)
- Writes relationships to `../tables/facilities/facility_company_relationships.parquet`
- Does NOT modify facility JSONs (relationships stored separately)

**Workflow:**
1. Import facilities (Phase 1) - extracts company mentions
2. Run enrichment (Phase 2) - resolves mentions to canonical IDs
3. Review pending items if needed

See `../docs/ENTITYIDENTITY_INTEGRATION_PLAN.md` for architecture details.

## Deep Research Integration

Enrich facilities with Gemini Deep Research data:

```bash
# Generate research prompt
python facilities.py research --generate-prompt --country ZAF --metal platinum --limit 50

# Process research output
python facilities.py research --process output.json --country ZAF --metal platinum

# Batch processing
python facilities.py research --batch research_batch.jsonl
```

See `../docs/DEEP_RESEARCH_WORKFLOW.md` for detailed workflow.

## Utility Scripts

Additional maintenance and verification tools:

```bash
# Run data quality checks
python audit_facilities.py

# Backfill company mentions from CSV sources
python backfill.py mentions --country IND

# Verify backfill results
python verify_backfill.py

# Export/import facilities to parquet format
python facilities.py sync --export
python facilities.py sync --import facilities.parquet
```

## Testing

Run test suites to verify functionality:

```bash
# Run all tests
python facilities.py test

# Run specific test suite
python facilities.py test --suite dedup
python facilities.py test --suite schema
```

## Directory Structure

```
scripts/
├── facilities.py                    # Unified CLI (main entry point)
├── import_from_report.py            # Phase 1: Import with entity resolution
├── backfill.py                      # Unified backfill system (geocoding, companies, metals, mentions)
├── enrich_companies.py              # Phase 2: Batch company enrichment
├── deep_research_integration.py     # Gemini Deep Research integration
│
├── tools/                           # Standalone utility tools
│   ├── audit_facilities.py          # Data quality checks
│   ├── deduplicate_facilities.py    # Batch deduplication utility
│   ├── verify_backfill.py           # Verify backfill results
│   ├── geocode_facilities.py        # Standalone geocoding utility
│   └── legacy/                      # Archived one-time migration scripts
│       ├── full_migration.py        # Legacy CSV → JSON migration
│       └── migrate_legacy_fields.py # Schema field migration
│
├── tests/                           # Test suites
│   ├── test_dedup.py
│   ├── test_import_enhanced.py
│   ├── test_facility_sync.py
│   └── test_schema.py
│
├── utils/                           # Shared utilities (library modules)
│   ├── company_resolver.py          # CompanyResolver with quality gates
│   ├── id_utils.py                  # Canonical ID mapping
│   ├── country_utils.py             # Country code normalization
│   ├── ownership_parser.py          # Ownership percentage parsing
│   ├── facility_sync.py             # Parquet export/import
│   └── paths.py                     # Shared path configuration
│
└── README.md
```

## Troubleshooting

**"Ctrl+D / Cmd+D not working after paste"**
- **Solution:** Use the two-step method (recommended):
  ```bash
  cat > report.txt
  [Paste, Ctrl+D]
  python facilities.py import report.txt --country DZ
  ```
- Or use clipboard directly: `pbpaste > report.txt` (Mac) or `xclip -o > report.txt` (Linux)

**"Report seems very small / paste was cut off"**
- Terminal paste has limits (typically 4-16KB)
- **Solution:** Save your report in a text editor first, then:
  ```bash
  python facilities.py import /path/to/report.txt --country DZ
  ```
- Or use clipboard: `pbpaste > report.txt` bypasses terminal paste limits

**"No facility tables found in report"**
- Ensure your report contains markdown tables with `|` separators
- Tables need headers like "Mine Name", "Commodity", "Location"
- Check that the full report was saved (use `wc -l report.txt` to verify)

**"Too many duplicates detected"**
- Check the import report JSON for details on what matched
- This is working correctly - it's preventing duplicate entries
- Review `../output/import_logs/import_report_*.json`

**"Coordinates not parsing"**
- Use decimal degrees: `34.267` not `34° 16' N`
- Converter: https://www.fcc.gov/media/radio/dms-decimal

## Schema

Facility files follow the schema: `../schemas/facility.schema.json`

Key fields:
- `facility_id`: Unique ID in format `{iso3}-{slug}-fac`
- `name`: Primary facility name
- `country_iso3`: ISO 3166-1 alpha-3 country code
- `location`: Coordinates and precision
- `types`: Array of facility types
- `commodities`: Array of metals with primary flag
- `status`: Operational status
- `verification`: Confidence and source information

## Development

All scripts log to `.log` files in the scripts directory for debugging.

For detailed logging: Check `research_import.log` or `migration.log`
