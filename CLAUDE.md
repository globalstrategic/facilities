# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is the **Global Mining & Metals Facilities Database** - a comprehensive database of mining, smelting, refining, and processing facilities worldwide. The system manages 8,606 facilities across 129 countries with structured JSON data, entity resolution, and LLM-powered research integration.

**Key Features:**
- Structured facility data organized by ISO3 country codes
- JSON Schema validation for data quality
- Duplicate detection and deduplication pipeline
- Integration with Gemini Deep Research for facility enrichment
- Company entity resolution during research enrichment (via entityidentity library)

## Common Development Commands

### Facility Management CLI

All operations use the unified `facilities.py` CLI:

```bash
# Import facilities from text reports
python scripts/facilities.py import report.txt --country DZ
python scripts/facilities.py import report.txt --country AF --source "Custom Report Name"

# Generate Deep Research prompts for enrichment
python scripts/facilities.py research --generate-prompt --country ZAF --metal platinum --limit 50

# Process Deep Research results
python scripts/facilities.py research --process output.json --country ZAF --metal platinum
python scripts/facilities.py research --batch batch_results.jsonl

# Run tests
python scripts/facilities.py test                    # All tests
python scripts/facilities.py test --suite dedup      # Duplicate detection tests
python scripts/facilities.py test --suite migration  # Migration tests
```

### Testing

```bash
# Run all tests with pytest
pytest

# Run with coverage reporting
pytest -v --cov=scripts --cov-report=html

# Run specific test files
pytest scripts/tests/test_dedup.py
pytest scripts/tests/test_migration_dry_run.py
```

### Development Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Install development dependencies (includes testing tools)
pip install -r requirements-dev.txt

# Set up environment variables (if needed for LLM features)
export ANTHROPIC_API_KEY="your-key"
export GOOGLE_MAPS_API_KEY="your-key"  # For geocoding features
```

## High-Level Architecture

### Data Model

**Facility JSON Structure:** Each facility is stored as an individual JSON file following `schemas/facility.schema.json`:

```
facilities/
├── USA/                              # ISO3 country directories
│   └── usa-east-boulder-fac.json
├── ZAF/
│   └── zaf-afrimat-demaneng-mine-fac.json
└── DZ/
    └── dz-gara-djebilet-mine-fac.json
```

**Facility ID Format:** `{iso3}-{slug}-fac` (e.g., `usa-stillwater-east-fac`)

**Key Fields:**
- `facility_id` - Unique identifier
- `name` - Primary facility name
- `aliases` - Alternative names for duplicate detection
- `country_iso3` - ISO 3166-1 alpha-3 country code
- `location` - Coordinates (lat/lon) with precision indicator
- `types` - Array of facility types (mine, smelter, refinery, plant, etc.)
- `commodities` - Metals/minerals with primary flag
- `status` - Operational status (operating, closed, construction, planned, etc.)
- `owner_links` - Company ownership with percentages and confidence scores
- `operator_link` - Operating company
- `products` - Production streams with capacity data
- `sources` - Data source references (CSV, research reports, URLs)
- `verification` - Status, confidence score, timestamps, and notes

### Import Pipeline

The import pipeline (`scripts/import_from_report.py`) extracts facilities from text reports:

1. **Text Parsing**: Extracts markdown tables from research reports
2. **Normalization**: Standardizes metals (e.g., "Cu" → "copper"), facility types, and operational status
3. **Country Detection**: Auto-detects correct ISO codes (handles both ISO2 and ISO3 input)
4. **Duplicate Detection**: Checks by name, location (~1km radius), and aliases
5. **JSON Generation**: Creates schema-compliant facility files
6. **Logging**: Detailed import reports with statistics in `output/import_logs/`

**Note:** The import pipeline does NOT use entityidentity - it uses simple text normalization. Entity resolution is only applied during the Deep Research enrichment phase.

**Duplicate Detection Logic:**
- Exact facility ID match
- Same name + location within 0.01° (~1km)
- Same name when no coordinates (conservative approach)
- Name matches existing facility's alias

### Deep Research Workflow

Integration with Gemini Deep Research for facility enrichment (`scripts/deep_research_integration.py`):

1. **Prompt Generation**: Creates research prompts with existing facility data
2. **Manual Research**: User submits to Gemini and gets structured JSON back
3. **Result Processing**: Merges research data with existing facilities
4. **Company Resolution**: Links companies using entityidentity library (this is the ONLY place entityidentity is used)
5. **Verification Updates**: Changes status from `csv_imported` to `llm_suggested`
6. **Audit Trail**: Preserves raw research in `output/research_raw/`

**Important:** The entityidentity library is located in a separate repository and must be in PYTHONPATH (e.g., `export PYTHONPATH="../entityidentity:$PYTHONPATH"`)

### Entity Resolution

The system integrates with the `entityidentity` library (separate repo) **only during Deep Research enrichment** for:
- **Company Resolution**: Canonical company IDs, LEI codes, Wikidata QIDs

The import pipeline uses **simple text-based normalization** (not entityidentity) for:
- **Country Detection**: Maps ISO2/ISO3 codes to existing directories using pycountry
- **Metal Normalization**: Hardcoded mappings (e.g., "Cu" → "copper", "REE" → "rare earths")
- **Facility Type Standardization**: Pattern matching (e.g., "Open Pit Mine" → "mine")

## Key Implementation Details

### Country Code Handling

The system handles flexible country code input:
- Auto-detects existing country directories
- Accepts ISO2 (DZ) or ISO3 (DZA) codes
- Maps to correct directory structure
- Located in: `scripts/import_from_report.py`

### Coordinate Parsing

Multiple coordinate formats supported:
- Decimal degrees: `34.267, -5.123`
- Combined format: `34.267, -5.123` or `(34.267, -5.123)`
- Handles missing coordinates gracefully
- Located in: `scripts/import_from_report.py` (parse_coordinates function)

### Verification Status Levels

- `csv_imported` - Initial import from source data
- `llm_suggested` - Enhanced by AI research
- `llm_verified` - Cross-referenced by multiple sources
- `human_verified` - Manually reviewed and confirmed
- `conflicting` - Contradictory information found

### Confidence Scoring

- **0.95**: Very High - Human verified with multiple sources
- **0.85**: High - EntityIdentity match or reliable source
- **0.65**: Moderate - CSV import with good data quality
- **0.40**: Low - Partial data or uncertain matching
- **0.20**: Very Low - Minimal data, needs research

## Important File Locations

### Core Scripts
- `scripts/facilities.py` - Unified CLI entry point
- `scripts/import_from_report.py` - Import pipeline implementation
- `scripts/deep_research_integration.py` - Research integration
- `scripts/migration/migrate_facilities.py` - CSV migration (legacy)

### Data Storage
- `facilities/{COUNTRY}/` - Facility JSON files organized by country
- `schemas/facility.schema.json` - JSON Schema for validation
- `output/import_logs/` - Import reports with timestamps
- `output/research_raw/` - Raw research outputs for audit trail

### Testing
- `scripts/tests/test_dedup.py` - Duplicate detection tests
- `scripts/tests/test_migration_dry_run.py` - Migration validation
- `pytest.ini` - Pytest configuration

### Documentation
- `docs/README_FACILITIES.md` - Complete system documentation
- `docs/DEEP_RESEARCH_WORKFLOW.md` - Research workflow guide
- `docs/FACILITIES_MIGRATION_PLAN.md` - Migration strategy
- `docs/ENTITY_IDENTITY_INTEGRATION.md` - Entity resolution guide

## Data Quality Conventions

1. **Always validate against schema** before committing facility files
2. **Include sources** for all non-trivial data updates
3. **Update verification timestamps** when modifying facilities
4. **Preserve audit trail** - never delete raw research outputs
5. **Use conservative duplicate detection** - better to flag than create duplicates
6. **Normalize metal names** - use canonical forms (e.g., "copper" not "Cu")

## Development Workflow

### Adding New Facilities

```bash
# 1. Save research report to file
cat > report.txt
[Paste report content]
[Press Ctrl+D]

# 2. Import facilities
python scripts/facilities.py import report.txt --country DZ

# 3. Review import report
cat output/import_logs/import_report_DZ_*.json

# 4. Verify generated files
ls -la facilities/DZ/
```

### Enriching Existing Facilities

```bash
# 1. Generate research prompt
python scripts/facilities.py research --generate-prompt --country ZAF --metal platinum

# 2. Submit to Gemini and save response as research_output.json

# 3. Process results
python scripts/facilities.py research --process research_output.json --country ZAF --metal platinum

# 4. Review changes (backups are created automatically)
```

### Running Quality Checks

```bash
# Validate all facilities against schema
find facilities -name "*.json" -exec python -m json.tool {} \; > /dev/null

# Count facilities by status
for status in operating closed construction; do
  echo "$status: $(grep -r "\"status\": \"$status\"" facilities | wc -l)"
done

# Find facilities needing enrichment
grep -r '"status": "unknown"' facilities

# Check duplicate detection is working
python scripts/facilities.py test --suite dedup
```

## System Dependencies

**Required:**
- `pandas>=1.5.0` - Data processing
- `pycountry>=22.0.0` - Country code handling
- `requests>=2.28.0` - HTTP requests

**Optional (for full functionality):**
- `anthropic>=0.18.0` - LLM geocoding features
- `googlemaps>=4.10.0` - Google Maps geocoding
- `whereabouts>=0.1.0` - Location utilities
- `diskcache>=5.6.0` - LLM response caching

**Development:**
- `pytest>=7.4.0` - Testing framework
- `pytest-cov>=4.1.0` - Coverage reporting
- `black>=23.0.0` - Code formatting
- `flake8>=6.0.0` - Linting

## Notes for Future Instances

- The CLI is **NOT installed as a package** - run it directly with `python scripts/facilities.py` (the setup.py references "talloy" which is legacy code)
- The system uses a **mixed country code structure** - some countries use ISO2 (DZ, AF), others use ISO3 (USA, ARG, AUS, ZAF). The import script auto-detects the correct directory.
- **Country code validation** happens via pycountry library, not hardcoded lists
- **Metal normalization** uses hardcoded mappings in import_from_report.py - NOT entityidentity
- The `entityidentity` library is ONLY used in deep_research_integration.py for company resolution during enrichment
- entityidentity is in a separate repo and must be in PYTHONPATH: `export PYTHONPATH="../entityidentity:$PYTHONPATH"`
- Import logs are **never** deleted - they serve as permanent audit trail
- The system creates **automatic backups** (with `.backup_[timestamp].json` extension) before updating existing facilities
- Current facility count: **8,606 facilities** as of the last migration
