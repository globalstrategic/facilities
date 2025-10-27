# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This repository manages a global database of **9,058 mining and processing facilities** across **129 countries**, featuring structured JSON-based architecture with comprehensive entity resolution, company linking, geocoding, and research pipeline integration powered by the **EntityIdentity library**.

**Package Name**: `talloy` (per setup.py)
**Version**: 2.1.1 (Enhanced Table Detection)

## Common Development Commands

### Installation & Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Install development dependencies
pip install -r requirements-dev.txt

# Install package in development mode
pip install -e .

# Run tests
pytest
pytest -v --cov
```

### Import Facilities

```bash
# From markdown files with tables (pipe-separated)
python scripts/import_from_report.py report.md --source "Country Research 2025"

# From CSV files
python scripts/import_from_report.py facilities.csv --country DZ --source "Data Import"

# Auto-detect country from filename
python scripts/import_from_report.py bulgaria.txt
python scripts/import_from_report.py albania_mines.csv

# Enhanced import with entity resolution (default)
python scripts/import_from_report.py report.txt --country DZ --source "Algeria Report 2025"

# From stdin
cat report.txt | python scripts/import_from_report.py --country DZ

# Supported formats:
# - Markdown tables (pipe-separated: | header | header |)
# - CSV files (comma-separated)
# - Tab-separated tables
# - Narrative text with facility mentions
```

### Unified CLI

```bash
# Main CLI entry point (scripts/facilities.py)
python scripts/facilities.py <command> [options]

# Import facilities (via CLI wrapper)
python scripts/facilities.py import report.txt --country DZ
# Note: For advanced features, use import_from_report.py directly

# Sync with EntityIdentity parquet format
python scripts/facilities.py sync --export                    # Export to parquet
python scripts/facilities.py sync --import facilities.parquet  # Import from parquet
python scripts/facilities.py sync --status                     # Database status

# Test entity resolution
python scripts/facilities.py resolve country "Algeria"
python scripts/facilities.py resolve metal "Cu"
python scripts/facilities.py resolve company "BHP"

# Research integration (Gemini Deep Research)
python scripts/facilities.py research --generate-prompt --country ZAF --metal platinum
python scripts/facilities.py research --process output.json --country ZAF

# Run tests
python scripts/facilities.py test
python scripts/facilities.py test --suite dedup
```

### Company Enrichment (Phase 2)

```bash
# Enrich facilities with company links using CompanyResolver
python scripts/enrich_companies.py                      # All countries
python scripts/enrich_companies.py --country IND        # Specific country
python scripts/enrich_companies.py --dry-run            # Preview without saving
python scripts/enrich_companies.py --min-confidence 0.75  # Set confidence threshold

# Outputs written to: tables/facilities/facility_company_relationships.parquet
```

### Backfill & Enrichment (NEW - v2.1)

```bash
# Install geocoding dependencies
pip install geopy

# Backfill everything for a country
python scripts/backfill.py all --country ARE --interactive

# Backfill everything for ALL countries (use with caution!)
python scripts/backfill.py all --all --dry-run  # Preview first
python scripts/backfill.py all --all            # Run for real

# Or run operations individually
python scripts/backfill.py geocode --country ARE --interactive
python scripts/backfill.py geocode --all --dry-run  # All countries
python scripts/backfill.py companies --country IND --profile moderate
python scripts/backfill.py companies --all --profile moderate
python scripts/backfill.py metals --all

# Batch processing (multiple countries)
python scripts/backfill.py geocode --countries ARE,IND,CHN

# Dry run (preview changes)
python scripts/backfill.py all --country ARE --dry-run
```

**What each operation does:**
- **geocode**: Adds missing coordinates using industrial zones → Nominatim API → interactive prompting
- **companies**: Resolves `company_mentions[]` to canonical company IDs (Phase 2)
- **metals**: Adds chemical formulas and categories to commodities
- **all**: Runs all three operations in sequence

**Geocoding strategies:**
1. Industrial zone database (UAE zones pre-configured)
2. Nominatim API (OpenStreetMap) with rate limiting
3. Location extraction from facility names
4. Interactive prompting (if `--interactive` flag enabled)

**Success rates:**
- Industrial zones: ~5-10%
- Nominatim API: ~10-15%
- Total automated: ~15-25%
- Remaining: Need interactive mode or better data

### Deduplication

**Automatic (during import)**: The import pipeline automatically prevents duplicates using `check_duplicate()` in `import_from_report.py`.

**Manual (batch cleanup)**: For cleaning up existing duplicates:

```bash
# Preview duplicates (dry run - recommended first step)
python scripts/tools/deduplicate_facilities.py --country ZAF --dry-run

# Clean up duplicates in a specific country
python scripts/tools/deduplicate_facilities.py --country ZAF

# Clean up all countries
python scripts/tools/deduplicate_facilities.py --all

# What it does:
# - Finds coordinate-based duplicates (two-tier matching: 0.01°/0.1° with name similarity)
# - Scores facilities by data completeness
# - Merges aliases, sources, commodities, company mentions
# - Deletes inferior duplicates
# - Tracks merge history in verification notes
# - Standalone utility script for batch cleanup only
```

## Code Architecture

### Directory Structure

```
facilities/
├── facilities/           # 8,606 facility JSONs organized by ISO3 country
│   ├── USA/ (1,623)     # United States
│   ├── CHN/ (1,837)     # China
│   ├── AUS/ (578)       # Australia
│   ├── IND/ (424)       # India
│   └── ... (125+ more)
│
├── scripts/             # Core import and management scripts
│   ├── facilities.py                    # Unified CLI with subcommands
│   ├── import_from_report.py            # Main import pipeline (with entity resolution)
│   ├── backfill.py                      # Unified backfill system (geocoding, companies, metals, mentions)
│   ├── enrich_companies.py              # Phase 2: Batch company enrichment
│   ├── deep_research_integration.py     # Gemini Deep Research integration
│   │
│   ├── tools/                           # Standalone utility tools
│   │   ├── audit_facilities.py          # Data quality checks
│   │   ├── deduplicate_facilities.py    # Batch deduplication utility
│   │   ├── verify_backfill.py           # Verify backfill results
│   │   ├── geocode_facilities.py        # Standalone geocoding utility
│   │   └── legacy/                      # Archived one-time migration scripts
│   │       ├── full_migration.py        # Legacy CSV → JSON migration
│   │       └── migrate_legacy_fields.py # Schema field migration
│   │
│   └── utils/                           # Entity resolution utilities
│       ├── company_resolver.py          # CompanyResolver with quality gates
│       ├── id_utils.py                  # Canonical ID mapping
│       ├── paths.py                     # Shared path configuration
│       ├── country_utils.py             # Country code normalization
│       ├── ownership_parser.py          # Parse ownership percentages
│       └── facility_sync.py             # Parquet export/import
│
├── schemas/
│   └── facility.schema.json             # JSON Schema v2.0.0
│
├── tables/                              # Parquet output (Phase 2)
│   └── facilities/
│       └── facility_company_relationships.parquet  # Canonical relationships
│
├── README.md                            # Complete documentation (all-in-one)
├── CLAUDE.md                            # Developer guide (this file)
├── CHANGELOG.md                         # Version history and release notes


│
├── output/                              # Generated outputs (gitignored)
│   ├── import_logs/                     # Import reports with statistics
│   ├── research_raw/                    # Gemini Deep Research outputs
│   └── entityidentity_export/           # Parquet exports
│

 for CompanyResolver

```

### Key Architectural Patterns

#### 1. Facility JSON Schema (v2.0.0)

Facilities are stored as individual JSON files following `schemas/facility.schema.json`:

```json
{
  "facility_id": "zaf-rustenburg-karee-fac",
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
    }
  ],
  "status": "operating",
  "company_mentions": [
    {
      "name": "Sibanye-Stillwater",
      "role": "operator",
      "confidence": 0.85,
      "source": "gemini_research"
    }
  ],
  "owner_links": [],
  "operator_link": null,
  "products": [],
  "sources": [],
  "verification": {
    "status": "llm_suggested",
    "confidence": 0.75,
    "last_checked": "2025-10-14T10:00:00",
    "checked_by": "import_pipeline"
  }
}
```

**Key Schema Points:**
- **`facility_id`**: Pattern `{iso3}-{slug}-fac` (e.g., `usa-stillwater-east-fac`)
- **`company_mentions`**: Array of raw company mentions from sources (Phase 1)
- **`owner_links` / `operator_link`**: Resolved canonical company links (Phase 2)
- **`commodities`**: Includes optional `chemical_formula` and `category` fields
- **`verification.status`**: `csv_imported`, `llm_suggested`, `llm_verified`, `human_verified`, `conflicting`

#### 2. Two-Phase Company Resolution Architecture

**Phase 1: Extraction** (import_from_report.py)
- Extract company mentions from source data
- Store in `company_mentions[]` array with role, source, confidence
- Does NOT resolve to canonical IDs yet

**Phase 2: Resolution** (enrich_companies.py)
- Batch process all facilities with `company_mentions`
- Use `CompanyResolver` with quality gates (strict/moderate/permissive profiles)
- Write resolved relationships to `facility_company_relationships.parquet`
- Separate relationships from facility JSONs for flexibility

**Key Design Decision:** Relationships stored in parquet, NOT in facility JSON. This allows:
- Batch reprocessing without modifying 8,606 JSON files
- Quality gate tuning without data migration
- Multiple relationship sources/versions
- Efficient querying via pandas/SQL

#### 3. Entity Resolution Integration

The system integrates with the **EntityIdentity library** (separate repo at `../entityidentity/`) for:

**Country Resolution:**
- Auto-detect country from filename or text
- Normalize to ISO3 codes (DZA, USA, ZAF)
- Handle both ISO2 and ISO3 directory names

**Metal Normalization:**
- `metal_identifier()` for chemical formulas (Cu, Fe2O3, etc.)
- Category classification (base_metal, precious_metal, rare_earth)
- Handles elements, alloys, and compounds

**Company Resolution:**
- Use `CompanyResolver` (wraps EntityIdentity's company matching)
- Match company names to canonical LEI-based IDs
- Phase 2: Multi-strategy matching with quality gates
- Database: 3,687+ companies with LEI codes and Wikidata links

**Facility Matching:**
- Multi-strategy duplicate detection (name, location, alias, company+commodity)
- Cross-reference with EntityIdentity facilities parquet
- Vectorized distance calculations for performance

#### 4. Import Pipeline Flow

```
TEXT INPUT (markdown tables, CSV, tab-separated, stdin)
  ↓
TABLE EXTRACTION (extract_markdown_tables, parse_csv_file)
  ├─ Markdown tables: pipe-separated (| ... |)
  ├─ CSV: comma-separated with headers
  ├─ Tab-separated: TSV format
  └─ Enhanced validation: plural forms, location indicators
  ↓
TABLE VALIDATION (is_facility_table - v2.1.1)
  ├─ Checks for 3+ indicator keywords in headers
  ├─ Recognizes: facility, mine, name, operator, owner
  ├─ Recognizes: commodity/commodities, metal/metals (plurals)
  ├─ Recognizes: location, province, region, site
  └─ Allows multiple indicators per header (counts all)
  ↓
ENTITY RESOLUTION (automatic)
  ├─ Country auto-detection from filename or content
  ├─ Metal normalization with chemical formulas
  └─ Company mention extraction (Phase 1)
  ↓
FACILITY CREATION
  ↓
DUPLICATE DETECTION (multi-strategy)
  ↓
VALIDATION (schema validation, confidence scoring)
  ↓
OUTPUT
  ├─ Facility JSON files
  ├─ Import log with statistics
  └─ company_mentions ready for Phase 2 enrichment
```

#### 5. Company Resolution Pipeline (Phase 2)

```
FACILITY JSONs with company_mentions
  ↓
EXTRACT MENTIONS (enrich_companies.py)
  ├─ Parse company_mentions[] arrays
  ├─ Handle operator, owner, majority_owner, minority_owner roles
  └─ Extract LEI, registry, country hints
  ↓
BATCH RESOLUTION (CompanyResolver)
  ├─ Registry-first matching (LEI, Wikidata)
  ├─ Name matching with fuzzy logic
  ├─ Country/geography filtering
  └─ Quality gates (strict/moderate/permissive)
  ↓
GATE APPLICATION
  ├─ auto_accept: High confidence, write immediately
  ├─ review: Medium confidence, needs human review
  └─ pending: No match found, track for later
  ↓
OUTPUT
  ├─ facility_company_relationships.parquet
  ├─ Pending companies tracked (if PendingCompanyTracker available)
  └─ Import statistics (resolutions, review queue, pending)
```

## Important Development Patterns

### 1. Table Format Requirements (v2.1.1)

**Header Validation Logic:**

The `is_facility_table()` function validates tables by checking for **3 or more indicator keyword matches** across all headers:

**Recognized Keywords:**
- Facility identifiers: `facility`, `mine`, `name`, `deposit`, `project`, `site`
- People/Companies: `operator`, `owner`
- Location: `location`, `province`, `region`, `latitude`, `longitude`
- Commodities: `commodity`, `commodities`, `metal`, `metals` (plural forms supported)

**Good Header Examples:**
```csv
✓ Facility Name, Operator, Location, Primary Metal, Status
  → Matches: facility, name, operator, location, metal (5 matches)

✓ Mine Name, Owner, Province, Commodity, Type
  → Matches: mine, name, owner, province, commodity (5 matches)

✓ Site, Company, Region, Metals, Latitude
  → Matches: site, region, metals, latitude (4 matches)

✓ Facility Name(s), Corporate Owner/Group, Location (Province), Primary Commodities
  → Matches: facility, name, owner, location, province, commodities (6 matches)
```

**Bad Header Examples:**
```csv
✗ Name, Company, Area, Product, Active
  → Matches: name (1 match - needs 3+)

✗ Title, Organization, Place, Material, Open
  → Matches: none (0 matches - needs 3+)
```

**Troubleshooting Table Import Issues:**

If your table isn't being detected:

1. **Check headers have 3+ indicator keywords:**
   ```bash
   # Your headers should contain words like:
   # facility, mine, name, operator, owner, location, commodity, metal
   ```

2. **Use recognized plural forms:**
   - ✓ "Commodities" works (plural recognized in v2.1.1)
   - ✓ "Metals" works (plural recognized)
   - ✗ "Mineral" doesn't match "metal" (use exact keywords)

3. **Combine multiple indicators in one header:**
   - ✓ "Facility Name" = 2 matches (facility + name)
   - ✓ "Mine Location" = 2 matches (mine + location)
   - ✓ "Primary Metal" = 1 match (metal)

4. **Common fixes:**
   ```csv
   # Change generic headers to specific ones:
   Company        → Operator
   Area           → Location
   Product        → Commodity
   Type           → Facility Type (adds both 'facility' + 'site/mine')
   ```

**Example Fix:**

Before (fails validation - only 1 match):
```csv
Name, Company, Area, Product, Status
```

After (passes validation - 5 matches):
```csv
Facility Name, Operator, Location, Primary Metal, Status
```

### 2. Country Code Handling

- **Storage**: Facilities organized by country in `facilities/{ISO2_OR_ISO3}/`
- **Schema**: `country_iso3` field always uses 3-letter codes (DZA, USA, ZAF)
- **Directory**: Mix of ISO2 (DZ, AF) and ISO3 (USA, ZAF) - both supported
- **Utilities**: Use `scripts/utils/country_utils.py` for normalization

**Example:**
```python
from scripts.utils.country_utils import normalize_country_to_iso3, iso3_to_country_name

iso3 = normalize_country_to_iso3("Algeria")  # → "DZA"
name = iso3_to_country_name("DZA")          # → "Algeria"
```

### 2. Company Resolution Pattern (Phase 2)

**Use CompanyResolver for batch resolution:**

```python
from scripts.utils.company_resolver import CompanyResolver

# Initialize with config profile (strict, moderate, permissive)
resolver = CompanyResolver.from_config(profile="strict")

# Batch resolve mentions
mentions = [
    {"name": "BHP", "role": "operator", "lei": None},
    {"name": "Rio Tinto", "role": "owner", "percentage": 60.0}
]

accepted, review, pending = resolver.resolve_mentions(mentions, facility=facility_dict)

# accepted: High confidence, auto-write
# review: Medium confidence, needs human review
# pending: No match found
```

**Key points:**
- Always use batch `resolve_mentions()` instead of single `resolve_name()`
- Quality gates determine auto-accept vs review vs pending
- Canonical IDs resolved via EntityIdentity
- Relationships written to parquet, NOT facility JSON

### 3. Facility ID Generation

**Pattern:** `{iso3}-{slugified-name}-fac`

```python
def slugify(text: str) -> str:
    """Convert text to URL-safe slug."""
    text = text.lower().strip()
    text = re.sub(r'\([^)]*\)', '', text)  # Remove parentheticals
    text = re.sub(r'[^a-z0-9]+', '-', text)
    return text.strip('-')

# Example:
# "Karee Mine (Rustenburg)" → "zaf-karee-mine-fac"
```

### 4. Confidence Scoring

**Base confidence levels:**
- 0.95: Very High - Human verified
- 0.85: High - EntityIdentity exact match
- 0.75: Moderate-High - LLM research with entity resolution
- 0.65: Moderate - CSV import with good data
- 0.40: Low - Partial data
- 0.20: Very Low - Needs research

**Confidence boosting (automatic):**
- Company operator resolved: +0.10
- Multiple commodities with formulas: +0.05
- Coordinates with site-level precision: +0.05

### 5. Duplicate Detection Strategy

The system uses a **4-priority matching strategy** in `check_duplicate()` function (`scripts/import_from_report.py:778`):

#### Priority 1: Coordinate-Based Matching (Primary)

**Two-tier system for precision:**

- **Tier 1**: Very close coordinates (0.01° ~1km)
  - Requires: Name similarity >60% OR shorter name contained in longer name
  - Use case: Exact same location with slight name variations
  - Example: "Two Rivers" vs "Two Rivers Platinum Mine" at identical coords

- **Tier 2**: Close coordinates (0.1° ~11km)
  - Requires: Name similarity >85% OR shorter name contained in longer name
  - Use case: Nearly identical mines with very similar names
  - Prevents false positives from nearby but distinct facilities

**Implementation:**
```python
# Check coordinates first (most reliable)
lat_diff = abs(lat - existing_lat)
lon_diff = abs(lon - existing_lon)

name_lower = name.lower()
existing_name_lower = existing_fac['name'].lower()
name_similarity = SequenceMatcher(None, name_lower, existing_name_lower).ratio()

# Check containment (e.g., "Two Rivers" in "Two Rivers Platinum Mine")
shorter = name_lower if len(name_lower) < len(existing_name_lower) else existing_name_lower
longer = existing_name_lower if len(name_lower) < len(existing_name_lower) else name_lower
contains_match = shorter in longer

# Two-tier matching
tier1_match = (lat_diff < 0.01 and lon_diff < 0.01) and (name_similarity > 0.6 or contains_match)
tier2_match = (lat_diff < 0.1 and lon_diff < 0.1) and (name_similarity > 0.85 or contains_match)

if tier1_match or tier2_match:
    return existing_id  # Duplicate detected
```

#### Priority 2: Exact Name Match

- Case-insensitive exact name comparison
- If both have coordinates, verify they're within 0.01° (~1km)
- If no coordinates, assume duplicate by name alone

#### Priority 3: Fuzzy Name Match

**Uses two methods:**

1. **String similarity**: SequenceMatcher ratio >85%
2. **Word overlap**: >80% of words match

```python
# Word overlap calculation
words1 = set(name_lower.split())
words2 = set(existing_name_lower.split())
word_overlap = len(words1 & words2) / min(len(words1), len(words2))

# Match if high similarity OR high word overlap
if name_similarity > 0.85 or word_overlap > 0.8:
    return existing_id  # Duplicate detected
```

**Examples:**
- "Two Rivers Mine" vs "Two Rivers Platinum Mine": 100% word overlap (3/3 words match)
- "New Denmark" vs "New Denmark Colliery": 100% word overlap (2/2 words match)
- "South Deep" vs "South Deep Gold Mine": 100% word overlap (2/2 words match)

#### Priority 4: Alias Match

- Checks if name appears in existing facility's `aliases[]` array
- Case-insensitive matching

#### Real-World Performance

**South Africa case study:**
- **Before**: 779 facilities (168 coordinate-based duplicate pairs)
- **Detected**: 147 duplicate groups
- **After cleanup**: 628 facilities (151 removed = 19.4% reduction)

**Example duplicate group:**
```
Group: Two Rivers Platinum Mine
  [38.0] zaf-two-rivers-platinum-mine-fac: Two Rivers Platinum Mine
  [32.0] zaf-two-rivers-fac: Two Rivers
  [29.0] zaf-two-rivers-mine-fac: Two Rivers Mine

Match logic:
  - All three at coords (-24.893, 30.124)
  - "Two Rivers" contained in "Two Rivers Platinum Mine"
  - "Two Rivers Mine" has 100% word overlap with "Two Rivers Platinum Mine"

Result: Merged into single facility with all names as aliases
```

### 6. Working with Parquet Relationships (Phase 2)

**Read relationships:**
```python
import pandas as pd

# Load facility-company relationships
df = pd.read_parquet("tables/facilities/facility_company_relationships.parquet")

# Filter by facility
facility_rels = df[df['facility_id'] == 'zaf-karee-mine-fac']

# Filter by gate (quality)
high_quality = df[df['gate'] == 'auto_accept']
needs_review = df[df['gate'] == 'review']
```

**Relationship schema:**
- `relationship_id`: Unique UUID
- `facility_id`: Links to facility JSON
- `company_id`: Canonical company ID
- `role`: operator, owner, majority_owner, minority_owner
- `confidence`: 0.0-1.0 score
- `gate`: auto_accept, review, pending
- `provenance`: Source of the mention
- `evidence`: Supporting evidence text
- `gates_applied`: Dict with penalties applied

### 7. Testing Patterns

**Location:** Tests are in `scripts/tests/` directory

**Run tests via CLI:**
```bash
python scripts/facilities.py test              # Run all tests
python scripts/facilities.py test --suite dedup  # Run dedup tests only
```

**Run tests with pytest directly:**
```bash
pytest scripts/tests/ -v
pytest scripts/tests/test_import_enhanced.py -v
```

## Common Workflows

### Adding a New Country

1. Create country directory (use ISO3 if possible):
   ```bash
   mkdir facilities/DZA  # Algeria
   ```

2. Import facilities:
   ```bash
   python scripts/import_from_report.py algeria_report.txt --country DZA
   ```

3. Enrich with company links:
   ```bash
   python scripts/enrich_companies.py --country DZA
   ```

4. Verify:
   ```bash
   python scripts/facilities.py sync --status
   ```

### Enriching Existing Facilities with Companies (Phase 2)

1. Ensure facilities have `company_mentions[]` arrays (use backfill script if needed):
   ```bash
   python scripts/backfill.py mentions --country IND
   ```

2. Run company enrichment:
   ```bash
   python scripts/enrich_companies.py --country IND
   ```

3. Review results:
   - Check `tables/facilities/facility_company_relationships.parquet`
   - Review statistics printed at end
   - Check pending companies (if tracker available)

4. Handle review queue:
   - Items in "review" gate need human validation
   - Could write review pack to file for batch processing

### Deduplicating Facilities

Complete workflow for cleaning up duplicate facilities:

#### Step 1: Preview Duplicates (Dry Run)

**Always start with a dry run to review what will be changed:**

```bash
python scripts/tools/deduplicate_facilities.py --country ZAF --dry-run
```

**Review the output:**
- Check duplicate groups are legitimate (not nearby but distinct facilities)
- Verify facility scores make sense (best facility has highest score)
- Note which facilities will be kept vs deleted
- Look for unexpected groupings

**Example output:**
```
Processing ZAF...
  Loaded 779 facilities
  Found 147 duplicate groups

  Group 1 (2 facilities):
    [38.0] zaf-two-rivers-platinum-mine-fac: Two Rivers Platinum Mine
    [32.0] zaf-two-rivers-fac: Two Rivers
    → Keeping: zaf-two-rivers-platinum-mine-fac

=== SUMMARY ===
ZAF: 147 groups, 0 removed, 147 kept (DRY RUN)
```

#### Step 2: Run Deduplication (Live Mode)

**Once satisfied with preview, run without --dry-run:**

```bash
python scripts/tools/deduplicate_facilities.py --country ZAF
```

**What happens:**
1. Identifies duplicate groups using 4-priority matching
2. Scores each facility by data completeness
3. Selects best facility to keep (highest score)
4. Merges data from duplicates:
   - Aliases: All duplicate names added as aliases
   - Sources: All import sources combined
   - Commodities: Best version of each commodity (prefer with formulas)
   - Company mentions: Highest confidence mention per company
5. Updates verification notes with merge history
6. Deletes duplicate JSON files

#### Step 3: Verify Results

```bash
# Check facility count
find facilities/ZAF -name "*.json" | wc -l

# Verify a merged facility
cat facilities/ZAF/zaf-two-rivers-platinum-mine-fac.json
```

**Verify merged data:**
- Aliases include all duplicate names
- Sources list includes all imports
- Commodities are most complete version
- Verification notes show merge history

**Example merged facility:**
```json
{
  "facility_id": "zaf-two-rivers-platinum-mine-fac",
  "name": "Two Rivers Platinum Mine",
  "aliases": ["Two Rivers", "Two Rivers Mine", "Tweefontein"],
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

#### Step 4: Handle Edge Cases

**If duplicates weren't detected:**
- Check if coordinates are too far apart (>0.1°)
- Verify name similarity is sufficient (needs >85% or >80% word overlap)
- Consider adding name to aliases manually if they're the same facility

**If false positives (different facilities grouped):**
- This is rare with current thresholds
- Manually un-merge by restoring deleted files from git
- Report the case to refine detection thresholds

**If you want to process all countries:**
```bash
# Preview all countries
python scripts/tools/deduplicate_facilities.py --all --dry-run

# Process all countries (use with caution!)
python scripts/tools/deduplicate_facilities.py --all
```

#### Expected Results by Country

**Typical deduplication impact:**
- Large countries (500+ facilities): 15-20% reduction
- Medium countries (100-500 facilities): 10-15% reduction
- Small countries (<100 facilities): 5-10% reduction

**South Africa example:**
- Before: 779 facilities
- Duplicates found: 147 groups
- After: 628 facilities
- Reduction: 19.4%

### Debugging Import Issues

1. **Country not auto-detected:**
   ```bash
   python scripts/facilities.py resolve country "input_text"
   # Use explicit --country flag if auto-detect fails
   ```

2. **Company resolution failing:**
   ```bash
   python scripts/facilities.py resolve company "BHP"
   # Check if company exists in EntityIdentity database
   ```

3. **Duplicate facilities:**
   - Check import logs in `output/import_logs/`
   - Review duplicate detection strategy
   - Manually merge if needed

4. **Schema validation errors:**
   - Ensure all required fields present
   - Check `chemical_formula` pattern (if provided)
   - Verify `category` enum values
   - Compare against `schemas/facility.schema.json`

### Exporting to EntityIdentity Format

```bash
# Export all facilities to parquet
python scripts/facilities.py sync --export

# Custom output location
python scripts/facilities.py sync --export --output /path/to/output

# Import from EntityIdentity parquet
python scripts/facilities.py sync --import facilities.parquet --overwrite
```

## Data Quality & Maintenance

### Audit Facilities

```bash
# Run audit checks
python scripts/tools/audit_facilities.py

# Find facilities without coordinates
grep -r '"lat": null' facilities/ | wc -l

# Find facilities with low confidence
find facilities -name "*.json" -exec grep -l '"confidence": 0\.[0-4]' {} \;

# Count facilities by status
for status in operating closed suspended; do
  echo "$status: $(grep -r "\"status\": \"$status\"" facilities | wc -l)"
done
```

### Quality Gates for Company Resolution

Hardcoded in `scripts/utils/company_resolver.py`:

**Profiles:**
- **strict**: High precision, lower recall (min_confidence 0.80)
- **moderate**: Balanced (min_confidence 0.70)
- **permissive**: High recall, lower precision (min_confidence 0.60)

**Penalties applied:**
- Country mismatch: -0.15
- No registry ID (LEI/Wikidata): -0.10
- Name length difference >20 chars: -0.10

## External Dependencies

### Required

- **EntityIdentity library**: Must be accessible (either installed or in `../entityidentity/`)
  - Company resolution (via `CompanyResolver` wrapper)
  - Metal normalization (`metal_identifier`)
  - Country detection (`country_identifier`)

### Optional

- **PendingCompanyTracker**: From EntityIdentity, tracks unresolved companies
- **Geocoding services**: For reverse geocoding coordinates to countries
- **Anthropic API**: For LLM-based geocoding fallback

### Environment Setup

```bash
# Ensure EntityIdentity is accessible
export PYTHONPATH="/Users/willb/Github/GSMC/entityidentity:$PYTHONPATH"

# Or install as package
pip install git+https://github.com/globalstrategic/entityidentity.git

# Verify import works
python -c "from entityidentity import country_identifier; print(country_identifier('Algeria'))"
```

## Statistics

Current database (as of 2025-10-21):

- **Total Facilities**: 9,058
- **Countries**: 129 (ISO3 codes)
- **Top Countries**: CHN (1,837), USA (1,623), ZAF (628), AUS (613), IDN (461), IND (424)
- **Metals/Commodities**: 50+ types
- **With Coordinates**: ~99% (8,970+ facilities)
- **Operating Facilities**: ~45%
- **Average Confidence**: 0.64

**Recent Improvements (v2.1.0):**
- **Geocoding System**: Multi-strategy automated geocoding (15-25% success rate)
- **Backfill System**: Unified enrichment for coordinates, companies, metals
- **Deep Research Import**: Added 298 facilities from 12 countries
- **Duplicate Detection**: 4-priority matching system (coordinate + name based)
- **ZAF Deduplication**: Reduced from 779 → 628 facilities (19.4% reduction)

## Known Issues & Gotchas

1. **Directory inconsistency**: Mix of ISO2 (DZ, AF) and ISO3 (USA, ZAF) country directories
   - Code handles both, but prefer ISO3 for new countries

2. **Company mentions vs links**:
   - Phase 1: Extract to `company_mentions[]` (raw strings)
   - Phase 2: Resolve to `owner_links[]` / `operator_link` (canonical IDs)
   - Don't confuse the two!

3. **Parquet loading overhead**:
   - EntityIdentity loads ~50MB parquet on first use
   - First import will be slower (~2-3s startup)
   - Subsequent imports use cached data

4. **Gate tuning**:
   - Too strict: Miss valid companies (low recall)
   - Too permissive: Accept wrong companies (low precision)
   - Review queue for borderline cases

5. **Relationship storage**:
   - Relationships in parquet, NOT in facility JSON
   - Query parquet to get facility-company links
   - Don't modify facility JSONs for company links (Phase 2 design)

6. **EntityIdentity API changes** (Fixed in v2.1.1):
   - EntityIdentity changed `metal_identifier()` return key from `'formula'` to `'chemical_formula'`
   - **Fix**: Use `result.get('chemical_formula')` in `scripts/import_from_report.py:911`
   - Symptom: "Metals with formulas: 0" despite entityidentity being installed
   - Test: Import should show "Metals with formulas: N" where N > 0 for data with metals
   - Verified with neodymium test: 100% success (33/33 facilities enriched with Nd formulas)

## Performance Characteristics

- **Import**: ~50 facilities/second (standard), ~10 facilities/second (with entity resolution)
- **Company enrichment**: ~5-10 facilities/second (batch resolution)
- **Database loading**: 8,606 facilities in ~0.5s
- **Parquet export**: 8,606 facilities in <5s
- **Memory usage**: ~150MB (with all resolvers loaded)

## Scripts Reference

### Production Scripts

#### 1. import_from_report.py (1,393 lines)
**Main import pipeline with Phase 1 extraction**

```bash
# Standard import with auto entity resolution
python scripts/import_from_report.py report.txt --country DZ

# Auto-detect country from filename
python scripts/import_from_report.py albania.txt

# Multi-country CSV (per-row detection)
python scripts/import_from_report.py gt/Mines.csv --source "Mines Database"

# From stdin
cat report.txt | python scripts/import_from_report.py --country DZ
```

**What it does:**
- Parses CSV, markdown tables, and text formats
- Extracts company mentions (Phase 1) - NO resolution yet
- Metal normalization with chemical formulas via `metal_identifier()`
- Per-row country detection for multi-country CSVs
- Duplicate detection (name + location matching)
- Writes facility JSONs with `company_mentions[]` array

**Output:** Facility JSONs ready for Phase 2 enrichment

---

#### 2. backfill.py (NEW - v2.1 - 795 lines)
**Unified backfill system for enriching existing facilities**

```bash
# Backfill geocoding (add coordinates)
python scripts/backfill.py geocode --country ARE
python scripts/backfill.py geocode --country ARE --interactive

# Backfill company resolution
python scripts/backfill.py companies --country IND --profile moderate

# Backfill metal normalization (add formulas/categories)
python scripts/backfill.py metals --all

# Extract company mentions from Mines.csv
python scripts/backfill.py mentions --country BRA
python scripts/backfill.py mentions --all --force

# Backfill everything at once
python scripts/backfill.py all --country ARE --interactive

# Batch processing (multiple countries)
python scripts/backfill.py geocode --countries ARE,IND,CHN

# Dry run
python scripts/backfill.py all --country ARE --dry-run
```

**What it does:**
- **geocode**: Adds missing coordinates using industrial zone DB + Nominatim API
- **companies**: Resolves `company_mentions[]` to canonical IDs with quality gates
- **metals**: Adds chemical formulas and categories to commodities via `metal_identifier()`
- **mentions**: Extracts company mentions from Mines.csv "Group Names" field
- **all**: Runs all enrichment operations in sequence

**Features:**
- Multi-strategy geocoding (industrial zones → Nominatim → interactive)
- Batch processing support (single/multiple countries)
- Detailed statistics tracking for each operation
- Dry-run mode for safe preview
- Preserves existing data (only adds missing fields)
- Updates `verification.last_checked` and `verification.notes`

**Output:** Updated facility JSONs with enriched data

---

#### 3. geocode_facilities.py (NEW - v2.1 - 268 lines)
**Standalone geocoding utility**

```bash
# Geocode all facilities in a country
python scripts/tools/geocode_facilities.py --country ARE

# Interactive mode (prompts for failures)
python scripts/tools/geocode_facilities.py --country ARE --interactive

# Dry run
python scripts/tools/geocode_facilities.py --country ARE --dry-run

# Geocode single facility
python scripts/tools/geocode_facilities.py --facility-id are-union-cement-company-fac

# Offline mode (industrial zones only)
python scripts/tools/geocode_facilities.py --country ARE --no-nominatim
```

**What it does:**
- Geocodes facilities with missing coordinates
- Uses `scripts/utils/geocoding.py` service
- Multiple fallback strategies (zones → Nominatim → interactive)
- Updates facility JSONs with coordinates and precision
- Tracks geocoding source and confidence

**Output:** Updated facility JSONs with coordinates

---

#### 4. enrich_companies.py (470 lines)
**Phase 2 company resolution and relationship creation**

```bash
# Enrich all facilities
python scripts/enrich_companies.py

# Enrich specific country
python scripts/enrich_companies.py --country IND

# Preview without saving
python scripts/enrich_companies.py --dry-run

# Set confidence threshold
python scripts/enrich_companies.py --min-confidence 0.75
```

**What it does:**
- Batch company resolution using `CompanyResolver`
- Quality gates (auto_accept / review / pending)
- Writes relationships to `facility_company_relationships.parquet`
- Does NOT modify facility JSONs (Phase 2 design)

**Output:** `tables/facilities/facility_company_relationships.parquet`

---

#### 5. deep_research_integration.py (606 lines)
**Gemini Deep Research integration**

```bash
# Generate research prompt
python scripts/deep_research_integration.py \
    --generate-prompt --country ZAF --metal platinum --limit 50

# Process research output
python scripts/deep_research_integration.py \
    --process research_output.json --country ZAF --metal platinum

# Batch processing
python scripts/deep_research_integration.py \
    --batch research_batch.jsonl
```

**What it does:**
- Generates research prompts from facility data
- Processes Gemini research outputs (JSON/JSONL)
- Resolves companies using `CompanyResolver`
- Updates facility JSONs with status, owners, operators, products

**Output:** Updated facility JSONs + raw research backups

---

#### 6. deduplicate_facilities.py (342 lines)
**Batch cleanup utility for existing duplicates**

**Purpose**: Standalone utility for one-time or periodic batch cleanup of duplicates. NOT part of automatic import workflow.

```bash
# Preview duplicates (dry run - always do this first)
python scripts/tools/deduplicate_facilities.py --country ZAF --dry-run

# Clean up duplicates in South Africa
python scripts/tools/deduplicate_facilities.py --country ZAF

# Clean up all countries (use with caution)
python scripts/tools/deduplicate_facilities.py --all
```

**What it does:**
- Finds duplicate groups using same 4-priority logic as import
- Scores facilities by data completeness
- Merges data from duplicates into best facility
- Deletes inferior duplicate files
- **Use case**: Clean up duplicates that existed before improved detection was added

**vs. Automatic Detection:**
- `import_from_report.py` - Prevents duplicates during import (automatic)
- `deduplicate_facilities.py` - Cleans up existing duplicates (manual, one-time)

**Output:**
- Modified facilities with merged data
- Deleted duplicate JSON files
- Console report with statistics

**Performance:**
- South Africa: 779 → 628 facilities (151 removed, 19.4% reduction)
- Typical: 10-20% reduction on first run, minimal thereafter

**Safety:**
- Always use `--dry-run` first
- Country-by-country recommended
- Full merge history in verification notes
- Deleted files recoverable from git

---

#### 5. facilities.py (456 lines)
**Unified CLI wrapper**

```bash
# Import (simple wrapper)
python scripts/facilities.py import report.txt --country DZ

# Test entity resolution
python scripts/facilities.py resolve country "Algeria"
python scripts/facilities.py resolve metal "Cu"
python scripts/facilities.py resolve company "BHP"

# Sync/export
python scripts/facilities.py sync --export
python scripts/facilities.py sync --status

# Research
python scripts/facilities.py research --generate-prompt --country ZAF --metal platinum

# Tests
python scripts/facilities.py test
```

**Note:** Limited compared to calling scripts directly. For advanced features, use scripts directly.

---

### Utility Scripts

#### 5. audit_facilities.py (312 lines)
Data quality checks and reporting

```bash
python scripts/tools/audit_facilities.py
```

---

#### 6. verify_backfill.py (185 lines)
Verify backfill results

```bash
python scripts/tools/verify_backfill.py
```

---

### Utility Modules (scripts/utils/)

- **geocoding.py** (NEW - v2.1): Multi-strategy geocoding service with industrial zones
- **company_resolver.py**: `CompanyResolver` with quality gates
- **deduplication.py**: Shared deduplication logic (4-priority matching)
- **id_utils.py**: Canonical ID mapping
- **paths.py**: Shared path configuration
- **country_utils.py**: Country code normalization
- **ownership_parser.py**: Parse ownership percentages
- **facility_sync.py**: Parquet export/import

---

### Complete Workflow Example

```bash
# Step 1: Import facilities (Phase 1 - Extraction)
python scripts/import_from_report.py albania_report.txt

# Step 2: Backfill enrichment (NEW - v2.1)
python scripts/backfill.py all --country ALB --interactive

# Step 3: (Optional) Deep research enrichment
python scripts/deep_research_integration.py \
    --generate-prompt --country ALB --metal chromium

# Copy prompt to Gemini Deep Research, then process results
python scripts/deep_research_integration.py \
    --process gemini_output.json --country ALB

# Step 4: Audit data quality
python scripts/tools/audit_facilities.py

# Step 5: Export to parquet
python scripts/facilities.py sync --export
```

---

## Related Documentation

- **[README.md](README.md)**: Complete all-in-one documentation (includes geocoding guide in Section 8)
- **[CHANGELOG.md](CHANGELOG.md)**: Version history and release notes

## Support

For questions or issues:
1. Check README.md for comprehensive documentation
2. Review facility schema: `schemas/facility.schema.json`
3. Check import logs: `output/import_logs/`
4. Examine example facilities in `facilities/*/`
5. Check CHANGELOG.md for recent changes
