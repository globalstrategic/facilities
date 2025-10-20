# Facilities Database: Complete Repository Structure Guide

**Version**: 2.0.0 (EntityIdentity Integration Complete)
**Database Size**: 8,609 facilities across 129 countries
**Package Name**: `talloy`
**Last Updated**: 2025-10-20

## Table of Contents

1. [Repository Overview](#repository-overview)
2. [Directory Structure](#directory-structure)
3. [Core Components](#core-components)
4. [Data Architecture](#data-architecture)
5. [Key Scripts & Their Purpose](#key-scripts--their-purpose)
6. [Entity Resolution System](#entity-resolution-system)
7. [Data Flow & Pipelines](#data-flow--pipelines)
8. [Configuration System](#configuration-system)
9. [Testing Framework](#testing-framework)
10. [Output & Artifacts](#output--artifacts)
11. [Integration with EntityIdentity](#integration-with-entityidentity)
12. [Common Workflows](#common-workflows)

---

## Repository Overview

This repository is a comprehensive global mining and processing facilities database that provides:

- **Structured data** for 8,609 facilities in JSON format
- **Entity resolution** for companies, metals, and countries
- **Two-phase company enrichment** pipeline
- **Research integration** with Gemini Deep Research
- **Schema validation** with JSON Schema v2.0.0
- **Quality gates** for company resolution
- **Parquet export/import** for data interchange

### Key Statistics

- **Total Facilities**: 8,609
- **Countries Covered**: 129 (ISO3 format)
- **Top Countries**: CHN (1,837), USA (1,621), AUS (578), IDN (461), IND (424)
- **Metals/Commodities**: 50+ types
- **Coordinate Coverage**: 99.3% (8,544 facilities)
- **Company Links**: Growing via Phase 2 enrichment
- **Average Confidence**: 0.641

---

## Directory Structure

```
facilities/                                    # Repository root
│
├── facilities/                                # 8,609 facility JSON files
│   ├── USA/                                   # 1,621 facilities (ISO3 code)
│   │   ├── usa-stillwater-east-fac.json
│   │   ├── usa-highland-fac.json
│   │   └── ...
│   ├── CHN/                                   # 1,837 facilities
│   ├── AUS/                                   # 578 facilities
│   ├── ZAF/                                   # 272 facilities
│   └── ... (125 more country directories)
│
├── scripts/                                   # Core Python scripts (~4,541 lines)
│   ├── facilities.py                          # Unified CLI (sync, resolve, test)
│   ├── import_from_report.py                  # Main import pipeline
│   ├── enrich_companies.py                    # Phase 2: Company enrichment
│   ├── deep_research_integration.py           # Gemini research integration
│   ├── backfill_mentions.py                   # Extract company mentions
│   ├── verify_backfill.py                     # Verify backfill results
│   ├── audit_facilities.py                    # Data quality checks
│   ├── full_migration.py                      # Legacy migration
│   ├── migrate_legacy_fields.py               # Field migration
│   ├── pipeline_ingest.py                     # Pipeline orchestration
│   ├── run_enrichment.sh                      # Batch enrichment wrapper
│   │
│   ├── utils/                                 # Utility modules
│   │   ├── __init__.py
│   │   ├── company_resolver.py                # CompanyResolver class (501 lines)
│   │   ├── id_utils.py                        # Canonical ID mapping
│   │   ├── paths.py                           # Shared path configuration
│   │   ├── country_utils.py                   # Country normalization
│   │   ├── ownership_parser.py                # Parse ownership strings
│   │   └── facility_sync.py                   # Parquet export/import
│   │
│   └── tests/                                 # Test suite
│       ├── test_dedup.py                      # Duplicate detection tests
│       ├── test_schema.py                     # Schema validation tests
│       └── test_facility_sync.py              # Sync functionality tests
│
├── schemas/                                   # JSON Schema definitions
│   └── facility.schema.json                   # Schema v2.0.0 (247 lines)
│
├── config/                                    # Configuration files
│   └── gates.json                             # Quality gate thresholds
│
├── tables/                                    # Parquet output (Phase 2)
│   └── facilities/
│       ├── facility_company_relationships.parquet    # 9.3 KB
│       ├── facility_company_relationships.csv        # 1.0 KB
│       └── relationships.accepted.csv                # 1.0 KB
│
├── docs/                                      # Documentation
│   ├── README_FACILITIES.md                   # Primary documentation (730 lines)
│   ├── ENTITYIDENTITY_INTEGRATION_PLAN.md     # Integration architecture
│   ├── SCHEMA_CHANGES_V2.md                   # Schema v2.0.0 docs
│   ├── DEEP_RESEARCH_WORKFLOW.md              # Research workflow guide
│   ├── ENTITY_IDENTITY_INTEGRATION.md         # Integration notes
│   ├── FACILITIES_MIGRATION_PLAN.md           # Migration documentation
│   ├── sample_enhanced_facility.json          # Example facility
│   │
│   ├── guides/                                # User guides
│   │   └── RESOLUTION_WORKFLOW.md             # Resolution workflow
│   │
│   └── implementation_history/                # Development history
│       ├── COVERAGE_2025-10-20.md             # Current coverage
│       └── PHASE_2_ALTERNATE_PATH.md          # Phase 2 design decisions
│
├── output/                                    # Generated outputs (gitignored)
│   ├── import_logs/                           # Import reports (29 files)
│   │   ├── import_report_ALB_20251020_103704.json
│   │   ├── import_report_DZA_20251014_170943.json
│   │   └── ...
│   ├── research_raw/                          # Gemini outputs
│   └── entityidentity_export/                 # Parquet exports
│
├── utils/                                     # Top-level utilities
│   └── __init__.py                            # Package initialization
│
├── migration/                                 # Legacy migration artifacts
│   └── ... (historical)
│
├── data/                                      # Data directory
│   └── ... (supplementary data)
│
├── gt/                                        # Ground truth data
│   └── ... (validation data)
│
├── examples/                                  # Example scripts/data
│   └── ... (demonstrations)
│
├── .pytest_cache/                             # Pytest cache
├── .git/                                      # Git repository
├── .claude/                                   # Claude Code settings
│   └── settings.local.json
│
├── setup.py                                   # Package setup (talloy)
├── requirements.txt                           # Runtime dependencies
├── requirements-dev.txt                       # Development dependencies
├── pytest.ini                                 # Pytest configuration
├── .gitignore                                 # Git ignore patterns
├── CLAUDE.md                                  # Claude Code instructions (20,465 bytes)
├── LICENSE                                    # Repository license
├── review_pack_2025-10-20.csv                # Review queue
└── deep_research_integration.log              # Research logs

```

---

## Core Components

### 1. Facility Database (`facilities/`)

**Structure**: Individual JSON files organized by ISO3 country code

- **Format**: `{iso3}/{iso3}-{slug}-fac.json`
- **Example**: `ZAF/zaf-rustenburg-karee-fac.json`
- **Count**: 8,609 files across 129 directories

**Key Features**:
- Each facility is a standalone JSON file
- Schema v2.0.0 compliant
- Contains company mentions (Phase 1) ready for enrichment (Phase 2)
- Includes verification status and confidence scores

### 2. Import Pipeline (`scripts/import_from_report.py`)

**Purpose**: Convert research reports into structured facility JSONs

**Features**:
- Extract markdown tables from text reports
- Auto-detect country from filename or content
- Normalize metal names and facility types
- Multi-strategy duplicate detection
- Schema validation
- Generate import statistics

**Enhanced Mode** (with EntityIdentity):
- Resolves company names to canonical IDs
- Adds chemical formulas to commodities
- Calculates confidence boosts
- Cross-references with EntityIdentity database

### 3. Company Enrichment (`scripts/enrich_companies.py`)

**Purpose**: Phase 2 - Batch resolve company mentions to canonical IDs

**Architecture**:
```
Facility JSONs (company_mentions[])
  ↓
Extract Mentions
  ↓
CompanyResolver (with quality gates)
  ↓
Gate Application (auto_accept / review / pending)
  ↓
Write to facility_company_relationships.parquet
```

**Quality Gates** (from `config/gates.json`):
- **auto_accept**: confidence ≥ 0.90 (write immediately)
- **review**: confidence ≥ 0.75 (human review needed)
- **pending**: confidence < 0.75 (track for later)

**Output**: Relationships stored in parquet, NOT in facility JSONs

### 4. Entity Resolution System (`scripts/utils/`)

#### `company_resolver.py` - CompanyResolver Class

**Key Methods**:
- `resolve_operator(name, country_hint, facility_coords)` → dict
- `resolve_owners(owner_text, country_hint)` → list[dict]
- `resolve_mentions(mentions, facility)` → (accepted, review, pending)

**Features**:
- LEI-based company matching via EntityIdentity
- Fuzzy name matching with confidence scoring
- Proximity boosting for nearby headquarters
- Caching to avoid redundant lookups
- Quality gate enforcement

**Database**: 3,687+ companies with LEI codes

#### `country_utils.py` - Country Normalization

**Functions**:
- `normalize_country_to_iso3(name)` → ISO3 code
- `iso3_to_country_name(iso3)` → full name
- Auto-detect from text or filename

#### `id_utils.py` - Canonical ID Mapping

**Functions**:
- `to_canonical(company_name)` → canonical ID
- `load_alias_map(path)` → dict of aliases

**Alias Map** (`config/company_aliases.json`):
Maps common company names to canonical IDs

#### `facility_sync.py` - Parquet Export/Import

**Features**:
- Export all facilities to parquet (compressed ~0.7 MB)
- Import from EntityIdentity parquet format
- Preserve all fields and metadata

### 5. Unified CLI (`scripts/facilities.py`)

**Command Groups**:

**System**:
- `sync --export` - Export to parquet
- `sync --import` - Import from parquet
- `sync --status` - Database statistics

**Resolution**:
- `resolve country "Algeria"` - Test country resolution
- `resolve metal "Cu"` - Test metal normalization
- `resolve company "BHP"` - Test company matching

**Research**:
- `research --generate-prompt` - Create research prompt
- `research --process output.json` - Process results

**Testing**:
- `test` - Run all tests
- `test --suite dedup` - Run specific test suite

---

## Data Architecture

### Facility JSON Schema (v2.0.0)

```json
{
  "facility_id": "zaf-rustenburg-karee-fac",           // Required: unique ID
  "ei_facility_id": "karee_52f2f3d6",                  // Optional: EntityIdentity link
  "name": "Karee Mine",                                // Required: facility name
  "aliases": ["Karee", "Rustenburg Karee"],            // Optional: alternative names
  "country_iso3": "ZAF",                               // Required: ISO3 country code

  "location": {                                        // Optional: geographic coordinates
    "lat": -25.666,
    "lon": 27.202,
    "precision": "site"                                // exact | site | approximate | region | unknown
  },

  "types": ["mine", "concentrator"],                   // Required: facility types

  "commodities": [                                     // Optional: metals produced
    {
      "metal": "platinum",
      "primary": true,
      "chemical_formula": "Pt",                        // New in v2.0
      "category": "precious_metal"                     // New in v2.0
    }
  ],

  "status": "operating",                               // Optional: operational status

  "company_mentions": [                                // Phase 1: Raw mentions
    {
      "name": "Sibanye-Stillwater",
      "role": "operator",                              // operator | owner | majority_owner | minority_owner
      "source": "gemini_research",
      "confidence": 0.85,
      "first_seen": "2025-10-14T10:00:00",
      "evidence": "Extracted from research report"
    }
  ],

  "owner_links": [],                                   // Phase 2: Resolved owners (currently empty)
  "operator_link": null,                               // Phase 2: Resolved operator (currently empty)

  "products": [],                                      // Optional: production streams

  "sources": [                                         // Optional: data sources
    {
      "type": "gemini_research",
      "id": "South Africa Platinum Study 2025",
      "date": "2025-10-14T00:00:00"
    }
  ],

  "verification": {                                    // Required: verification status
    "status": "llm_suggested",                         // csv_imported | llm_suggested | llm_verified | human_verified | conflicting
    "confidence": 0.85,
    "last_checked": "2025-10-14T10:00:00",
    "checked_by": "import_pipeline"
  }
}
```

### Two-Phase Company Resolution

**Design Decision**: Relationships stored in parquet, NOT in facility JSON

**Phase 1: Extraction** (`import_from_report.py`)
- Extract company names from source data
- Store in `company_mentions[]` array
- Include role, source, confidence, evidence
- NO resolution to canonical IDs yet

**Phase 2: Resolution** (`enrich_companies.py`)
- Batch process all facilities with `company_mentions`
- Use `CompanyResolver` with quality gates
- Write relationships to `facility_company_relationships.parquet`
- Separate storage allows:
  - Batch reprocessing without modifying 8,609 JSONs
  - Quality gate tuning without data migration
  - Multiple relationship sources/versions
  - Efficient querying via pandas/SQL

### Relationship Parquet Schema

```
facility_company_relationships.parquet:
  - relationship_id: UUID
  - facility_id: str (links to JSON file)
  - company_id: str (canonical LEI-based ID)
  - role: str (operator | owner | majority_owner | minority_owner)
  - confidence: float (0.0-1.0)
  - gate: str (auto_accept | review | pending)
  - provenance: str (source of mention)
  - evidence: str (supporting text)
  - gates_applied: dict (penalties applied)
  - created_at: timestamp
```

---

## Key Scripts & Their Purpose

### Import & Enrichment

| Script | Purpose | Usage |
|--------|---------|-------|
| `import_from_report.py` | Import facilities from text reports | `python scripts/import_from_report.py report.txt --country DZA` |
| `enrich_companies.py` | Phase 2: Batch company resolution | `python scripts/enrich_companies.py --country IND` |
| `backfill_mentions.py` | Extract company mentions from existing facilities | `python scripts/backfill_mentions.py --country ZAF` |
| `verify_backfill.py` | Verify backfill completeness | `python scripts/verify_backfill.py` |

### Data Management

| Script | Purpose | Usage |
|--------|---------|-------|
| `facilities.py` | Unified CLI | `python scripts/facilities.py sync --export` |
| `audit_facilities.py` | Data quality checks | `python scripts/audit_facilities.py` |
| `full_migration.py` | Legacy data migration | `python scripts/full_migration.py` |
| `migrate_legacy_fields.py` | Field-level migration | `python scripts/migrate_legacy_fields.py` |
| `pipeline_ingest.py` | Orchestrate import pipeline | `python scripts/pipeline_ingest.py` |

### Research Integration

| Script | Purpose | Usage |
|--------|---------|-------|
| `deep_research_integration.py` | Gemini Deep Research integration | `python scripts/deep_research_integration.py --generate-prompt --country ZAF --metal platinum` |

### Utilities

| Module | Purpose | Key Classes/Functions |
|--------|---------|----------------------|
| `company_resolver.py` | Company name resolution | `CompanyResolver`, `resolve_operator()`, `resolve_mentions()` |
| `id_utils.py` | Canonical ID mapping | `to_canonical()`, `load_alias_map()` |
| `paths.py` | Path configuration | `relationships_path()`, shared constants |
| `country_utils.py` | Country normalization | `normalize_country_to_iso3()`, `iso3_to_country_name()` |
| `ownership_parser.py` | Parse ownership strings | `parse_ownership()` |
| `facility_sync.py` | Parquet export/import | `FacilitySyncManager` |

### Bash Scripts

| Script | Purpose | Usage |
|--------|---------|-------|
| `run_enrichment.sh` | Wrapper for batch enrichment | `./scripts/run_enrichment.sh --country BRA` |

---

## Entity Resolution System

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Entity Resolution System                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐   │
│  │   Country    │     │    Metal     │     │   Company    │   │
│  │  Resolution  │     │ Normalization│     │  Resolution  │   │
│  └──────────────┘     └──────────────┘     └──────────────┘   │
│         │                     │                     │           │
│         ▼                     ▼                     ▼           │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐   │
│  │ country_utils│     │metal_normaliz│     │company_resolv│   │
│  │     .py      │     │    er.py     │     │    er.py     │   │
│  └──────────────┘     └──────────────┘     └──────────────┘   │
│         │                     │                     │           │
│         └─────────────────────┴─────────────────────┘           │
│                               │                                 │
│                               ▼                                 │
│                  ┌───────────────────────────┐                  │
│                  │  EntityIdentity Library   │                  │
│                  │  (External Dependency)    │                  │
│                  └───────────────────────────┘                  │
│                               │                                 │
│                  ┌────────────┴────────────┐                    │
│                  ▼                         ▼                    │
│         ┌────────────────┐       ┌────────────────┐            │
│         │ Company DB     │       │ Metal DB       │            │
│         │ 3,687 LEIs     │       │ 50+ metals     │            │
│         └────────────────┘       └────────────────┘            │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### CompanyResolver Workflow

```
Input: Company Mention
  │
  ├─ name: "BHP"
  ├─ role: "operator"
  ├─ country_hint: "AUS"
  └─ facility_coords: (-25.666, 27.202)
  │
  ▼
┌────────────────────────┐
│  Check Cache           │
│  (5min TTL)            │
└────────────────────────┘
  │
  ▼ [Cache Miss]
┌────────────────────────┐
│  EntityIdentity Match  │
│  - Fuzzy name match    │
│  - LEI lookup          │
│  - Wikidata link       │
└────────────────────────┘
  │
  ▼
┌────────────────────────┐
│  Calculate Confidence  │
│  - Base score (0-1)    │
│  - Proximity boost     │
│  - Apply penalties     │
└────────────────────────┘
  │
  ▼
┌────────────────────────┐
│  Apply Quality Gates   │
│  - ≥0.90: auto_accept  │
│  - ≥0.75: review       │
│  - <0.75: pending      │
└────────────────────────┘
  │
  ▼
Output: Resolution
  │
  ├─ company_id: "cmp-549300HX3DJC74TG4332"
  ├─ company_name: "BHP Group Limited"
  ├─ confidence: 0.95
  ├─ gate: "auto_accept"
  └─ match_explanation: "Exact name match"
```

---

## Data Flow & Pipelines

### Import Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                       IMPORT PIPELINE                            │
└─────────────────────────────────────────────────────────────────┘

INPUT: Research Report (markdown, CSV, stdin)
  │
  ▼
┌────────────────────────┐
│ Extract Tables         │  extract_markdown_tables()
│ - Parse markdown       │
│ - Handle CSV           │
└────────────────────────┘
  │
  ▼
┌────────────────────────┐
│ Entity Resolution      │  (Optional: --enhanced flag)
│ - Auto-detect country  │  country_utils.detect_country()
│ - Normalize metals     │  metal_normalizer.normalize()
│ - Extract companies    │  company_resolver.resolve_operator()
└────────────────────────┘
  │
  ▼
┌────────────────────────┐
│ Create Facility JSON   │
│ - Generate ID          │  slugify() → iso3-slug-fac
│ - Map fields           │  types, status, commodities
│ - Add sources          │  track provenance
└────────────────────────┘
  │
  ▼
┌────────────────────────┐
│ Duplicate Detection    │  FacilityMatcher
│ - Name match           │
│ - Location (5km)       │
│ - Alias match          │
│ - Company + commodity  │
└────────────────────────┘
  │
  ▼
┌────────────────────────┐
│ Schema Validation      │  jsonschema.validate()
│ - Required fields      │
│ - Field types          │
│ - Enum values          │
└────────────────────────┘
  │
  ▼
┌────────────────────────┐
│ Write to JSON          │  facilities/{iso3}/{id}.json
│ - Pretty print         │
│ - UTF-8 encoding       │
└────────────────────────┘
  │
  ▼
┌────────────────────────┐
│ Generate Statistics    │  output/import_logs/
│ - Facilities added     │
│ - Duplicates skipped   │
│ - Entity resolution    │
└────────────────────────┘

OUTPUT: Facility JSONs + Import Log
```

### Company Enrichment Pipeline (Phase 2)

```
┌─────────────────────────────────────────────────────────────────┐
│                   COMPANY ENRICHMENT PIPELINE                    │
└─────────────────────────────────────────────────────────────────┘

INPUT: Facility JSONs with company_mentions[]
  │
  ▼
┌────────────────────────┐
│ Extract Mentions       │
│ - Parse mentions array │
│ - Filter by role       │
│ - Gather hints (LEI,   │
│   country, coords)     │
└────────────────────────┘
  │
  ▼
┌────────────────────────┐
│ Batch Resolution       │  CompanyResolver.resolve_mentions()
│ - Match to LEI DB      │
│ - Calculate confidence │
│ - Apply proximity boost│
└────────────────────────┘
  │
  ▼
┌────────────────────────┐
│ Apply Quality Gates    │
│ - auto_accept (≥0.90)  │
│ - review (≥0.75)       │
│ - pending (<0.75)      │
└────────────────────────┘
  │
  ├─────────────────┬─────────────────┐
  ▼                 ▼                 ▼
┌──────────┐  ┌──────────┐  ┌──────────┐
│ Accepted │  │  Review  │  │ Pending  │
│ (write)  │  │ (queue)  │  │ (track)  │
└──────────┘  └──────────┘  └──────────┘
  │                 │                 │
  ▼                 ▼                 ▼
┌────────────────────────────────────────┐
│ Write to Parquet                       │
│ tables/facilities/                     │
│ facility_company_relationships.parquet │
└────────────────────────────────────────┘
  │
  ▼
OUTPUT: Relationships Parquet + Statistics
  - X relationships accepted
  - Y in review queue
  - Z pending
```

---

## Configuration System

### Quality Gate Configuration (`config/gates.json`)

```json
{
  "auto_accept_threshold": 0.90,      // Auto-write relationships
  "review_min_threshold": 0.75,       // Queue for human review
  "prefer_registry_boost": 0.05,      // Bonus for LEI/Wikidata match
  "dual_source_boost": 0.03,          // Bonus for multiple sources
  "parent_match_boost": 0.02          // Bonus for parent company match
}
```

**Profiles** (can be added):
- `strict`: High precision, low recall (min_confidence 0.80)
- `moderate`: Balanced (min_confidence 0.70)
- `permissive`: High recall, low precision (min_confidence 0.60)

### Company Aliases (`config/company_aliases.json`)

Maps common variations to canonical IDs:

```json
{
  "BHP": "cmp-549300HX3DJC74TG4332",
  "BHP Billiton": "cmp-549300HX3DJC74TG4332",
  "BHP Group": "cmp-549300HX3DJC74TG4332",
  "Rio Tinto": "cmp-213800YOEO5OQ72G2R84",
  ...
}
```

---

## Testing Framework

### Test Structure

```
scripts/tests/
├── test_dedup.py              # Duplicate detection tests
├── test_schema.py             # JSON schema validation tests
└── test_facility_sync.py      # Parquet export/import tests
```

### Running Tests

```bash
# All tests
pytest

# Specific test file
pytest scripts/tests/test_schema.py

# With coverage
pytest --cov --cov-report=html

# Verbose output
pytest -v

# Via CLI
python scripts/facilities.py test
python scripts/facilities.py test --suite dedup
```

### Test Configuration (`pytest.ini`)

```ini
[pytest]
testpaths = scripts/tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
```

---

## Output & Artifacts

### Import Logs (`output/import_logs/`)

**Format**: `import_report_{ISO3}_{timestamp}.json`

**Contains**:
- Number of facilities imported
- Duplicates detected and skipped
- Entity resolution statistics
- Confidence score distribution
- Errors and warnings

**Example**:
```json
{
  "timestamp": "2025-10-20T10:37:04",
  "country": "ALB",
  "source": "albania.txt",
  "new_facilities": 15,
  "duplicates_skipped": 2,
  "files_written": 15,
  "entity_resolution": {
    "metals_with_formulas": 28,
    "companies_resolved": 12,
    "confidence_boosts": 12
  }
}
```

### Relationship Parquet (`tables/facilities/`)

**Files**:
- `facility_company_relationships.parquet` - Main relationships (9.3 KB)
- `facility_company_relationships.csv` - CSV export (1.0 KB)
- `relationships.accepted.csv` - Auto-accepted only (1.0 KB)

**Schema**: See [Relationship Parquet Schema](#relationship-parquet-schema)

### Review Pack (`review_pack_2025-10-20.csv`)

**Purpose**: Queue of relationships needing human review

**Columns**:
- facility_id
- company_name
- resolved_to
- confidence
- evidence
- review_notes

---

## Integration with EntityIdentity

### External Dependency

**Repository**: `../entityidentity/` (sibling directory)

**Required Components**:
- `scripts.utils.company_resolver.CompanyResolver` - Company matching (wraps EntityIdentity)
- `entityidentity.companies.pending_tracker.PendingCompanyTracker` - Track unresolved
- `entityidentity.country_identifier()` - Country detection
- `entityidentity.metal_identifier()` - Metal normalization

### Setup

```bash
# Clone EntityIdentity to parent directory
cd /Users/willb/Github/GSMC/
git clone https://github.com/globalstrategic/entityidentity.git

# Or add to PYTHONPATH
export PYTHONPATH="/Users/willb/Github/GSMC/entityidentity:$PYTHONPATH"

# Verify import works
python -c "from scripts.utils.company_resolver import CompanyResolver; print('OK')"
```

### Data Loading

**On First Use**:
- EntityIdentity loads ~50MB parquet files
- Company database: 3,687 LEI entries
- Metal database: 50+ normalized entries
- First import: ~2-3s startup time
- Subsequent imports: cached, <10ms

### Cross-Reference

**Facility Linking**:
- `ei_facility_id` field in facility JSON
- Links to EntityIdentity facilities parquet
- Enables cross-database queries

**Company Linking**:
- `company_id` in relationships parquet
- Format: `cmp-{LEI}` or `cmp-{identifier}`
- Maps to EntityIdentity company records

---

## Common Workflows

### 1. Import New Facilities

```bash
# Standard import
python scripts/import_from_report.py report.txt --country DZA --source "Algeria Report 2025"

# View import log
cat output/import_logs/import_report_DZA_*.json | jq .
```

### 2. Enrich Facilities with Companies

```bash
# Single country
python scripts/enrich_companies.py --country IND

# All countries
python scripts/enrich_companies.py

# Dry run (preview)
python scripts/enrich_companies.py --country IND --dry-run

# View results
python -c "import pandas as pd; df = pd.read_parquet('tables/facilities/facility_company_relationships.parquet'); print(df.head())"
```

### 3. Query Facilities

**By Country**:
```bash
# Count facilities
find facilities/ZAF -name "*.json" | wc -l

# List facility names
find facilities/ZAF -name "*.json" -exec jq -r '.name' {} \;
```

**By Metal**:
```bash
# Find all platinum facilities
grep -r '"metal": "platinum"' facilities/ | cut -d: -f1 | sort -u
```

**By Company**:
```bash
# Find facilities mentioning "BHP"
grep -r '"name": "BHP"' facilities/ --include="*.json" | cut -d: -f1
```

### 4. Export to Parquet

```bash
# Export all facilities
python scripts/facilities.py sync --export

# Custom output location
python scripts/facilities.py sync --export --output /path/to/output/

# Check export
ls -lh output/entityidentity_export/
```

### 5. Data Quality Checks

```bash
# Run audit
python scripts/audit_facilities.py

# Find facilities without coordinates
grep -r '"lat": null' facilities/ | wc -l

# Find low confidence facilities
find facilities -name "*.json" -exec grep -l '"confidence": 0\.[0-4]' {} \;

# Count by status
for status in operating closed suspended; do
  echo "$status: $(grep -r "\"status\": \"$status\"" facilities | wc -l)"
done
```

### 6. Test Entity Resolution

```bash
# Test country resolution
python scripts/facilities.py resolve country "Algeria"

# Test metal normalization
python scripts/facilities.py resolve metal "Cu"
python scripts/facilities.py resolve metal "lithium carbonate"

# Test company resolution
python scripts/facilities.py resolve company "BHP"
python scripts/facilities.py resolve company "Sibanye-Stillwater" --country ZAF
```

### 7. Generate Research Prompts

```bash
# Create prompt for Gemini Deep Research
python scripts/deep_research_integration.py \
    --generate-prompt \
    --country ZAF \
    --metal platinum \
    --limit 50

# Process results
python scripts/deep_research_integration.py \
    --process research_output.json \
    --country ZAF \
    --metal platinum
```

---

## Summary

This repository provides a comprehensive, production-ready global facilities database with:

✅ **8,609 structured facilities** across 129 countries
✅ **Schema v2.0.0** with validation and versioning
✅ **Two-phase company enrichment** with quality gates
✅ **Entity resolution** for countries, metals, and companies
✅ **Research integration** with Gemini Deep Research
✅ **Parquet export/import** for data interchange
✅ **Quality assurance** with confidence scoring and verification
✅ **Comprehensive documentation** and examples

The system is designed for:
- **Scalability**: Handle 10,000+ facilities efficiently
- **Quality**: Multi-strategy validation and resolution
- **Flexibility**: Modular architecture, pluggable components
- **Maintainability**: Clear separation of concerns, extensive docs
- **Integration**: Compatible with EntityIdentity and external systems

For detailed usage, see:
- **[README_FACILITIES.md](docs/README_FACILITIES.md)** - Primary documentation
- **[ENTITYIDENTITY_INTEGRATION_PLAN.md](docs/ENTITYIDENTITY_INTEGRATION_PLAN.md)** - Integration architecture
- **[SCHEMA_CHANGES_V2.md](docs/SCHEMA_CHANGES_V2.md)** - Schema documentation
- **[CLAUDE.md](CLAUDE.md)** - Development instructions for Claude Code
