# Changelog

All notable changes to the Facilities Database project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.1.1] - 2025-10-21

### Fixed
- **EntityIdentity Integration**: Fixed metal formula field mapping in `scripts/import_from_report.py:911`
  - Changed `result.get('formula')` to `result.get('chemical_formula')` to match updated entityidentity API
  - Metal normalization now successfully extracts chemical formulas and categories
  - Tested with neodymium facilities: 100% success rate (33/33 facilities got "Nd" formula and "rare_earth_element" category)
  - Resolved issue where metal resolutions were showing 0 despite entityidentity being available

### Tested
- **Import Pipeline Validation**: Comprehensive test with 36 neodymium/REE facilities
  - ✅ Duplicate detection: 100% accurate (3/3 caught: Bayan Obo, Mountain Pass, Steenkampskraal)
  - ✅ Country assignment: 100% accurate (33 facilities sorted into 21 countries)
  - ✅ Metal formulas: 34 commodities enriched with chemical formulas via entityidentity
  - ✅ Status mapping: Correctly assigned operating/suspended/unknown states
  - ✅ CSV parsing: Multi-column tables with semicolon-separated commodities
  - Test data: `data/neodymium_converted.csv` (36 global REE facilities)

## [2.1.0] - 2025-10-21

### Added
- **Geocoding & Backfill System**: Comprehensive data enrichment tools
  - Multi-strategy geocoding: Industrial zones → Nominatim API → Interactive prompting
  - Industrial zones database (UAE zones pre-configured: ICAD I/II/III, Musaffah, Jebel Ali, FOIZ, Hamriyah)
  - Nominatim (OpenStreetMap) API integration with 1 req/sec rate limiting
  - Interactive prompting for manual geocoding when automated methods fail
  - `scripts/backfill.py` - Unified enrichment system with subcommands (geocode, companies, metals, all)
  - `scripts/geocode_facilities.py` - Standalone geocoding utility
  - `scripts/utils/geocoding.py` - Multi-strategy geocoding service
  - Batch processing support for multiple countries
  - Dry-run mode for all backfill operations
  - Statistics tracking for each enrichment operation
  - Deep research import: Added 298 new facilities from 12 countries

### Changed
- **Database Growth**: 8,752 → 9,058 facilities
- **Documentation Consolidation**: Merged BACKFILL_GUIDE.md into README.md, CLAUDE.md, and CHANGELOG.md
  - README.md: Complete geocoding guide in Section 8
  - CLAUDE.md: Added backfill section with usage patterns and scripts reference
  - CHANGELOG.md: Added v2.1.0 release notes with geocoding features
- **Import Pipeline**: Enhanced geocoding during import for facilities without coordinates

### Fixed
- Removed malformed facilities (table footnotes parsed as facility names)
- Fixed typo in UAE facility commodity name ("wire rod)" → "wire rod")
- Geocoded 6 UAE facilities automatically (16.1% success rate with automated strategies)

## [2.0.1] - 2025-10-21

### Added
- **Deduplication System**: Comprehensive duplicate detection and cleanup
  - 4-priority matching strategy (coordinate-based, exact name, fuzzy name, alias)
  - Two-tier coordinate matching (0.01°/0.1° thresholds)
  - Word overlap matching (80% threshold) for name variations
  - `scripts/deduplicate_facilities.py` for batch cleanup
  - Facility scoring system for intelligent merge selection
  - Full data preservation during merge (aliases, sources, commodities, company mentions)
  - `scripts/utils/deduplication.py` shared logic module

### Changed
- **Import Pipeline** (`scripts/import_from_report.py`):
  - Enhanced duplicate detection with coordinate-first matching
  - Added name containment and word overlap checks
  - Improved handling of facilities with missing coordinates
- **Documentation** (Major consolidation):
  - All documentation consolidated into 3 files: README.md, CLAUDE.md, CHANGELOG.md
  - Removed docs/ directory entirely
  - README.md: Comprehensive 1,000+ line all-in-one guide
    - Includes: Quick Start, Architecture, Data Model, EntityIdentity Integration
    - Includes: Import Workflows, Deduplication, Company Resolution, Deep Research
    - Includes: Querying, Schema Reference, CLI Commands, Data Quality, Statistics
  - CLAUDE.md: Developer-focused guide with code patterns and workflows
  - CHANGELOG.md: Version history (this file)

### Fixed
- **South Africa Duplicates**: Cleaned up 151 duplicate facilities (779 → 628, 19.4% reduction)
  - Two Rivers Platinum Mine: 3 duplicates → 1 consolidated facility
  - New Denmark: 2 duplicates → 1 consolidated
  - Messina: 3 duplicates → 2 consolidated
  - 147 total duplicate groups resolved

### Removed
- **Documentation consolidation**: Deleted entire docs/ directory
  - All content consolidated into README.md
  - Removed: `docs/README_FACILITIES.md` (→ README.md)
  - Removed: `docs/ENTITYIDENTITY_INTEGRATION_PLAN.md` (→ README.md)
  - Removed: `docs/SCHEMA_CHANGES_V2.md` (→ README.md)
  - Removed: `docs/DEEP_RESEARCH_WORKFLOW.md` (→ README.md)
- **Config directory deleted**: Hardcoded defaults in code
  - Removed: `config/` directory
  - Quality gates now hardcoded in `scripts/utils/company_resolver.py`
  - Optional config file override still supported but not required
- **Experimental scripts removed**: Cleaned up unused utilities
  - Removed: `scripts/parse_narrative_operators.py` (experimental, not integrated)
  - Removed: `scripts/extract_companies_from_narrative.py` (experimental, not integrated)
- Previously deleted (earlier in v2.0.1):
  - `REPOSITORY_STRUCTURE.md` (→ CLAUDE.md)
  - `DUPLICATE_FUNCTIONALITY_ANALYSIS.md` (superseded)
  - `SCRIPT_AUDIT.md` (→ CLAUDE.md)
  - `docs/INDEX.md` (→ README.md)

## [2.0.0] - 2025-10-20

### Added
- **EntityIdentity Integration** (Complete):
  - Company resolution with quality gates (strict/moderate/permissive profiles)
  - Metal normalization with chemical formulas and categories
  - Country auto-detection from text and filenames
  - Facility matching with multi-strategy duplicate detection
- **Two-Phase Company Resolution**:
  - Phase 1: Extract company mentions during import
  - Phase 2: Batch resolve to canonical IDs with quality gates
  - `scripts/enrich_companies.py` for batch enrichment
  - `tables/facilities/facility_company_relationships.parquet` output
- **Schema v2.0.0**:
  - Added `chemical_formula` and `category` to commodities
  - Added `company_mentions[]` array for Phase 1
  - Added `owner_links[]` and `operator_link` for Phase 2
  - Backward compatible with v1.x
- **Deep Research Integration**:
  - `scripts/deep_research_integration.py` for Gemini research
  - Prompt generation from facility data
  - Research output processing with company resolution
- **Utilities**:
  - `scripts/utils/company_resolver.py` - CompanyResolver with quality gates
  - `scripts/utils/country_utils.py` - Country normalization
  - `scripts/utils/id_utils.py` - Canonical ID mapping
  - `scripts/utils/facility_sync.py` - Parquet export/import

### Changed
- **Import Pipeline** (`scripts/import_from_report.py`):
  - Enhanced with entity resolution (metals, companies)
  - Per-row country detection for multi-country CSVs
  - Company mentions extraction (Phase 1 pattern)
  - Improved duplicate detection
- **Facility Schema**: Upgraded to v2.0.0 with new fields
- **Database Size**: 8,606 facilities across 129 countries

### Fixed
- Improved metal normalization accuracy (50+ metals with formulas)
- Better handling of company name variations
- More reliable country code detection

## [1.8.0] - 2025-10-14

### Added
- **Gemini Deep Research Workflow**:
  - Research prompt generation by country/metal
  - Batch processing of research outputs
  - Status and product enrichment from research
- **Schema Changes**:
  - Documented v2.0.0 schema enhancements
  - Migration guide for legacy fields

### Changed
- Import pipeline refactored for entity resolution
- Improved logging and error handling

## [1.5.0] - 2025-10-13

### Added
- Initial EntityIdentity integration planning
- Company resolution utilities
- Facility matching utilities

### Changed
- Database structure refined for better organization
- Import scripts modularized

## [1.0.0] - 2025-10-10

### Added
- Initial release of Facilities Database
- 8,500+ facilities across 129 countries
- JSON-based storage structure
- CSV import pipeline
- Basic duplicate detection
- Facility schema v1.0.0

### Infrastructure
- Directory structure: `facilities/{ISO3}/`
- Schema validation
- Import logging
- Basic company and metal normalization

---

## Version History Summary

- **2.1.0** (2025-10-21): Geocoding & backfill system, deep research import, 9,058 facilities
- **2.0.1** (2025-10-21): Deduplication system, documentation consolidation
- **2.0.0** (2025-10-20): EntityIdentity integration, two-phase company resolution, schema v2.0
- **1.8.0** (2025-10-14): Deep research integration, schema documentation
- **1.5.0** (2025-10-13): EntityIdentity planning, utilities
- **1.0.0** (2025-10-10): Initial release

---

## Migration Notes

### 2.0.1 → 2.1.0
- No schema changes
- Install geocoding dependencies: `pip install geopy`
- Optionally backfill missing coordinates: `python scripts/backfill.py geocode --country <ISO3>`
- Review deprecated BACKFILL_GUIDE.md (content now in README.md Section 8)

### 2.0.0 → 2.0.1
- No schema changes
- Run `deduplicate_facilities.py` on countries with known duplicates
- Update documentation references if customized

### 1.x → 2.0.0
- **Schema**: Facilities automatically upgraded on import
- **Company data**:
  - Old `operator` → `operator_link` (Phase 2)
  - Old owner strings → `owner_links[]` (Phase 2)
  - Raw mentions preserved in `company_mentions[]`
- **Commodities**: Enhanced with `chemical_formula` and `category`
- **Scripts**: Import pipeline backward compatible

---

## Deprecations

### 2.1.0
- **BACKFILL_GUIDE.md** - Content consolidated into README.md (Section 8), CLAUDE.md (Backfill section), and CHANGELOG.md (v2.1.0 notes)

### 2.0.1
- **All documentation consolidated into 3 files**:
  - **README.md** - Complete all-in-one documentation (was docs/README_FACILITIES.md + 3 other docs)
  - **CLAUDE.md** - Developer guide (was CLAUDE.md + merged deprecated docs)
  - **CHANGELOG.md** - Version history (this file)
- **Deleted docs/ directory entirely** - All content merged into README.md
- **Deleted redundant docs** - REPOSITORY_STRUCTURE.md, DUPLICATE_FUNCTIONALITY_ANALYSIS.md, SCRIPT_AUDIT.md

### 2.0.0
- **`import_from_report_enhanced.py`**: Merged into `import_from_report.py`
- **Legacy CSV structure**: Migrated to JSON
- **Schema v1.x**: Superseded by v2.0.0

---

## Known Issues

### Current (2.1.0)
- Low automated geocoding success rate (10-20%) - use interactive mode or add industrial zones
- Some countries still have potential duplicates (run deduplication)
- Directory naming inconsistency (mix of ISO2/ISO3) - harmless but could be standardized
- ~1% of facilities still missing coordinates (down from ~0.7% after backfill)

### Resolved in 2.0.1
- ✅ South Africa duplicates (151 removed)
- ✅ Coordinate-based duplicate detection
- ✅ Name variation handling

---

## Statistics

### Current Database (2025-10-21)
- **Total Facilities**: 9,058
- **Countries**: 129 (ISO3 codes)
- **Top Countries**: CHN (1,837), USA (1,623), ZAF (628), AUS (613), IDN (461), IND (424)
- **With Coordinates**: ~99% (8,970+ facilities)
- **Average Confidence**: 0.64

### Growth
- **2025-10-10**: 8,500 facilities (v1.0.0)
- **2025-10-20**: 8,606 facilities (v2.0.0)
- **2025-10-21 (morning)**: 8,455 facilities (v2.0.1 - post deduplication)
- **2025-10-21 (afternoon)**: 8,752 facilities (deep research import)
- **2025-10-21 (final)**: 9,058 facilities (v2.1.0 - geocoding & backfill)

---

## Contributors

- Claude Code (Anthropic) - Primary development assistant
- EntityIdentity library - Entity resolution backbone
- Gemini Deep Research - Facility enrichment

---

## Links

- [Complete Documentation](README.md) - All-in-one guide with everything you need
- [Developer Guide](CLAUDE.md) - Code patterns and workflows for contributors
- [Version History](CHANGELOG.md) - This file
