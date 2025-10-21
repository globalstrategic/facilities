# Changelog

All notable changes to the Facilities Database project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.1] - 2025-10-21

### Added
- **Deduplication System**: Comprehensive duplicate detection and cleanup
  - 4-priority matching strategy (coordinate-based, exact name, fuzzy name, alias)
  - Two-tier coordinate matching (0.01°/0.1° thresholds)
  - Word overlap matching (80% threshold) for name variations
  - `scripts/deduplicate_facilities.py` for batch cleanup
  - Facility scoring system for intelligent merge selection
  - Full data preservation during merge (aliases, sources, commodities, company mentions)

### Changed
- **Import Pipeline** (`scripts/import_from_report.py`):
  - Enhanced duplicate detection with coordinate-first matching
  - Added name containment and word overlap checks
  - Improved handling of facilities with missing coordinates
- **Documentation**:
  - Consolidated 16 markdown files → 9 active documentation files
  - Updated CLAUDE.md with comprehensive deduplication section
  - Updated docs/README_FACILITIES.md with deduplication workflows
  - Created CHANGELOG.md for version history
  - Created docs/INDEX.md for navigation

### Fixed
- **South Africa Duplicates**: Cleaned up 151 duplicate facilities (779 → 628, 19.4% reduction)
  - Two Rivers Platinum Mine: 3 duplicates → 1 consolidated facility
  - New Denmark: 2 duplicates → 1 consolidated
  - Messina: 3 duplicates → 2 consolidated
  - 147 total duplicate groups resolved

### Removed
- Deleted redundant documentation:
  - `REPOSITORY_STRUCTURE.md` (content merged into CLAUDE.md)
  - `DUPLICATE_FUNCTIONALITY_ANALYSIS.md` (superseded by implementation)
  - `SCRIPT_AUDIT.md` (superseded by CLAUDE.md Scripts Reference)
  - `docs/FACILITIES_MIGRATION_PLAN.md` (completed migration)
  - `docs/ENTITY_IDENTITY_INTEGRATION.md` (superseded by comprehensive plan)

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

- **2.0.1** (2025-10-21): Deduplication system, documentation consolidation
- **2.0.0** (2025-10-20): EntityIdentity integration, two-phase company resolution, schema v2.0
- **1.8.0** (2025-10-14): Deep research integration, schema documentation
- **1.5.0** (2025-10-13): EntityIdentity planning, utilities
- **1.0.0** (2025-10-10): Initial release

---

## Migration Notes

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

### 2.0.1
- **Documentation files** (deleted):
  - `REPOSITORY_STRUCTURE.md` - Use CLAUDE.md
  - `DUPLICATE_FUNCTIONALITY_ANALYSIS.md` - See this CHANGELOG
  - `SCRIPT_AUDIT.md` - See CLAUDE.md Scripts Reference section
  - `docs/FACILITIES_MIGRATION_PLAN.md` - Migration completed
  - `docs/ENTITY_IDENTITY_INTEGRATION.md` - See ENTITYIDENTITY_INTEGRATION_PLAN.md

### 2.0.0
- **`import_from_report_enhanced.py`**: Merged into `import_from_report.py`
- **Legacy CSV structure**: Migrated to JSON
- **Schema v1.x**: Superseded by v2.0.0

---

## Known Issues

### Current (2.0.1)
- Some countries still have potential duplicates (run deduplication)
- Directory naming inconsistency (mix of ISO2/ISO3) - harmless but could be standardized
- ~0.7% of facilities still missing coordinates

### Resolved in 2.0.1
- ✅ South Africa duplicates (151 removed)
- ✅ Coordinate-based duplicate detection
- ✅ Name variation handling

---

## Statistics

### Current Database (2025-10-21)
- **Total Facilities**: ~8,455 (after ZAF deduplication)
- **Countries**: 129 (ISO3 codes)
- **Top Countries**: CHN (1,837), USA (1,623), ZAF (628), AUS (578), IDN (461), IND (424)
- **With Coordinates**: 99.3%
- **Average Confidence**: 0.641

### Growth
- **2025-10-10**: 8,500 facilities (v1.0.0)
- **2025-10-20**: 8,606 facilities (v2.0.0)
- **2025-10-21**: 8,455 facilities (v2.0.1 - post deduplication)

---

## Contributors

- Claude Code (Anthropic) - Primary development assistant
- EntityIdentity library - Entity resolution backbone
- Gemini Deep Research - Facility enrichment

---

## Links

- [Primary Documentation](docs/README_FACILITIES.md)
- [Developer Guide](CLAUDE.md)
- [EntityIdentity Integration Plan](docs/ENTITYIDENTITY_INTEGRATION_PLAN.md)
- [Schema Documentation](docs/SCHEMA_CHANGES_V2.md)
