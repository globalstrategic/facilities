# Global Mining & Metals Facilities Database

A comprehensive database of mining, smelting, refining, and processing facilities worldwide for metals and minerals.

## Overview

This repository contains structured facility data organized by country with comprehensive **entity resolution**, company linking, and research pipeline integration powered by the **EntityIdentity library**.

**Current Data**: 8,606 facilities across 129 countries
**Version**: 2.0.0 (EntityIdentity Integration Complete)

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

# Export to EntityIdentity parquet format
python scripts/facilities.py sync --export

# View a facility
cat facilities/USA/usa-stillwater-east-fac.json
```

## Repository Structure

```
facilities/
├── facilities/              # Facility data organized by ISO3 country code
│   ├── USA/
│   ├── CAN/
│   ├── AUS/
│   └── ...
├── schemas/                 # JSON Schema validation
│   └── facility.schema.json
├── scripts/                 # Data processing scripts
│   └── migrate_facilities.py
├── docs/                    # Documentation
│   ├── README_FACILITIES.md
│   ├── FACILITIES_MIGRATION_PLAN.md
│   └── ENTITY_IDENTITY_INTEGRATION.md
└── output/                  # Generated outputs (not in git)
    └── migration_logs/
```

## Facility Data Format (Schema v2.0.0)

Each facility is a JSON file with standardized structure:

```json
{
  "facility_id": "usa-stillwater-east-fac",
  "ei_facility_id": "stillwater_east_boul_ca835b22",
  "name": "Stillwater East",
  "aliases": ["Stillwater Mine East Boulder", "East Boulder"],
  "country_iso3": "USA",
  "location": {
    "lat": 45.416,
    "lon": -109.85,
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
      "percentage": 100.0,
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
      "capacity": 500000,
      "unit": "oz 2E",
      "year": 2024
    }
  ],
  "sources": [
    {
      "type": "gemini_research",
      "id": "Montana PGM Study 2025",
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

**Schema v2.0.0 Enhancements:**
- `ei_facility_id`: Links to EntityIdentity database
- `chemical_formula`: Chemical formula for each commodity (e.g., "Pt", "Cu", "Fe2O3")
- `category`: Metal classification (base_metal, precious_metal, rare_earth, etc.)
- `company_id`: LEI-based canonical company IDs (e.g., "cmp-378900F238434B74D281")

## Data Coverage

- **Industry**: Mining, smelting, refining, processing
- **Metals**: 50+ types including precious metals, base metals, rare earths, industrial minerals
- **Geography**: Global coverage with geocoded locations
- **Status**: Operating, closed, development, unknown

## Usage Examples

### Query Facilities by Country

```python
import json
from pathlib import Path

# Load all US facilities
for facility_file in Path('facilities/USA').glob('*.json'):
    with open(facility_file) as f:
        facility = json.load(f)
        print(f"{facility['name']}: {facility['commodities']}")
```

### Find Facilities by Metal

```python
import json
from pathlib import Path

def find_platinum_facilities():
    facilities = []
    for facility_file in Path('facilities').glob('**/*.json'):
        with open(facility_file) as f:
            facility = json.load(f)
            for commodity in facility.get('commodities', []):
                if commodity['metal'] == 'platinum':
                    facilities.append(facility)
                    break
    return facilities
```

## Data Quality

### Confidence Levels
- **0.95**: Very High - Human verified with multiple sources
- **0.85**: High - EntityIdentity match or reliable source
- **0.65**: Moderate - CSV import with good data quality
- **0.40**: Low - Partial data or uncertain matching
- **0.20**: Very Low - Minimal data, needs research

### Verification Status
- `csv_imported`: Initial import from source data
- `llm_suggested`: Enhanced by AI research
- `llm_verified`: Cross-referenced by multiple sources
- `human_verified`: Manually reviewed and confirmed
- `conflicting`: Contradictory information found

## Key Features (v2.0.0)

### Entity Resolution
- **Country Detection**: Auto-detect ISO codes from facility data
- **Metal Normalization**: 95%+ coverage with chemical formulas
- **Company Resolution**: 3,687 companies with LEI codes and Wikidata links
- **Facility Matching**: Multi-strategy duplicate detection (5 strategies)
- **Parquet Sync**: Export/import EntityIdentity format

### CLI Commands
```bash
# Sync operations
python scripts/facilities.py sync --export                    # Export to parquet
python scripts/facilities.py sync --import facilities.parquet # Import from parquet
python scripts/facilities.py sync --status                    # Database status

# Entity resolution testing
python scripts/facilities.py resolve country "Algeria"        # Country resolution
python scripts/facilities.py resolve metal "Cu"               # Metal normalization
python scripts/facilities.py resolve company "BHP"            # Company matching

# Enhanced import
python scripts/import_from_report_enhanced.py report.txt --country DZ --enhanced
```

### Test Coverage
- **156 comprehensive tests** across all modules
- **98.7% pass rate** (154/156 passing)
- All entity resolution, import, sync, and schema validation

## Documentation

- **[Facilities System Guide](docs/README_FACILITIES.md)** - Complete system documentation (v2.0)
- **[EntityIdentity Integration Plan](docs/ENTITYIDENTITY_INTEGRATION_PLAN.md)** - Complete architecture
- **[Schema Changes v2.0](docs/SCHEMA_CHANGES_V2.md)** - Schema v2.0.0 documentation
- **[Deep Research Workflow](docs/DEEP_RESEARCH_WORKFLOW.md)** - Research enrichment guide
- **[Migration Plan](docs/FACILITIES_MIGRATION_PLAN.md)** - Legacy data migration

## Data Sources

Facilities data compiled from:
- Industry databases
- Company reports
- Geological surveys
- Public mining registries
- Manual curation

## Contributing

When adding or updating facility data:
1. Use the standard JSON schema
2. Include verification information
3. Cite data sources
4. Use accurate coordinates when available

## License

See [LICENSE](LICENSE) file for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/globalstrategic/facilities/issues)
- **Documentation**: [Facilities Guide](docs/README_FACILITIES.md)
