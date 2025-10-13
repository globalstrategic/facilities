# Global Mining & Metals Facilities Database

A comprehensive database of mining, smelting, refining, and processing facilities worldwide for metals and minerals.

## Overview

This repository contains structured facility data organized by country with entity resolution, company linking, and research pipeline integration.

**Current Data**: 8,443 facilities across 129 countries

## Quick Start

```bash
# View a facility
cat facilities/USA/usa-stillwater-east-fac.json

# Run migration script (if updating from source data)
python scripts/migrate_facilities.py

# Check migration report
cat output/migration_logs/migration_report_*.json
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

## Facility Data Format

Each facility is a JSON file with standardized structure:

```json
{
  "facility_id": "usa-stillwater-east-fac",
  "name": "Stillwater East",
  "aliases": ["Stillwater Mine East Boulder"],
  "country_iso3": "USA",
  "location": {
    "lat": 45.416,
    "lon": -109.85,
    "precision": "site"
  },
  "types": ["mine", "concentrator"],
  "commodities": [
    {"metal": "platinum", "primary": true},
    {"metal": "palladium", "primary": false}
  ],
  "status": "operating",
  "owner_links": [],
  "operator_link": null,
  "products": [],
  "sources": [],
  "verification": {
    "status": "csv_imported",
    "confidence": 0.65,
    "last_checked": "2025-10-12T10:00:00",
    "checked_by": "migration_script"
  }
}
```

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

## Documentation

- **[Facilities System Guide](docs/README_FACILITIES.md)** - Complete system documentation
- **[Migration Plan](docs/FACILITIES_MIGRATION_PLAN.md)** - Data migration process
- **[Entity Identity Integration](docs/ENTITY_IDENTITY_INTEGRATION.md)** - Company/country/metal resolution

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
