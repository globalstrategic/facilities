# Facilities Data Management System

## Overview

This system manages mining and processing facility data, migrating from the legacy `Mines.csv` format to a structured JSON-based architecture with entity resolution, company linking, and research pipeline integration.

## Quick Start

```bash
# 1. Run the migration script
python scripts/migrate_facilities.py

# 2. Check the migration report
cat output/migration_logs/migration_report_*.json

# 3. View sample facility data
cat facilities/USA/usa-stillwater-east-fac.json

# 4. Check metal-specific facilities
cat config/supply/platinum/facilities.index.json
```

## Architecture

```
talloy/
├── Mines.csv                     # Source data (8,508 facilities)
├── config/
│   ├── facilities/               # Canonical facility JSONs
│   │   ├── USA/                 # Organized by ISO3 country
│   │   ├── ZAF/
│   │   └── ...
│   ├── supply/                  # Per-metal organization
│   │   ├── aluminum/
│   │   │   ├── facilities.index.json  # Metal-specific indexes
│   │   │   ├── mining.json           # Company lists (existing)
│   │   │   └── smelting.json
│   │   └── ...
│   └── mappings/                # Canonical mappings
│       ├── company_canonical.json
│       ├── country_canonical.json
│       └── metal_canonical.json
├── output/
│   ├── migration_logs/          # Migration audit trails
│   ├── research_raw/            # Gemini Deep Research outputs
│   └── latest_results/          # Company enrichment data
├── schemas/
│   └── facility.schema.json     # JSON Schema for validation
└── scripts/
    └── migrate_facilities.py     # Main migration script
```

## Data Model

### Facility JSON Structure

```json
{
  "facility_id": "zaf-rustenburg-karee-fac",
  "name": "Karee",
  "aliases": ["Karee Mine", "Rustenburg Karee"],
  "country_iso3": "ZAF",
  "location": {
    "lat": -25.666,
    "lon": 27.202,
    "precision": "site"
  },
  "types": ["mine", "concentrator"],
  "commodities": [
    {"metal": "platinum", "primary": true},
    {"metal": "palladium", "primary": false}
  ],
  "status": "operating",
  "owner_links": [
    {
      "company_id": "cmp-implats",
      "role": "owner",
      "percentage": 74.0,
      "confidence": 0.95
    }
  ],
  "verification": {
    "status": "csv_imported",
    "confidence": 0.65,
    "last_checked": "2025-10-12T10:00:00"
  }
}
```

## EntityIdentity Integration

The system leverages the `entityidentity` library for:

### Company Resolution
- Canonical company identification
- LEI code matching
- Wikidata QID linking
- Confidence scoring

### Country Normalization
- ISO code standardization
- Fuzzy matching for variations
- Typo tolerance

### Metal Standardization
- Chemical formula recognition
- Alloy/compound resolution
- Supply chain clustering

### Usage Example

```python
from entityidentity import company_identifier, country_identifier, metal_identifier

# Resolve companies
company_id = company_identifier("BHP", "AU")  # → 'BHP Group Limited:AU'

# Normalize countries
iso_code = country_identifier("United States")  # → 'US'

# Standardize metals
metal = metal_identifier("Li2CO3")  # → {'name': 'Lithium carbonate', ...}
```

## Workflow

### 1. Initial Migration (Completed)

```bash
# Run migration script
python scripts/migrate_facilities.py

# This will:
# - Parse 8,508 facilities from Mines.csv
# - Normalize countries → ISO3 codes
# - Standardize metal names
# - Create facility JSONs in facilities/
# - Generate per-metal indexes
# - Create mapping files
```

### 2. Company Linking (Next Step)

```python
# Use entityidentity to link facilities to companies
from entityidentity import match_company

# For each facility with owner/operator hints
company_data = match_company("Impala Platinum", "ZA")
# Link to facility using canonical company ID
```

### 3. Gemini Deep Research (Future)

```bash
# For each (country, metal) batch:
# 1. Extract facility list
# 2. Send to Gemini Deep Research
# 3. Parse and validate results
# 4. Update facility JSONs
# 5. Change verification status
```

## API Usage

### Query Facilities by Metal

```python
import json

# Load platinum facilities
with open('config/supply/platinum/facilities.index.json') as f:
    index = json.load(f)

# Get facility details
for facility_id in index['facilities']:
    country = facility_id.split('-')[0].upper()
    with open(f'facilities/{country}/{facility_id}.json') as f:
        facility = json.load(f)
        print(f"{facility['name']}: {facility['country_iso3']}")
```

### Find Facilities by Company

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
            if facility.get('operator_link', {}).get('company_id') == company_id:
                facilities.append(facility)
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

- `csv_imported`: Initial import from Mines.csv
- `llm_suggested`: Enhanced by Gemini Deep Research
- `llm_verified`: Cross-referenced by multiple LLM sources
- `human_verified`: Manually reviewed and confirmed
- `conflicting`: Contradictory information found

## Statistics

Current migration results:
- **Total Facilities**: 8,508
- **Countries**: 100+ unique ISO3 codes
- **Metals/Commodities**: 50+ types
- **With Coordinates**: ~95%
- **Multi-commodity**: ~40%

## Maintenance

### Update Facility Data

```python
# Load, modify, save
import json

with open('facilities/USA/usa-stillwater-east-fac.json') as f:
    facility = json.load(f)

facility['status'] = 'operating'
facility['verification']['status'] = 'human_verified'

with open('facilities/USA/usa-stillwater-east-fac.json', 'w') as f:
    json.dump(facility, f, indent=2)
```

### Validate Schema

```python
import json
import jsonschema

# Load schema
with open('schemas/facility.schema.json') as f:
    schema = json.load(f)

# Validate facility
with open('facilities/USA/usa-stillwater-east-fac.json') as f:
    facility = json.load(f)

jsonschema.validate(facility, schema)  # Raises exception if invalid
```

### Rebuild Indexes

```python
# Regenerate metal indexes after manual edits
python scripts/rebuild_indexes.py
```

## Future Enhancements

1. **Real-time Updates**
   - WebSocket feed for status changes
   - Production data integration
   - Price impact correlation

2. **Advanced Analytics**
   - Supply chain mapping
   - Geographic clustering
   - Capacity utilization trends

3. **External Integration**
   - S&P Global Market Intelligence
   - Wood Mackenzie data
   - Government mining registries

4. **Research Automation**
   - Scheduled Gemini refreshes
   - News monitoring for changes
   - Satellite imagery analysis

## Troubleshooting

### Common Issues

1. **EntityIdentity not found**
   ```bash
   # Add to PYTHONPATH
   export PYTHONPATH="../entityidentity:$PYTHONPATH"
   ```

2. **Country not resolved**
   - Check `config/mappings/country_canonical.json`
   - Add manual mapping if needed

3. **Duplicate facilities**
   - Check aliases for variations
   - Merge using higher confidence source

4. **Missing companies**
   - Run through entityidentity first
   - Check LEI/Wikidata databases
   - Add to manual mappings if needed

## Related Documentation

- [Entity Identity Integration Guide](./ENTITY_IDENTITY_INTEGRATION.md)
- [Facilities Migration Plan](./FACILITIES_MIGRATION_PLAN.md)
- [Company Data Structure](../config/supply/README.md)

## Contact

For questions or issues with the facilities system, please refer to the main project documentation or create an issue in the repository.