# EntityIdentity Integration Guide

## Overview

EntityIdentity is a powerful entity resolution library located in the parent directory (`../entityidentity/`) that provides fast, in-memory entity resolution for companies, countries, metals, and facilities using fuzzy matching and smart normalization.

## Key Capabilities

### 1. Company Resolution
```python
from entityidentity import company_identifier, match_company

# Get canonical identifier
company_identifier("BHP")  # → 'BHP Group Limited:AU'
company_identifier("AAPL")  # → 'Apple Inc:US'

# Get full company record
match_company("BHP", "AU")
# Returns: {'name': 'BHP Group Limited', 'country': 'AU', 'lei': '...', ...}
```

**Features:**
- 655+ pre-filtered mining/metals companies
- Handles tickers, abbreviations, variations
- Returns stable canonical identifiers
- Includes LEI codes, Wikidata QIDs, aliases

### 2. Country Resolution
```python
from entityidentity import country_identifier

country_identifier("USA")            # → 'US'
country_identifier("United Kingdom") # → 'GB'
country_identifier("Untied States")  # → 'US' (typo tolerance!)
```

**Features:**
- 249 ISO-compliant countries
- Fuzzy matching for typos
- Returns ISO 2-letter codes

### 3. Metal Resolution
```python
from entityidentity import metal_identifier

metal_identifier("Pt")                # → {'name': 'Platinum', 'symbol': 'Pt', ...}
metal_identifier("lithium carbonate") # → {'name': 'Lithium carbonate', 'formula': 'Li2CO3', ...}
```

**Features:**
- 50+ elements, alloys, and compounds
- Chemical formula recognition
- Supply chain clustering

### 4. Facility Linking (Stub Implementation)
```python
from entityidentity.facilities import FacilityLinker

linker = FacilityLinker(facilities_path="path/to/facilities.parquet")
result = linker.link(
    facility_name="Karee Mine",
    company_hint="Implats",
    latitude=-25.666,
    longitude=27.202
)
```

**Current Features:**
- Geographic distance scoring (Haversine)
- Name fuzzy matching
- Company resolution integration
- Probabilistic linking

**Note:** The facility module is currently a stub implementation waiting for data integration.

### 5. Additional Modules

- **Baskets**: Multi-metal composites (PGM 4E/5E, NdPr, REE Light)
- **Periods**: Temporal normalization (years, quarters, months)
- **Places**: Geographic locations with country blocking
- **Units**: Unit conversion and basis normalization
- **Instruments**: Price ticker resolution

## Performance Characteristics

- **<100ms** query latency
- **In-memory** operation (~50-200MB RAM)
- **LRU caching** for session persistence
- **Zero infrastructure** - pure Python

## Current Data Flow in talloy

### Existing Company Data
1. **Source configs**: `config/supply/{metal}/mining.json`, `smelting.json`
2. **Aggregated data**: `output/latest_results/` containing:
   - `all_companies.json` - Complete company list
   - `lei_companies.json` - Companies with LEI codes
   - `linkedin_companies.json` - LinkedIn-matched companies
   - `wikipedia_companies.json` - Wikipedia-matched companies

### Company Record Structure
```json
{
  "name": "BHP Group Limited",
  "alternatives": ["BHP", "BHP Billiton"],
  "country": "Australia",
  "type": "Mining Company",
  "specialization": "Iron Ore, Copper, Coal",
  "identifier": "LEI:549300C116EEXG6RAL38",
  "match_type": "lei",
  "confidence": "high"
}
```

## Integration Opportunities

### 1. Company ID Standardization
Use entityidentity to create canonical company IDs:

```python
# In migration script
from entityidentity import company_identifier

def get_canonical_company_id(company_name, country=None):
    """Get stable company ID using entityidentity."""
    canonical = company_identifier(company_name, country)
    if canonical:
        return f"cmp-{canonical.replace(':', '-').lower()}"
    return None
```

### 2. Facility-Company Linking
Leverage the FacilityLinker for ownership resolution:

```python
# Link facilities to canonical companies
linker = FacilityLinker()
for facility in facilities:
    if facility.get('owner_name'):
        result = linker.link(
            facility_name=facility['name'],
            company_hint=facility['owner_name'],
            latitude=facility.get('lat'),
            longitude=facility.get('lon')
        )
        if result['company_id']:
            facility['owner_links'].append({
                'company_id': result['company_id'],
                'confidence': result['link_score']
            })
```

### 3. Metal Normalization
Use metal_identifier for consistent commodity coding:

```python
from entityidentity import metal_identifier

def normalize_commodity(commodity_name):
    """Normalize commodity names to standard identifiers."""
    result = metal_identifier(commodity_name)
    if result:
        return {
            'metal': result['name'].lower(),
            'symbol': result.get('symbol'),
            'formula': result.get('formula')
        }
    return {'metal': commodity_name.lower()}
```

### 4. Country Normalization
Ensure consistent ISO codes:

```python
from entityidentity import country_identifier

def normalize_country(country_name):
    """Convert country names to ISO3 codes."""
    iso2 = country_identifier(country_name)
    if iso2:
        # Convert ISO2 to ISO3 (implement mapping)
        return iso2_to_iso3(iso2)
    return country_name[:3].upper()  # Fallback
```

## Recommended Integration Steps

### Phase 1: Company Canonicalization
1. Run all existing company names through entityidentity
2. Create mapping file: `config/mappings/company_canonical.json`
3. Update facility owner/operator references to use canonical IDs

### Phase 2: Facility Data Preparation
1. Create facilities parquet file from Mines.csv
2. Enrich with entityidentity company matches
3. Store in format compatible with FacilityLinker

### Phase 3: Bidirectional Linking
1. Facilities reference companies by canonical ID
2. Companies maintain list of facility IDs
3. Use entityidentity for all new entity resolution

### Phase 4: Research Pipeline Integration
1. Use entityidentity to validate Gemini Deep Research results
2. Cross-reference against LEI/Wikidata/LinkedIn identifiers
3. Track confidence scores from both sources

## CLI Tools Available

EntityIdentity provides CLI commands:

```bash
# Company resolution
python -m entityidentity.cli company "BHP Group"

# Country resolution
python -m entityidentity.cli country "United States"

# Metal resolution
python -m entityidentity.cli metal "platinum"

# Batch processing (if implemented)
python -m entityidentity.cli batch-companies companies.csv
```

## Benefits of Integration

1. **Deduplication**: Automatic detection of duplicate companies across metals
2. **Standardization**: Consistent identifiers across all data sources
3. **Validation**: Cross-reference against authoritative sources (LEI, Wikidata)
4. **Performance**: Fast in-memory lookups replace complex SQL queries
5. **Flexibility**: Easy to extend with new entity types
6. **Maintenance**: Single source of truth for entity resolution logic

## Next Steps

1. **Immediate**: Use entityidentity for company resolution in facilities migration
2. **Short-term**: Create canonical company registry with stable IDs
3. **Medium-term**: Build facilities parquet for FacilityLinker integration
4. **Long-term**: Extend entityidentity with mining-specific entity types