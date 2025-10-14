# Facilities Migration Plan

## Executive Summary

Migrate 8,508 facilities from `Mines.csv` to a structured JSON format in `facilities/`, with per-metal indexes and integration with entityidentity for company resolution.

## Current State

### Data Sources
1. **Mines.csv**: 8,508 facilities with columns:
   - Confidence Factor (Very High, High, Moderate, Low, Very Low)
   - Mine Name
   - Group Names (semicolon-separated aliases)
   - Latitude, Longitude
   - Asset Type (Mine, Smelter, Plant, etc.)
   - Country or Region
   - Primary/Secondary/Other Commodities

2. **Company Data**:
   - 1,002 unique companies in `config/supply/` JSON files
   - Enriched company data in `output/latest_results/`:
     - LEI codes (876,661 companies)
     - LinkedIn matches (59 companies)
     - Wikipedia matches (available)

3. **EntityIdentity**: Entity resolution library with:
   - Company canonicalization
   - Country normalization (ISO codes)
   - Metal standardization
   - Facility linking (stub implementation)

## Target Architecture

```
config/
  facilities/                       # Canonical facility files
    USA/                           # ISO3 country directories
      usa-stillwater-east-fac.json
    ZAF/
      zaf-rustenburg-karee-fac.json
    ...
  supply/
    aluminum/
      facilities.index.json        # References to canonical facilities
      mining.json                  # Existing company lists
      smelting.json
    platinum/
      facilities.index.json
      mining.json
    ...
  mappings/
    company_canonical.json         # Company name → canonical ID mapping
    country_iso.json              # Country name → ISO3 mapping
    metal_canonical.json          # Metal variations → standard names

output/
  research_raw/                   # Raw Gemini Deep Research outputs
    aluminum/
      USA.jsonl
      CAN.jsonl
    platinum/
      ZAF.jsonl
  research_evidence/              # Supporting documents, URLs
  migration_logs/                 # Audit trail of migration process

schemas/
  facility.schema.json            # JSON Schema for validation
  company.schema.json
```

## Facility JSON Schema

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
    {"metal": "palladium", "primary": false},
    {"metal": "rhodium", "primary": false}
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
  "operator_link": {
    "company_id": "cmp-implats",
    "confidence": 0.95
  },
  "products": [
    {
      "stream": "PGM concentrate",
      "capacity": null,
      "unit": null,
      "year": null
    }
  ],
  "sources": [
    {"type": "mines_csv", "row": 1234},
    {"type": "gemini_research", "id": "2025-10-12-platinum-ZAF"},
    {"type": "wikipedia", "url": "..."}
  ],
  "verification": {
    "status": "llm_suggested",
    "confidence": 0.65,
    "last_checked": "2025-10-12",
    "checked_by": "gemini_deep_research"
  }
}
```

## Migration Phases

### Phase 1: Data Preparation (Week 1)
1. **Clean Mines.csv**
   - Standardize column names
   - Fix encoding issues
   - Validate coordinates
   - Deduplicate entries

2. **Create Canonical Mappings**
   - Run all countries through entityidentity → ISO3
   - Normalize all metal names
   - Create initial company mapping from existing JSON files

3. **Setup Directory Structure**
   - Create all ISO3 directories
   - Initialize schemas
   - Setup validation tools

### Phase 2: Initial Migration (Week 1)
1. **Run Migration Script**
   - Convert CSV → JSON facilities
   - Apply canonical mappings
   - Generate per-metal indexes
   - Create migration audit log

2. **Validation**
   - Schema validation for all JSONs
   - Cross-reference metal lists
   - Verify geographic coordinates
   - Check facility ID uniqueness

### Phase 3: Company Linking (Week 2)
1. **EntityIdentity Integration**
   - Run all company names through entityidentity
   - Create canonical company registry
   - Link facilities to companies where possible
   - Track linking confidence scores

2. **Manual Review**
   - Review low-confidence matches
   - Identify missing companies
   - Flag facilities needing research

### Phase 4: Gemini Deep Research (Weeks 2-4)
1. **Batch Processing**
   - Group facilities by (country, metal)
   - Create research prompts with existing data
   - Require source citations for all facts

2. **Research Pipeline**
   ```
   For each (country, metal) batch:
   1. Extract facility names from batch
   2. Include existing data as context
   3. Send to Gemini Deep Research
   4. Save raw response to research_raw/
   5. Parse and validate response
   6. Merge with existing facilities
   7. Update verification status
   ```

3. **Evidence Collection**
   - Store URLs and sources
   - Capture screenshots of key pages
   - Track data lineage

### Phase 5: Quality Assurance (Week 4)
1. **Cross-Validation**
   - Compare against existing company lists
   - Verify against public databases
   - Check for logical consistency

2. **Confidence Scoring**
   - Update confidence based on source quality
   - Flag discrepancies for review
   - Prioritize human verification

## Implementation Details

### Migration Script Structure

```python
# scripts/migrate_facilities.py

import sys
sys.path.append('../entityidentity')

from entityidentity import (
    company_identifier,
    country_identifier,
    metal_identifier
)

class FacilityMigrator:
    def __init__(self):
        self.company_cache = {}
        self.country_cache = {}
        self.metal_cache = {}
        self.stats = defaultdict(int)

    def migrate_csv_to_json(self):
        """Main migration logic"""
        pass

    def normalize_country(self, country_name):
        """Use entityidentity for country normalization"""
        if country_name not in self.country_cache:
            iso2 = country_identifier(country_name)
            iso3 = self.iso2_to_iso3(iso2) if iso2 else None
            self.country_cache[country_name] = iso3
        return self.country_cache[country_name]

    def resolve_company(self, company_name, country=None):
        """Use entityidentity for company resolution"""
        key = (company_name, country)
        if key not in self.company_cache:
            canonical = company_identifier(company_name, country)
            if canonical:
                company_id = f"cmp-{self.slugify(canonical)}"
                self.company_cache[key] = {
                    'company_id': company_id,
                    'canonical_name': canonical,
                    'confidence': 0.85
                }
        return self.company_cache.get(key)

    def build_metal_indexes(self):
        """Create per-metal facility indexes"""
        pass
```

### Gemini Research Prompt Template

```
You are analyzing mining facilities for {metal} in {country}.

Existing facility data:
{json_facilities}

For each facility, please provide:
1. Current operational status
2. Owner companies with ownership percentages
3. Operating company
4. Production capacity (if available)
5. Key products/streams
6. Recent developments or changes

Requirements:
- Output valid JSON matching this schema: {schema}
- Include source URLs for every fact
- Mark confidence level for each piece of information
- Flag any contradictory information found

Focus on accuracy over completeness. Only include verified information.
```

### Validation Rules

1. **Required Fields**
   - facility_id (unique)
   - name
   - country_iso3
   - types (at least one)
   - verification status

2. **Consistency Checks**
   - Primary commodity must be in commodities list
   - Ownership percentages ≤ 100%
   - Valid ISO3 country codes
   - Valid latitude/longitude ranges

3. **Cross-References**
   - Company IDs must exist in company registry
   - Metal names must be canonical
   - No duplicate facility IDs

## Success Metrics

1. **Coverage**
   - 100% of Mines.csv entries migrated
   - ≥80% with verified company links
   - ≥90% with standardized metal names

2. **Quality**
   - 100% schema compliance
   - ≥70% with confidence ≥ 0.65
   - ≥50% with external sources

3. **Integration**
   - All facilities accessible via metal indexes
   - EntityIdentity integration functional
   - Research pipeline operational

## Risk Mitigation

1. **Data Loss**
   - Keep original Mines.csv unchanged
   - Version control all changes
   - Create backups before each phase

2. **Quality Issues**
   - Manual review of low-confidence matches
   - Gradual rollout with pilot metals
   - Maintain audit trail

3. **Performance**
   - Batch processing for large operations
   - Caching for repeated lookups
   - Incremental index updates

## Timeline

- **Week 1**: Data preparation, initial migration
- **Week 2**: Company linking, research setup
- **Weeks 3-4**: Gemini Deep Research processing
- **Week 4**: Quality assurance, finalization

## Next Steps

1. **Immediate Actions**
   - Review and approve migration plan
   - Set up directory structure
   - Install entityidentity dependencies

2. **Day 1-2**
   - Clean and validate Mines.csv
   - Create canonical mappings
   - Write migration script

3. **Day 3-5**
   - Run initial migration
   - Validate outputs
   - Begin company linking

4. **Week 2+**
   - Start Gemini Deep Research
   - Iterative refinement
   - Documentation updates