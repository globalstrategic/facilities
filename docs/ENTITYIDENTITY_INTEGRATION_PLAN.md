# EntityIdentity Integration Plan

## Executive Summary

This document outlines the integration strategy for incorporating `entityidentity` functionality into the facilities codebase. The integration will:

1. **Eliminate manual country code specification** - Auto-detect countries from facility data
2. **Resolve company ownership** - Match operator/owner strings to canonical company IDs
3. **Normalize metals/commodities** - Use standardized metal identifiers
4. **Enable facility matching** - Leverage geo-encoded facility database for duplicate detection
5. **Improve data quality** - Higher confidence scores through canonical entity linking

---

## Current State Analysis

### Facilities Codebase

**Storage Structure:**
```
facilities/
├── DZ/          # ISO2 codes (Algeria)
├── AF/          # ISO2 codes (Afghanistan)
├── USA/         # ISO3 codes (United States)
└── ZAF/         # ISO3 codes (South Africa)
```

**JSON Schema:** `schemas/facility.schema.json`
- `facility_id`: Pattern `{iso3}-{slug}-fac`
- `country_iso3`: Required ISO3 code (e.g., "DZA", "USA")
- `owner_links[]`: Array with `company_id` pattern `^cmp-[a-z0-9-]+$`
- `operator_link`: Object with `company_id` field
- `commodities[]`: Array with `{metal: string, primary: boolean}`

**Current Limitations:**
1. Manual country code specification required at import time
2. No company resolution - owner/operator names stored as strings only
3. Inconsistent metal normalization (mix of chemical symbols, full names, compounds)
4. Directory uses both ISO2 and ISO3 codes inconsistently
5. Duplicate detection relies on name + coordinate matching only

### EntityIdentity Library

**Available Resolvers:**

1. **Country Resolution** (Stateless)
   - `country_identifier("Algeria")` → `"DZ"` (ISO2)
   - `country_identifiers(["USA", "UK"])` → `["US", "GB"]`
   - Fuzzy matching with pycountry backend

2. **Company Resolution** (Stateful - loads parquet)
   - `EnhancedCompanyMatcher().match_best("BHP", limit=3, min_score=70)`
   - Returns: `company_id`, `company_name`, `score`, `explanation`
   - Data: `enriched_lei_dataset_geocoded_latest.parquet` (~50MB)
   - Includes geo-coordinates for facility proximity matching

3. **Metal Resolution** (Stateless)
   - `metal_identifier("Pt")` → `{"name": "Platinum", "symbol": "Pt", ...}`
   - `match_metal("lithium", k=3)` → List of matches with scores
   - Handles elements, alloys, compounds

4. **Facility Linking** (Experimental)
   - `link_facility(facility_name="Escondida Mine", company_hint="BHP")`
   - Uses geo-encoded parquet files in `entityidentity/tables/facilities/`

**Geo-Encoded Parquet Files:**

Located at: `/Users/willb/Github/GSMC/entityidentity/tables/facilities/`

**companies_20251003_134822.parquet** (52 rows):
```python
Columns: [
    'company_id',           # e.g., "mimosa_jv_50_sibanye_50_implat_a8411a55"
    'company_name',         # e.g., "Mimosa JV (50% Sibanye, 50% Implats)"
    'aliases',              # List of alternative names
    'website',
    'hq_country',           # Country name
    'hq_country_iso2',      # ISO2 code
    'hq_admin1',            # State/province
    'parent_company_id',    # Parent company reference
    'source_count',         # Number of sources
    'created_at_utc',
    'updated_at_utc'
]
```

**facilities_20251003_134822.parquet** (55 rows):
```python
Columns: [
    'facility_id',          # e.g., "mimosa_52f2f3d6"
    'company_id',           # Links to companies table
    'facility_name',        # e.g., "Mimosa"
    'alt_names',            # Alternative names (list)
    'facility_type',        # e.g., "mine", "smelter"
    'country',              # Country name
    'country_iso2',         # ISO2 code
    'admin1',               # State/province
    'city',
    'address',
    'lat',                  # Latitude (float)
    'lon',                  # Longitude (float)
    'geo_precision',        # "exact", "site", "approximate", etc.
    'commodities',          # List of metals/minerals
    'process_stages',       # List of processing types
    'capacity_value',       # Production capacity
    'capacity_unit',        # Unit of measurement
    'capacity_asof',        # Date of capacity data
    'operating_status',     # "operating", "closed", etc.
    'evidence_urls',        # List of source URLs
    'evidence_titles',      # List of source titles
    'confidence',           # Confidence score (0-1)
    'is_verified',          # Boolean
    'verification_notes',   # String
    'first_seen_utc',
    'last_seen_utc',
    'source'                # Data source
]
```

---

## Integration Architecture

### Phase 1: Core Entity Resolution

#### 1.1 Country Auto-Detection

**Current:** User must specify `--country DZA` at import time

**Proposed:** Auto-detect from facility data or text

```python
from entityidentity import country_identifier

def detect_country_from_facility(facility_data: dict) -> str:
    """Auto-detect country ISO3 code from facility data.

    Tries multiple strategies:
    1. Explicit country field in data
    2. Location-based geocoding (if coordinates available)
    3. Company headquarters country (if operator known)
    """
    # Strategy 1: Direct country field
    if 'country' in facility_data:
        iso2 = country_identifier(facility_data['country'])
        return iso2_to_iso3(iso2)

    # Strategy 2: Geocode coordinates (requires geocoding service)
    if facility_data.get('lat') and facility_data.get('lon'):
        country_name = reverse_geocode(
            facility_data['lat'],
            facility_data['lon']
        )
        iso2 = country_identifier(country_name)
        return iso2_to_iso3(iso2)

    # Strategy 3: Fallback to manual specification
    raise ValueError("Cannot auto-detect country, please specify --country")
```

**Implementation Files:**
- Modify: `scripts/import_from_report.py` - Remove `--country` requirement
- Add: `scripts/utils/country_detection.py` - New module for auto-detection
- Modify: `scripts/facilities.py` - Update CLI to make `--country` optional

#### 1.2 Company Resolution

**Current:** Operator/owner stored as raw strings, no canonical IDs

**Proposed:** Resolve to canonical company IDs using `EnhancedCompanyMatcher`

```python
from entityidentity.companies import EnhancedCompanyMatcher

class FacilityCompanyResolver:
    """Resolve company names to canonical IDs for facilities."""

    def __init__(self):
        self.matcher = EnhancedCompanyMatcher()
        self._cache = {}  # Cache resolved companies within session

    def resolve_operator(
        self,
        operator_name: str,
        country_hint: str = None,
        facility_coords: tuple = None
    ) -> dict:
        """Resolve operator to canonical company ID.

        Args:
            operator_name: Raw operator name from import data
            country_hint: ISO2/ISO3 country code for filtering
            facility_coords: (lat, lon) tuple for proximity matching

        Returns:
            {
                "company_id": "cmp-bhp-billiton",
                "confidence": 0.92,
                "match_explanation": "Exact name match with alias"
            }
        """
        cache_key = (operator_name, country_hint)
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Match with entityidentity
        results = self.matcher.match_best(
            operator_name,
            limit=3,
            min_score=70
        )

        if not results:
            return None

        best_match = results[0]

        # Apply proximity boost if coordinates available
        if facility_coords and hasattr(best_match, 'hq_lat'):
            distance_km = haversine_distance(
                facility_coords,
                (best_match.hq_lat, best_match.hq_lon)
            )
            if distance_km < 100:  # Within 100km of HQ
                best_match['confidence'] += 0.05

        # Convert to facilities schema format
        result = {
            "company_id": f"cmp-{best_match['company_id']}",
            "confidence": best_match['score'] / 100,
            "match_explanation": best_match.get('explanation', '')
        }

        self._cache[cache_key] = result
        return result

    def resolve_owners(
        self,
        owner_text: str,
        country_hint: str = None
    ) -> list:
        """Parse and resolve ownership structure.

        Handles formats like:
        - "BHP (60%), Rio Tinto (40%)"
        - "Sibanye-Stillwater"
        - "Joint venture: Anglo American Platinum 50%, Impala Platinum 50%"
        """
        # Parse ownership percentages
        ownership_pattern = r'([^,\(]+?)\s*\((\d+(?:\.\d+)?)\s*%\)'
        matches = re.findall(ownership_pattern, owner_text)

        owner_links = []
        for company_name, percentage in matches:
            resolved = self.resolve_operator(
                company_name.strip(),
                country_hint
            )
            if resolved:
                owner_links.append({
                    "company_id": resolved['company_id'],
                    "role": "owner" if float(percentage) > 50 else "minority_owner",
                    "percentage": float(percentage),
                    "confidence": resolved['confidence']
                })

        return owner_links
```

**Implementation Files:**
- Add: `scripts/utils/company_resolver.py` - New module
- Modify: `scripts/import_from_report.py` - Add company resolution step
- Requires: Update `requirements.txt` to include `entityidentity`

#### 1.3 Metal Normalization

**Current:** Mix of formats - "copper", "Cu", "lithium carbonate", "PGM"

**Proposed:** Standardize using `metal_identifier()`

```python
from entityidentity import metal_identifier, match_metal

def normalize_commodity(commodity_string: str) -> dict:
    """Normalize commodity name to canonical form.

    Args:
        commodity_string: Raw metal/commodity name

    Returns:
        {
            "metal": "platinum",
            "chemical_formula": "Pt",
            "category": "precious_metal"
        }
    """
    # Try exact match first
    result = metal_identifier(commodity_string)
    if result:
        return {
            "metal": result['name'].lower(),
            "chemical_formula": result.get('symbol'),
            "category": result.get('category')
        }

    # Try fuzzy match
    matches = match_metal(commodity_string, k=1)
    if matches and matches[0]['score'] > 85:
        return {
            "metal": matches[0]['name'].lower(),
            "chemical_formula": matches[0].get('symbol'),
            "category": matches[0].get('category')
        }

    # Fallback: return as-is with warning
    logger.warning(f"Could not normalize metal: {commodity_string}")
    return {
        "metal": commodity_string.lower(),
        "chemical_formula": None,
        "category": "unknown"
    }
```

**Implementation Files:**
- Add: `scripts/utils/metal_normalizer.py` - New module
- Modify: `scripts/import_from_report.py` - Replace `normalize_metal()` function
- Update: `schemas/facility.schema.json` - Add optional `chemical_formula` field to commodities

---

### Phase 2: Facility Matching & Deduplication

#### 2.1 Enhanced Duplicate Detection

**Current:** Name + coordinate matching only (1km radius)

**Proposed:** Multi-strategy matching using entityidentity facilities parquet

```python
import pandas as pd
from pathlib import Path

class FacilityMatcher:
    """Match facilities using entityidentity geo-encoded database."""

    def __init__(self):
        # Load entityidentity facilities database
        parquet_path = Path("/Users/willb/Github/GSMC/entityidentity/tables/facilities")
        latest_facilities = max(parquet_path.glob("facilities_*.parquet"))
        self.ei_facilities = pd.read_parquet(latest_facilities)

        # Load local facilities database
        self.local_facilities = self._load_local_facilities()

    def _load_local_facilities(self) -> pd.DataFrame:
        """Load all local facility JSONs into DataFrame."""
        facilities = []
        facilities_dir = Path("facilities")

        for country_dir in facilities_dir.iterdir():
            if not country_dir.is_dir():
                continue

            for fac_file in country_dir.glob("*.json"):
                with open(fac_file) as f:
                    facilities.append(json.load(f))

        return pd.DataFrame(facilities)

    def find_duplicates(
        self,
        facility: dict,
        strategies: list = None
    ) -> list:
        """Find potential duplicates using multiple strategies.

        Strategies:
        1. Exact name match (case-insensitive)
        2. Name + location proximity (5km radius)
        3. Alias match
        4. Company + commodity match (within 50km)
        5. Cross-reference with entityidentity database

        Returns:
            List of duplicate candidates with confidence scores
        """
        if strategies is None:
            strategies = ['name', 'location', 'alias', 'company', 'entityidentity']

        candidates = []

        # Strategy 1: Exact name match
        if 'name' in strategies:
            name_matches = self.local_facilities[
                self.local_facilities['name'].str.lower() ==
                facility['name'].lower()
            ]
            for _, match in name_matches.iterrows():
                candidates.append({
                    "facility_id": match['facility_id'],
                    "strategy": "exact_name",
                    "confidence": 0.95
                })

        # Strategy 2: Location proximity
        if 'location' in strategies and facility['location']['lat']:
            lat, lon = facility['location']['lat'], facility['location']['lon']

            # Vectorized distance calculation
            local_with_coords = self.local_facilities.dropna(
                subset=['location.lat', 'location.lon']
            )
            distances = haversine_vectorized(
                lat, lon,
                local_with_coords['location.lat'],
                local_with_coords['location.lon']
            )

            nearby = local_with_coords[distances < 5.0]  # 5km radius
            for idx, match in nearby.iterrows():
                candidates.append({
                    "facility_id": match['facility_id'],
                    "strategy": "location_proximity",
                    "confidence": 0.90,
                    "distance_km": distances[idx]
                })

        # Strategy 5: Cross-reference with entityidentity
        if 'entityidentity' in strategies:
            ei_matches = self._match_against_ei_database(facility)
            candidates.extend(ei_matches)

        # Deduplicate and rank candidates
        return self._rank_candidates(candidates)

    def _match_against_ei_database(self, facility: dict) -> list:
        """Match against entityidentity facilities database."""
        matches = []

        # Name-based matching
        from rapidfuzz import fuzz

        for _, ei_fac in self.ei_facilities.iterrows():
            name_score = fuzz.ratio(
                facility['name'].lower(),
                ei_fac['facility_name'].lower()
            )

            if name_score > 85:
                # Check if we already have this facility linked
                existing = self.local_facilities[
                    self.local_facilities.get('ei_facility_id') == ei_fac['facility_id']
                ]

                if len(existing) > 0:
                    matches.append({
                        "facility_id": existing.iloc[0]['facility_id'],
                        "strategy": "entityidentity_name",
                        "confidence": name_score / 100,
                        "ei_facility_id": ei_fac['facility_id']
                    })

        return matches
```

**Implementation Files:**
- Add: `scripts/utils/facility_matcher.py` - New module
- Modify: `scripts/import_from_report.py` - Replace `check_duplicate()` function
- Add: Schema field `ei_facility_id` to link with entityidentity database

#### 2.2 Bi-directional Sync

**Goal:** Keep facilities database in sync with entityidentity parquet files

```python
class FacilitySyncManager:
    """Manage synchronization between facilities JSONs and entityidentity parquets."""

    def export_to_entityidentity_format(
        self,
        output_path: Path
    ) -> Path:
        """Export facilities to entityidentity parquet format.

        Converts all facility JSONs to parquet matching schema:
        facilities_YYYYMMDD_HHMMSS.parquet
        companies_YYYYMMDD_HHMMSS.parquet
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Load all facilities
        facilities = []
        for country_dir in Path("facilities").iterdir():
            if not country_dir.is_dir():
                continue
            for fac_file in country_dir.glob("*.json"):
                with open(fac_file) as f:
                    facilities.append(json.load(f))

        # Convert to DataFrame
        df = pd.DataFrame([
            {
                "facility_id": f['facility_id'],
                "company_id": f.get('operator_link', {}).get('company_id'),
                "facility_name": f['name'],
                "alt_names": f.get('aliases', []),
                "facility_type": f['types'][0] if f['types'] else None,
                "country": iso3_to_country_name(f['country_iso3']),
                "country_iso2": iso3_to_iso2(f['country_iso3']),
                "lat": f['location'].get('lat'),
                "lon": f['location'].get('lon'),
                "geo_precision": f['location'].get('precision'),
                "commodities": [c['metal'] for c in f.get('commodities', [])],
                "operating_status": f.get('status'),
                "confidence": f['verification'].get('confidence'),
                "is_verified": f['verification']['status'] in ['human_verified', 'llm_verified'],
                "verification_notes": f['verification'].get('notes'),
                "first_seen_utc": f['verification']['last_checked'],
                "last_seen_utc": f['verification']['last_checked'],
                "source": f['sources'][0]['type'] if f['sources'] else None
            }
            for f in facilities
        ])

        # Export
        output_file = output_path / f"facilities_{timestamp}.parquet"
        df.to_parquet(output_file, index=False)

        logger.info(f"Exported {len(df)} facilities to {output_file}")
        return output_file

    def import_from_entityidentity(
        self,
        parquet_path: Path,
        overwrite: bool = False
    ):
        """Import facilities from entityidentity parquet.

        Creates new facility JSONs for any facilities in parquet
        that don't exist in local database.
        """
        df = pd.read_parquet(parquet_path)

        imported = 0
        skipped = 0

        for _, row in df.iterrows():
            # Check if facility already exists
            facility_id = row['facility_id']

            if self._facility_exists(facility_id) and not overwrite:
                skipped += 1
                continue

            # Convert to facility JSON format
            facility = self._parquet_row_to_facility(row)

            # Write to file
            country_dir = Path("facilities") / facility['country_iso3']
            country_dir.mkdir(exist_ok=True)

            output_file = country_dir / f"{facility_id}.json"
            with open(output_file, 'w') as f:
                json.dump(facility, f, indent=2)

            imported += 1

        logger.info(f"Imported {imported} facilities, skipped {skipped} existing")
```

**Implementation Files:**
- Add: `scripts/utils/facility_sync.py` - New module
- Add: CLI commands in `scripts/facilities.py`:
  - `python scripts/facilities.py sync --export` - Export to parquet
  - `python scripts/facilities.py sync --import facilities.parquet` - Import from parquet

---

### Phase 3: Import Pipeline Enhancement

#### 3.1 Enhanced Import Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│                    ENHANCED IMPORT PIPELINE                      │
└─────────────────────────────────────────────────────────────────┘

1. TEXT INPUT
   ├─ Research report (markdown tables)
   ├─ CSV file
   └─ Manual paste

2. TABLE EXTRACTION
   └─ extract_markdown_tables() [UNCHANGED]

3. ENTITY RESOLUTION (NEW)
   ├─ Country detection
   │  ├─ Auto-detect from text
   │  └─ Fallback to --country flag
   │
   ├─ Metal normalization
   │  └─ metal_identifier() for each commodity
   │
   └─ Company resolution
      ├─ Operator matching
      └─ Ownership parsing

4. FACILITY MATCHING (ENHANCED)
   ├─ Local database search
   ├─ EntityIdentity cross-reference
   └─ Multi-strategy ranking

5. VALIDATION
   ├─ Schema validation [UNCHANGED]
   ├─ Confidence scoring (boosted by entity resolution)
   └─ Manual review flagging

6. OUTPUT
   ├─ Facility JSON files
   ├─ Import log
   └─ Optional: Export to entityidentity parquet
```

#### 3.2 Modified Import Function

```python
def process_report_enhanced(
    report_text: str,
    country_iso3: str = None,  # Now optional
    source_name: str = None,
    auto_resolve: bool = True
) -> Dict:
    """Enhanced pipeline with entity resolution."""

    # Initialize resolvers (cached for session)
    company_resolver = FacilityCompanyResolver() if auto_resolve else None
    facility_matcher = FacilityMatcher()

    # Extract tables [UNCHANGED]
    tables = extract_markdown_tables(report_text)

    # Auto-detect country if not provided
    if not country_iso3 and tables:
        country_iso3 = detect_country_from_tables(tables)
        logger.info(f"Auto-detected country: {country_iso3}")

    # Load existing facilities
    existing = load_existing_facilities(country_iso3)

    facilities = []
    for table in tables:
        for row in table['rows']:
            # Parse base facility data
            facility = parse_facility_row(row, country_iso3)

            # ENHANCED: Normalize commodities
            if auto_resolve:
                facility['commodities'] = [
                    {
                        **normalize_commodity(c['metal']),
                        "primary": c['primary']
                    }
                    for c in facility['commodities']
                ]

            # ENHANCED: Resolve companies
            if auto_resolve and row.get('operator'):
                operator_result = company_resolver.resolve_operator(
                    row['operator'],
                    country_hint=country_iso3,
                    facility_coords=(facility['location']['lat'],
                                   facility['location']['lon'])
                )

                if operator_result:
                    facility['operator_link'] = {
                        "company_id": operator_result['company_id'],
                        "confidence": operator_result['confidence']
                    }
                    # Boost overall confidence
                    facility['verification']['confidence'] += 0.1

            # ENHANCED: Multi-strategy duplicate detection
            duplicates = facility_matcher.find_duplicates(
                facility,
                strategies=['name', 'location', 'alias', 'entityidentity']
            )

            if duplicates and duplicates[0]['confidence'] > 0.85:
                logger.info(f"Duplicate found: {duplicates[0]}")
                continue

            facilities.append(facility)

    return {
        "facilities": facilities,
        "country_iso3": country_iso3,
        # ... rest of result dict
    }
```

---

## Implementation Roadmap

### Phase 1: Foundation (Week 1)
- [ ] Add `entityidentity` to `requirements.txt`
- [ ] Create `scripts/utils/` directory structure
- [ ] Implement `country_detection.py` module
- [ ] Implement `metal_normalizer.py` module
- [ ] Add unit tests for entity resolution

### Phase 2: Company Resolution (Week 2)
- [ ] Implement `company_resolver.py` module
- [ ] Add ownership parsing logic
- [ ] Update facility schema to support company IDs
- [ ] Test with sample import data
- [ ] Add integration tests

### Phase 3: Facility Matching (Week 3)
- [ ] Implement `facility_matcher.py` module
- [ ] Add vectorized distance calculations
- [ ] Implement multi-strategy duplicate detection
- [ ] Add `ei_facility_id` to schema
- [ ] Test with existing facilities database

### Phase 4: Sync & Export (Week 4)
- [ ] Implement `facility_sync.py` module
- [ ] Add export to parquet functionality
- [ ] Add import from parquet functionality
- [ ] Create CLI commands for sync operations
- [ ] Documentation and examples

### Phase 5: Integration Testing (Week 5)
- [ ] End-to-end testing with real data
- [ ] Performance benchmarking
- [ ] Confidence score validation
- [ ] User acceptance testing
- [ ] Bug fixes and refinements

---

## Migration Strategy

### Backward Compatibility

**Option 1: Gradual Migration (Recommended)**
- Keep existing import pipeline working as-is
- Add new `--enhanced` flag to opt-in to entity resolution
- Migrate country-by-country to new system
- Validate results before full cutover

```bash
# Old way (still works)
python scripts/facilities.py import report.txt --country DZ

# New way (opt-in)
python scripts/facilities.py import report.txt --enhanced
```

**Option 2: Parallel Systems**
- Maintain both old and new import pipelines
- Use new system for new countries
- Gradually backfill existing countries
- Remove old system after 100% migration

### Data Quality Validation

Before cutting over to enhanced system:

1. **Duplicate Detection Accuracy**
   - Test against known duplicate sets
   - Measure precision/recall
   - Target: >95% precision, >90% recall

2. **Company Resolution Accuracy**
   - Manual review of 100 random matches
   - Validate confidence scores align with quality
   - Target: >90% correct at confidence >0.8

3. **Metal Normalization Coverage**
   - List all unique metals in current database
   - Measure coverage by metal_identifier()
   - Target: >95% coverage

---

## Dependencies & Requirements

### Python Packages

```txt
# Core dependencies
entityidentity>=0.1.0
pandas>=1.5.0
pycountry>=22.0.0

# For enhanced matching
rapidfuzz>=3.0.0
scikit-learn>=1.3.0  # For vectorized distance calculations

# Optional (for geocoding)
geopy>=2.3.0
anthropic>=0.18.0  # For LLM-based geocoding
```

### Data Files

1. **EntityIdentity Parquets** (Required)
   - `enriched_lei_dataset_geocoded_latest.parquet` (~50MB)
   - Located in `entityidentity/entityidentity/companies/data/`

2. **Facility Parquets** (Optional - for cross-reference)
   - `facilities_YYYYMMDD_HHMMSS.parquet`
   - `companies_YYYYMMDD_HHMMSS.parquet`
   - Located in `entityidentity/tables/facilities/`

### System Requirements

- Python 3.9+
- 4GB RAM minimum (for loading parquet files)
- Disk space: ~500MB for all data files

---

## Testing Strategy

### Unit Tests

```python
# tests/test_entity_resolution.py
def test_country_detection():
    """Test country auto-detection from facility data."""
    facility = {"name": "Test Mine", "country": "Algeria"}
    assert detect_country(facility) == "DZA"

def test_company_resolution():
    """Test company matching."""
    resolver = FacilityCompanyResolver()
    result = resolver.resolve_operator("BHP Billiton")
    assert result['company_id'].startswith("cmp-")
    assert result['confidence'] > 0.8

def test_metal_normalization():
    """Test metal normalization."""
    assert normalize_commodity("Cu")['metal'] == "copper"
    assert normalize_commodity("Platinum")['chemical_formula'] == "Pt"
```

### Integration Tests

```python
# tests/test_enhanced_import.py
def test_full_import_pipeline():
    """Test complete enhanced import."""
    report_text = load_test_report("algeria_sample.txt")
    result = process_report_enhanced(report_text, auto_resolve=True)

    assert result['country_iso3'] == "DZA"
    assert len(result['facilities']) > 0

    # Check entity resolution applied
    fac = result['facilities'][0]
    assert 'chemical_formula' in fac['commodities'][0]
    assert fac['operator_link'] is not None
    assert fac['operator_link']['company_id'].startswith("cmp-")
```

### Benchmark Tests

```python
# tests/test_performance.py
def test_import_performance():
    """Ensure enhanced import doesn't slow down pipeline."""
    import time

    report_text = load_large_report("1000_facilities.txt")

    start = time.time()
    result = process_report_enhanced(report_text, auto_resolve=True)
    elapsed = time.time() - start

    # Should process 1000 facilities in <60 seconds
    assert elapsed < 60
    assert len(result['facilities']) == 1000
```

---

## Open Questions

1. **Company ID Format:** Should we convert entityidentity company IDs to match our schema pattern (`cmp-{slug}`)? Or keep their IDs as-is?

2. **Directory Structure:** Should we migrate all directories to ISO3 codes for consistency? Or continue supporting both ISO2/ISO3?

3. **Confidence Score Calculation:** How should we weight entity resolution matches when calculating overall facility confidence?

4. **Parquet Update Frequency:** How often should we sync with entityidentity parquet files? Manual, daily, weekly?

5. **Fallback Strategy:** What should happen when entity resolution fails? Keep raw strings, flag for manual review, or block import?

---

## Success Metrics

- **Automation:** Reduce manual country specification to <10% of imports
- **Accuracy:** Achieve >95% correct company matches at confidence >0.8
- **Deduplication:** Reduce duplicate facilities in database by >50%
- **Data Quality:** Increase average facility confidence score from 0.65 to 0.80
- **Coverage:** Achieve >95% metal normalization coverage

---

## Next Steps

1. **Review & Approval:** Get stakeholder sign-off on integration approach
2. **Proof of Concept:** Implement Phase 1 (country + metal resolution) as POC
3. **Validation:** Test POC on 100 sample facilities across 3 countries
4. **Iterate:** Refine based on POC results
5. **Full Implementation:** Roll out remaining phases after successful POC
