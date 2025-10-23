# Advanced Geocoding System (v2.1)

**Status**: Production-ready
**Version**: 2.1.0
**Last Updated**: 2025-10-21

## Overview

Multi-source geocoding system for mining facilities with intelligent fallback, fuzzy name matching, and confidence scoring. Successfully tested on Kazakhstan uranium JVs with sub-kilometer accuracy.

## Architecture

### Source Priority (Cheap → Robust)

1. **OSM Overpass** - Mining-specific tags (man_made, landuse=quarry, resource=*)
2. **Wikidata SPARQL** - Mine/deposit items with P625 coordinates + multilingual aliases
3. **Mindat API** - Mine localities with site-level coords (requires API key - not yet implemented)
4. **National Cadastres** - Country-specific (SERNAGEOMIN, GEOCATMIN, etc. - not yet implemented)
5. **Web Search** - NI 43-101/JORC technical reports (requires API keys - not yet implemented)
6. **Nominatim** - General geocoder fallback

### Name Matching

- **rapidfuzz** - Token sort ratio + word overlap (handles word order, abbreviations)
- **transliterate** - Cyrillic ↔ Latin transliteration (e.g., "Инкай" ↔ "Inkai")
- **libpostal** - Robust tokenization (optional, requires C library)

### Scoring & Precision

**Source Scores** (base confidence):
- Cadastre / NI 43-101: 0.95
- Mindat / Wikidata: 0.85
- OSM Overpass: 0.75
- Nominatim: 0.60

**Final Confidence** = (source_score × 0.6) + (name_match_score × 0.4)

**Precision Labels**:
- `site` - Specific mine location (cadastre, Mindat, technical reports)
- `city` - City/town centroid
- `region` - Regional/province centroid
- `country` - Country centroid
- `unknown` - No coordinates found

## Usage

### Basic Usage

```python
from scripts.utils.geocoding_v2 import AdvancedGeocoder

# Initialize geocoder
geocoder = AdvancedGeocoder(
    use_overpass=True,      # Enable OSM Overpass
    use_wikidata=True,      # Enable Wikidata SPARQL
    use_nominatim=True,     # Enable Nominatim fallback
    cache_results=True      # Cache results to disk
)

# Geocode a facility
result = geocoder.geocode_facility(
    facility_name="Inkai",
    country_iso3="KAZ",
    commodities=["uranium"],
    aliases=["JV Inkai", "South Inkai"],
    min_confidence=0.6
)

# Access results
print(f"Coordinates: {result.lat}, {result.lon}")
print(f"Precision: {result.precision}")
print(f"Source: {result.source} ({result.source_id})")
print(f"Matched: {result.matched_name}")
print(f"Confidence: {result.confidence:.3f}")
print(f"Match score: {result.match_score:.3f}")
```

### Test Script

```bash
# Test all sources
python scripts/test_geocoding_v2.py

# Test specific source
python scripts/test_geocoding_v2.py --source wikidata
python scripts/test_geocoding_v2.py --source overpass
python scripts/test_geocoding_v2.py --source nominatim

# Disable cache (for testing)
python scripts/test_geocoding_v2.py --no-cache

# Verbose output
python scripts/test_geocoding_v2.py --verbose
```

### Integration with Backfill System

The advanced geocoder can be integrated into the existing backfill system:

```python
from scripts.utils.geocoding_v2 import AdvancedGeocoder

# In backfill_geocode()
geocoder = AdvancedGeocoder(
    use_overpass=True,
    use_wikidata=True,
    use_nominatim=True
)

result = geocoder.geocode_facility(
    facility_name=facility['name'],
    country_iso3=country_iso3,
    commodities=[c['metal'] for c in facility.get('commodities', [])],
    aliases=facility.get('aliases', [])
)

if result.lat and result.lon:
    facility['location'] = {
        'lat': result.lat,
        'lon': result.lon,
        'precision': result.precision
    }
```

## Performance

### Benchmark Results (Kazakhstan Uranium JVs)

| Facility | Source | Accuracy | Match Score | Confidence | Time |
|----------|--------|----------|-------------|------------|------|
| Inkai | Wikidata (Q1627371) | 0.4 km | 1.000 | 0.910 | ~2s |
| South Inkai | Wikidata (Q16977734) | 0.6 km | 1.000 | 0.910 | ~2s |
| Zarechnoye | Wikidata (Q16979252) | 0.4 km | 1.000 | 0.910 | ~2s |

**Success Rate**: 100% (3/3 test cases)
**Average Accuracy**: <1 km from expected coordinates
**Average Confidence**: 0.91

### Performance Characteristics

- **Wikidata**: ~2-3s per query (first query to endpoint, then rate-limited)
- **Overpass**: ~1-2s per query (depends on query complexity)
- **Nominatim**: ~1s per query (rate-limited to 1 req/sec)
- **Cache hits**: <10ms

## Data Sources

### 1. OSM Overpass API

**Endpoint**: https://overpass-api.de/api/interpreter

**Mining Tags**:
- `man_made=mineshaft` - Mine shaft
- `man_made=adit` - Mine adit/entrance
- `landuse=quarry` - Quarry
- `resource=*` - Resource type (uranium, copper, gold, etc.)

**Example Query**:
```overpass
[out:json][timeout:120];
area["ISO3166-1"="KZ"]->.searchArea;
(
  node(area.searchArea)["man_made"~"mineshaft|adit"];
  way(area.searchArea)["landuse"="quarry"];
  nwr(area.searchArea)["resource"="uranium"];
);
out center tags;
```

**Rate Limit**: ~0.5s between requests (conservative for community instances)

### 2. Wikidata SPARQL

**Endpoint**: https://query.wikidata.org/sparql

**Mine Classes**:
- Q386190 - Mine
- Q820477 - Mineral deposit
- Q188076 - Quarry

**Example Query**:
```sparql
SELECT DISTINCT ?item ?itemLabel ?coord ?alias WHERE {
  ?item wdt:P31/wdt:P279* wd:Q386190;  # mine / mining site class
        wdt:P17 wd:Q232;                # Kazakhstan
        wdt:P625 ?coord.                # coordinates
  OPTIONAL { ?item skos:altLabel ?alias. }
  SERVICE wikibase:label {
    bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en,ru".
  }
}
LIMIT 1000
```

**Coverage** (as of 2025-10-21):
- Kazakhstan: 27 mines with coordinates
- Includes major uranium mines (Inkai, South Inkai, Zarechnoye)

**Rate Limit**: ~0.2s between requests (5 req/sec)

### 3. Nominatim (Fallback)

**Endpoint**: https://nominatim.openstreetmap.org/search

**Usage**: City/region lookup when facility-specific geocoding fails

**Rate Limit**: 1 req/sec (OSM Foundation policy)

## Configuration

### Environment Variables

None required for basic operation. Optional API keys:

```bash
# Mindat API (optional)
export MINDAT_API_KEY="your_key_here"

# Web search (optional)
export TAVILY_API_KEY="your_key_here"
export BRAVE_API_KEY="your_key_here"
```

### Cache Directory

Default: `~/.cache/facilities/geocoding/`

Cache files named: `{facility_name}_{country_iso3}_{commodities}.json`

Clear cache:
```bash
rm -rf ~/.cache/facilities/geocoding
```

## Known Limitations

1. **Wikidata Coverage** - Not all mines have Wikidata items
2. **OSM Tagging** - Inconsistent resource tagging across countries
3. **Commodity Filtering** - Disabled in SPARQL (Wikidata's commodity tagging is inconsistent)
4. **Name Matching** - May match wrong facility if names are very similar
5. **Transliteration** - Currently supports Cyrillic ↔ Latin only

## Future Enhancements

### Short-term (v2.2)

- [ ] Mindat API integration (OpenMindat)
- [ ] National cadastre integrations (Chile, Peru, South Africa, Australia)
- [ ] Reverse geocoding validation (sanity checks)
- [ ] Proximity scoring (if lat/lon hints provided)

### Medium-term (v2.3)

- [ ] Web search integration (Tavily/Brave for NI 43-101/JORC reports)
- [ ] libpostal integration (better tokenization)
- [ ] Multi-language support beyond Cyrillic/Latin
- [ ] Batch geocoding optimization (parallel queries)

### Long-term (v3.0)

- [ ] ML-based name matching (BERT embeddings)
- [ ] Active learning for ambiguous cases
- [ ] User feedback loop for corrections
- [ ] Self-hosted Overpass instance for high-volume

## Troubleshooting

### No coordinates found

1. **Check source availability**: Wikidata/OSM may not have the facility
2. **Lower min_confidence**: Try `min_confidence=0.4` instead of default 0.6
3. **Add aliases**: Include alternative names and transliterations
4. **Check cache**: Clear cache if getting stale results

### Wrong facility matched

1. **Check match_score**: Low score (<0.7) indicates uncertain match
2. **Provide more context**: Add commodities and aliases
3. **Use lat/lon hints**: Provide approximate coordinates for proximity scoring (future)

### API rate limits

1. **Wikidata**: Wait 0.2s between queries (5 req/sec max)
2. **Overpass**: Wait 0.5s between queries (2 req/sec conservative)
3. **Nominatim**: Wait 1.0s between queries (1 req/sec OSM Foundation policy)
4. **Self-host**: For high-volume, self-host Nominatim/Overpass

### Performance issues

1. **Enable caching**: `cache_results=True` (default)
2. **Batch processing**: Process by country to benefit from SPARQL country queries
3. **Reduce sources**: Disable unused sources (e.g., `use_overpass=False`)

## Contributing

### Adding a New Source

1. Create source client in `scripts/utils/sources/{source_name}.py`
2. Implement query method returning list of candidates
3. Add query method to `AdvancedGeocoder._query_{source_name}()`
4. Update source scores in `_score_candidates()`
5. Add tests to `scripts/test_geocoding_v2.py`

### Example Source Implementation

```python
# scripts/utils/sources/mindat.py

class MindatClient:
    def query_localities(
        self,
        country_iso3: str,
        commodity: Optional[str] = None
    ) -> List[MindatLocality]:
        # Implementation here
        pass
```

## References

- [OSM Overpass API](https://wiki.openstreetmap.org/wiki/Overpass_API)
- [Wikidata SPARQL Examples](https://www.wikidata.org/wiki/Wikidata:SPARQL_query_service/queries/examples)
- [Mindat API Docs](https://www.mindat.org/api.php)
- [rapidfuzz Documentation](https://rapidfuzz.github.io/RapidFuzz/)
- [transliterate Documentation](https://pypi.org/project/transliterate/)

## License

Part of the facilities repository. See main README for license information.
