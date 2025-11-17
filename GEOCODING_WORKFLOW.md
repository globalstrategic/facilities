# Automated Geocoding Workflow

## Quick Start

```bash
# 1. Export facilities missing coordinates to CSV (for review)
python scripts/find_missing_coords.py

# 2. Run automated geocoding for a specific country
python scripts/backfill.py geocode --country ZAF

# 3. Run for all countries with missing coords
python scripts/backfill.py geocode --all

# 4. Preview changes first (dry-run mode)
python scripts/backfill.py geocode --country ZAF --dry-run
```

## Current Status

**Total facilities**: 10,650
**Missing coordinates**: 1,814 (17.0%)

### Top Countries by Missing Coordinates

| Country | Missing | Total | % Missing |
|---------|---------|-------|-----------|
| ZAF (South Africa) | 297 | 630 | 47.1% |
| BEL (Belgium) | 56 | 73 | 76.7% |
| IND (India) | 56 | 480 | 11.7% |
| MAR (Morocco) | 41 | 56 | 73.2% |
| KAZ (Kazakhstan) | 39 | 124 | 31.5% |
| CAN (Canada) | 37 | 232 | 15.9% |

## How the Automated Geocoding Works

The `backfill.py geocode` command:

1. **Loads facilities** for the specified country
2. **Filters** to facilities with missing `location.lat` / `location.lon`
3. **Geocodes** each facility using Nominatim (OpenStreetMap):
   - Query format: `"{Facility Name}, {Country Name}"`
   - Example: `"Olympic Dam Mine, Australia"`
4. **Validates** coordinates through multiple safety gates:
   - ✗ Rejects sentinel coordinates (0,0 or 90,180)
   - ✗ Rejects invalid coordinates (out of range)
   - ✗ Rejects out-of-country coordinates (bbox check)
5. **Writes** validated coordinates back to facility JSON:
   ```json
   {
     "location": {
       "lat": -30.451,
       "lon": 136.924,
       "precision": "town"
     }
   }
   ```

## Options

### Dry Run (Preview Changes)
```bash
python scripts/backfill.py geocode --country ZAF --dry-run
```
Shows what would be changed without actually modifying files.

### Interactive Mode
```bash
python scripts/backfill.py geocode --country ZAF --interactive
```
Prompts for manual input when automated geocoding fails.

### Batch Mode (Multiple Countries)
```bash
python scripts/backfill.py geocode --countries ZAF,IND,MAR
```

### Process All Countries
```bash
python scripts/backfill.py geocode --all
```

## Output Files

The `find_missing_coords.py` script generates two CSV files:

1. **`output/Mines.csv`** - All 10,650 facilities with lat/lon extracted
2. **`output/Mines_Missing_Coords.csv`** - Only the 1,814 facilities with missing coords

Both files include:
- `facility_id` - Unique identifier
- `name` - Facility name
- `country_iso3` - ISO3 country code
- `latitude` - Extracted from location.lat
- `longitude` - Extracted from location.lon
- `location` - Full location object as string
- `province` - Province/state
- `status` - Operating status
- `facility_type` - Type (mine, smelter, etc)
- `primary_metal` - Main commodity
- `operator` - Operator name
- `_file_path` - Path to JSON file

## Validation & Safety

The geocoding system has multiple safety gates to prevent bad data:

1. **Sentinel Coordinate Detection**: Rejects (0,0), (90,180), etc.
2. **Range Validation**: Ensures lat ∈ [-90,90], lon ∈ [-180,180]
3. **Country Bounding Box**: Verifies coordinates are within country boundaries
4. **Data Quality Flags**: Marks failures in `data_quality.flags`:
   - `sentinel_coords_rejected`
   - `invalid_coords`
   - `out_of_country`
   - `geocode_failed`

## Recommended Workflow

### For Small Sets (Single Country)
```bash
# 1. Preview what will change
python scripts/backfill.py geocode --country ZAF --dry-run

# 2. Run the geocoding
python scripts/backfill.py geocode --country ZAF

# 3. Review results
python scripts/find_missing_coords.py
```

### For Large Sets (All Countries)
```bash
# 1. Export current state
python scripts/find_missing_coords.py

# 2. Run automated geocoding (takes ~30 min for 1,814 facilities)
python scripts/backfill.py geocode --all

# 3. Check what remains
python scripts/find_missing_coords.py

# 4. Review failed cases in Mines_Missing_Coords.csv
```

### For Interactive Cleanup
```bash
# For facilities that automated geocoding couldn't handle
python scripts/backfill.py geocode --country ZAF --interactive
```

## Performance

- **Nominatim rate limit**: 1 request/second (enforced by backfill.py)
- **Expected time**: ~1,814 seconds = ~30 minutes for all missing coords
- **Success rate**: Typically 60-80% for facility names
- **Manual intervention**: Remaining 20-40% need interactive or manual lookup

## Rate Limiting

The backfill script respects Nominatim usage policy:
- 1 second delay between requests
- User-Agent header with contact email
- Uses env var `OSM_CONTACT_EMAIL` (defaults to ops@gsmc.example)

Set your contact email:
```bash
export OSM_CONTACT_EMAIL="your-email@example.com"
```

## Next Steps After Geocoding

Once coordinates are filled:
1. Run `python scripts/backfill.py towns --country ZAF` to add town names
2. Run `python scripts/backfill.py canonical_names --country ZAF` to update display names
3. Export to parquet: `python scripts/facilities.py sync --export`

## Troubleshooting

### "No results from Nominatim"
- Facility name may be too generic or unknown
- Try interactive mode to provide manual coordinates
- Check if facility exists in OpenStreetMap

### "Out-of-country coordinates"
- Nominatim found a match but in wrong country
- Common for facilities with common names (e.g., "Central Mine")
- May need manual lookup or more specific name

### "Sentinel coordinates rejected"
- Safety gate working correctly
- Facility had placeholder coordinates like (0,0)
- Need to geocode from scratch

## Files Reference

- **`scripts/backfill.py`** - Main backfill system (1,398 lines)
- **`scripts/find_missing_coords.py`** - Export & analysis script (220 lines)
- **`scripts/utils/geocoding.py`** - Geocoding utilities
- **`output/Mines.csv`** - All facilities export
- **`output/Mines_Missing_Coords.csv`** - Missing coords export
