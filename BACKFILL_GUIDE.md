# Backfill & Geocoding Guide

**Version 2.1.0** - Complete guide to enriching existing facilities

---

## Overview

The backfill system automatically enriches existing facilities with missing data:
- **Geocoding**: Add coordinates to facilities
- **Company Resolution**: Resolve company mentions to canonical IDs
- **Metal Normalization**: Add chemical formulas and categories

## Quick Start

```bash
# Install dependencies
pip install geopy

# Backfill everything for a country
python scripts/backfill.py all --country ARE --interactive

# Or run operations individually
python scripts/backfill.py geocode --country ARE
python scripts/backfill.py companies --country IND
python scripts/backfill.py metals --all
```

---

## Geocoding

### What It Does

Adds missing coordinates to facilities using multiple strategies:
1. **Industrial Zone Database** - Pre-mapped coordinates for known zones
2. **Nominatim API** (OpenStreetMap) - Free geocoding service
3. **Location Extraction** - Auto-detects cities from facility names
4. **Interactive Prompting** - Manual input when automated methods fail

### Usage

```bash
# Automated geocoding
python scripts/backfill.py geocode --country ARE

# Interactive mode (prompts for failures)
python scripts/backfill.py geocode --country ARE --interactive

# Multiple countries
python scripts/backfill.py geocode --countries ARE,IND,CHN

# Standalone geocoding utility
python scripts/geocode_facilities.py --country ARE
python scripts/geocode_facilities.py --facility-id are-union-cement-company-fac

# Dry run (preview changes)
python scripts/backfill.py geocode --country ARE --dry-run
```

### Industrial Zones Database

Pre-configured coordinates for UAE:
- **ICAD I/II/III** (Abu Dhabi)
- **Musaffah** (Abu Dhabi)
- **Jebel Ali** (Dubai)
- **FOIZ** (Fujairah Oil Industry Zone)
- **Hamriyah** (Sharjah)

**Add more zones** in `scripts/utils/geocoding.py`:
```python
INDUSTRIAL_ZONES = {
    "zone_name": {
        "lat": 24.338,
        "lon": 54.524,
        "city": "City Name",
        "country": "ARE"
    }
}
```

### Success Rates

**UAE Test Results:**
- Industrial zones: ~6% (2/32 facilities)
- Nominatim API: ~10% (3/32 facilities)
- **Total automated:** ~16% (5/32 facilities)
- Remaining: Need interactive mode or better data

---

## Company Resolution

### What It Does

Resolves `company_mentions[]` array to canonical company IDs:
- Populates `operator_link` with operator company ID
- Populates `owner_links[]` with owner company IDs
- Uses quality gates (strict/moderate/permissive)
- Writes relationships to parquet file

### Usage

```bash
# Backfill company resolution
python scripts/backfill.py companies --country IND

# Choose quality profile
python scripts/backfill.py companies --country IND --profile strict

# Multiple countries
python scripts/backfill.py companies --countries IND,CHN,BRA

# Dry run
python scripts/backfill.py companies --country IND --dry-run
```

### Quality Profiles

- **strict**: High precision (min confidence 0.80)
- **moderate**: Balanced (min confidence 0.70) - **Default**
- **permissive**: High recall (min confidence 0.60)

### Output

**Relationships written to:**
`tables/facilities/facility_company_relationships.parquet`

**Facility updates:**
```json
{
  "operator_link": {
    "company_id": "lei-549300abc123",
    "confidence": 0.85
  },
  "owner_links": [
    {
      "company_id": "lei-549300xyz789",
      "role": "majority_owner",
      "percentage": 60.0,
      "confidence": 0.90
    }
  ]
}
```

---

## Metal Normalization

### What It Does

Enriches commodities with chemical formulas and categories:
- Adds `chemical_formula` (e.g., "Cu", "Fe2O3", "Pt")
- Adds `category` (e.g., "base_metal", "precious_metal", "rare_earth")
- Uses EntityIdentity's `metal_identifier()` function
- Skips already-enriched commodities

### Usage

```bash
# Backfill metal normalization
python scripts/backfill.py metals --country CHN

# All countries
python scripts/backfill.py metals --all

# Dry run
python scripts/backfill.py metals --all --dry-run
```

### Example Output

**Before:**
```json
{
  "commodities": [
    {
      "metal": "copper",
      "primary": true
    }
  ]
}
```

**After:**
```json
{
  "commodities": [
    {
      "metal": "copper",
      "primary": true,
      "chemical_formula": "Cu",
      "category": "base_metal"
    }
  ]
}
```

---

## Unified Backfill (All Operations)

### What It Does

Runs all three enrichment operations in sequence:
1. Geocoding (add coordinates)
2. Metal normalization (add formulas)
3. Company resolution (resolve companies)

### Usage

```bash
# Backfill everything
python scripts/backfill.py all --country ARE

# Interactive mode (prompts for geocoding failures)
python scripts/backfill.py all --country ARE --interactive

# Multiple countries
python scripts/backfill.py all --countries ARE,IND,CHN

# Dry run
python scripts/backfill.py all --country ARE --dry-run
```

### Output

Complete statistics for each operation:
```
============================================================
BACKFILL SUMMARY: geocoding
============================================================
Total facilities: 35
Processed: 31
Updated: 5
Skipped: 0
Failed: 26
Success rate: 16.1%
============================================================

============================================================
BACKFILL SUMMARY: metals
============================================================
Total facilities: 35
Processed: 35
Updated: 0
Skipped: 35
Failed: 0
Success rate: 0.0%
============================================================

============================================================
BACKFILL SUMMARY: companies
============================================================
Total facilities: 35
Processed: 12
Updated: 8
Skipped: 4
Failed: 0
Success rate: 66.7%
============================================================
```

---

## Batch Processing

### Multiple Countries

```bash
# Geocode multiple countries
python scripts/backfill.py geocode --countries ARE,IND,CHN

# All operations for multiple countries
python scripts/backfill.py all --countries ARE,IND,CHN --interactive
```

### All Countries

```bash
# Metal normalization for all countries
python scripts/backfill.py metals --all
```

---

## Interactive Mode

### How It Works

When geocoding fails, prompts for manual input:

```
============================================================
GEOCODING REQUIRED: Union Cement Company (UCC)
Country: United Arab Emirates (ARE)
============================================================

Options:
  1. Enter coordinates (lat, lon)
  2. Enter city/location (will geocode)
  3. Skip (leave coordinates empty)

Choice [1/2/3]: 2
City/Location: Abu Dhabi
Found: 24.453, 54.377 (precision: city)
Use these coordinates? [y/n]: y
```

### Enable Interactive Mode

```bash
# Geocoding with prompts
python scripts/backfill.py geocode --country ARE --interactive

# All operations with prompts
python scripts/backfill.py all --country ARE --interactive
```

---

## Best Practices

### 1. Always Dry Run First

```bash
# Preview changes before applying
python scripts/backfill.py all --country ARE --dry-run
```

### 2. Start with Single Country

```bash
# Test on one country first
python scripts/backfill.py geocode --country ARE

# Then expand to batch
python scripts/backfill.py geocode --countries ARE,IND,CHN
```

### 3. Use Interactive Mode for Small Batches

```bash
# For countries with <50 facilities, use interactive
python scripts/backfill.py all --country ARE --interactive

# For large batches, skip interactive (too time-consuming)
python scripts/backfill.py geocode --country CHN
```

### 4. Process in Order

Recommended order for complete enrichment:
1. **Metals** (fast, no dependencies)
2. **Geocoding** (slow, uses API)
3. **Companies** (requires company_mentions)

```bash
python scripts/backfill.py metals --all
python scripts/backfill.py geocode --country ARE
python scripts/backfill.py companies --country ARE
```

---

## Troubleshooting

### Low Geocoding Success Rate

**Problem:** Only 10-20% of facilities geocoded automatically

**Solutions:**
1. Use interactive mode: `--interactive`
2. Add facilities to industrial zones database
3. Improve facility names (add city/location info)
4. Manual geocoding via Google Maps → copy coordinates

### Company Resolution Fails

**Problem:** No companies resolved

**Causes:**
- No `company_mentions[]` array in facilities
- Run `backfill_mentions.py` first
- Company names don't match EntityIdentity database

**Solutions:**
```bash
# Extract mentions first
python scripts/backfill_mentions.py --country IND

# Then resolve
python scripts/backfill.py companies --country IND

# Try permissive profile
python scripts/backfill.py companies --country IND --profile permissive
```

### Metal Normalization Does Nothing

**Problem:** 0% success rate, all skipped

**Cause:** Commodities already have formulas/categories

**Verify:**
```bash
grep -A3 '"commodities"' facilities/ARE/are-jebel-ali-smelter-fac.json
```

If `chemical_formula` and `category` already exist, normalization skips.

---

## API Rate Limits

### Nominatim (OpenStreetMap)

- **Rate limit:** 1 request per second
- **Automatic handling:** Built-in rate limiting
- **No API key required**
- **Usage policy:** https://operations.osmfoundation.org/policies/nominatim/

### Respectful Use

The backfill system automatically:
- Rate-limits to 1 req/sec
- Uses caching where possible
- Skips already-geocoded facilities
- Allows offline mode (`--no-nominatim`)

---

## File Locations

**Scripts:**
- `scripts/backfill.py` - Unified backfill command
- `scripts/geocode_facilities.py` - Standalone geocoding
- `scripts/utils/geocoding.py` - Geocoding service

**Config:**
- `config/gate_config.json` - Company resolution quality gates
- `config/company_aliases.json` - Canonical company IDs

**Output:**
- Facility JSONs updated in place
- `tables/facilities/facility_company_relationships.parquet` - Company relationships

---

## Version History

**v2.1.0 (2025-10-21):**
- Initial release of unified backfill system
- Geocoding with industrial zones + Nominatim
- Interactive prompting support
- Batch processing capabilities
- Statistics tracking

---

## See Also

- [README.md](README.md) - Complete facilities documentation
- [CLAUDE.md](CLAUDE.md) - Developer guidance and scripts reference
- [CHANGELOG.md](CHANGELOG.md) - Version history

---

**Status:** Production-ready | **Dependencies:** `geopy` (optional, for geocoding)
