# Facilities Data Pipeline Guide

Complete guide to the facilities database pipeline: from JSON storage → Parquet export → Database population.

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Facilities Repo (JSON Storage)](#facilities-repo-json-storage)
4. [Export to Parquet](#export-to-parquet)
5. [Database Population (tal repo)](#database-population-tal-repo)
6. [Complete Workflow](#complete-workflow)
7. [Troubleshooting](#troubleshooting)

---

## Overview

The facilities pipeline manages **10,537 facilities** across **134 countries**, with commodity and company relationships.

**Pipeline Flow:**
```
JSON files (facilities/)
    ↓
Parquet export (facilities.parquet, facility_materials.parquet, facility_companies.parquet)
    ↓
Database population (ENTITY.FACILITY, ENTITY.FACILITY_MATERIAL, ENTITY.FACILITY_COMPANY)
```

---

## Architecture

### Data Storage Locations

```
facilities/                              # Main JSON storage repo
├── facilities/                          # Country directories with facility JSONs
│   ├── USA/usa-*-fac.json              # ~1,623 US facilities
│   ├── CHN/chn-*-fac.json              # ~1,837 Chinese facilities
│   └── [132 other countries]/          # All other facilities
├── scripts/                             # Export and utility scripts
│   ├── export_to_parquet.py            # ★ Main export script
│   ├── export_relationships_parquet.py # Relationship extraction
│   └── import_from_report.py           # Import new facilities
├── schemas/facility.schema.json        # JSON schema v2.0.0
├── facilities.parquet                   # ★ Exported facility data
├── facility_materials.parquet           # ★ Material relationships
└── facility_companies.parquet           # ★ Company relationships

tal/scripts/db/                          # Database population repo
└── populate_all_facilities.py           # ★ Database loader
```

### Data Schema

**Facility JSON Structure:**
```json
{
  "facility_id": "usa-example-mine-fac",
  "name": "Example Mine",
  "country_iso3": "USA",
  "location": {
    "lat": 40.7128,
    "lon": -74.0060,
    "precision": "site"
  },
  "types": ["mine"],
  "commodities": [
    {
      "metal": "copper",
      "primary": true
    }
  ],
  "status": "operating",
  "company_mentions": ["Example Mining Corp"]
}
```

**Parquet Schema:**
- **facilities.parquet**: Flattened facility data (31 columns)
- **facility_materials.parquet**: `facility_id`, `material_name`, `is_primary`
- **facility_companies.parquet**: `facility_id`, `company_name`, `relationship_type`, `lei`

---

## Facilities Repo (JSON Storage)

### Location
`/path/to/facilities/` - Main JSON storage repository

### File Organization
```
facilities/
├── USA/
│   ├── usa-alloy-surface-coal-mine-fac.json
│   ├── usa-burns-mine-fac.json
│   └── ... (1,623 facilities)
├── CHN/
│   ├── chn-jiawula-fac.json
│   └── ... (1,837 facilities)
└── [132 other countries]/
```

### Facility ID Pattern
```
{country_iso3}-{slug}-fac
```
Example: `usa-example-mine-fac`

### Key Scripts

**Import new facilities:**
```bash
python scripts/import_from_report.py report.txt --country USA
```

**Backfill missing data:**
```bash
python scripts/backfill.py geocode --country USA
python scripts/backfill.py materials --all
```

---

## Export to Parquet

### Script Location
`facilities/scripts/export_to_parquet.py`

### What It Does
Creates **3 parquet files** from all facility JSONs:
1. **facilities.parquet** - Main facility data (10,537 rows)
2. **facility_materials.parquet** - Commodity relationships (14,377 rows)
3. **facility_companies.parquet** - Company relationships (22,222 rows)

### Usage

**Basic export (to current directory):**
```bash
cd /path/to/facilities
python scripts/export_to_parquet.py
```

**Export to specific directory:**
```bash
python scripts/export_to_parquet.py --output /tmp/export
```

**With preview:**
```bash
python scripts/export_to_parquet.py --preview
```

### Output
```
============================================================
EXPORT COMPLETE
============================================================
Output directory: /path/to/facilities

Files created:
  • facilities.parquet          10,537 rows  ( 0.90 MB)
  • facility_materials.parquet  14,377 rows  ( 0.18 MB)
  • facility_companies.parquet  22,222 rows  ( 0.40 MB)

Total: 1.48 MB

Facilities: 10,537
  • With coordinates: 9,853 (93.5%)
  • Countries: 134

Materials: 540 unique
  • Primary: 10,106
  • Secondary: 4,271

Companies: 13,871 unique
  • Operators: 0
  • Owners: 0
  • Mentions: 22,222
```

### Column Mappings

**facilities.parquet columns → Database columns:**
```
facility_id         → CANONICAL_KEY
name                → NAME
types (first item)  → FACILITY_TYPE
country_iso3        → COUNTRY_ID (via lookup)
location_region     → ADMIN1
latitude/longitude  → LATITUDE/LONGITUDE
location_precision  → PRECISION_LEVEL
status              → STATUS
primary_commodity   → PRIMARY_METAL
```

**facility_materials.parquet:**
```
facility_id    → Resolved to FACILITY_ID
material_name  → Resolved to MATERIAL_ID (via lookup)
is_primary     → Determines RANK (1=primary, 2=secondary)
```

**facility_companies.parquet:**
```
facility_id       → Resolved to FACILITY_ID
company_name      → Resolved to COMPANY_ID (via lookup)
relationship_type → RELATIONSHIP_TYPE (operator/owner/mention)
lei               → LEI
```

---

## Database Population (tal repo)

### Script Location
`tal/scripts/db/populate_all_facilities.py`

### What It Does
Loads parquet files into Snowflake database:
- `ENTITY.FACILITY`
- `ENTITY.FACILITY_MATERIAL`
- `ENTITY.FACILITY_COMPANY`

### Usage

**Auto-detect parquets from facilities repo:**
```bash
cd /path/to/tal
python -m scripts.db.populate_all_facilities
```

**Specify parquet directory:**
```bash
python -m scripts.db.populate_all_facilities /tmp/facilities_export
```

**Dry run first (recommended):**
```bash
python -m scripts.db.populate_all_facilities --dry-run
```

### How It Works

1. **Loads parquets** from directory (auto-detects facilities repo)
2. **Validates** facilities.parquet exists
3. **Connects** to Snowflake database
4. **Merges facilities** into ENTITY.FACILITY (upsert by CANONICAL_KEY)
5. **Resolves materials** via lookup to ENTITY.MATERIAL
6. **Inserts relationships** into ENTITY.FACILITY_MATERIAL
7. **Commits** transaction

### Output
```
============================================================
SUMMARY
============================================================
Facilities:
  Before: 10,538
  After: 10,537
  Affected by MERGE: 10,537

Facility-Material relationships:
  Before: 0
  After: 9,149
  Inserted: 9,149

SUCCESS: All facilities and relationships populated
```

### Material Resolution

The script uses **simple lowercase matching**:
```python
material_name.lower() → ENTITY.MATERIAL lookup
```

**Common issues:**
- Case sensitivity: "Gold" vs "gold" (both should work)
- Aliases: "thermal coal" might not match "coal" (needs manual mapping)
- Descriptors: "Iron Ore" vs "iron ore" (case-sensitive)

**To improve matching:**
1. Add materials to `ENTITY.MATERIAL` table
2. Add aliases/normalized names to lookup
3. OR use entity resolution (future enhancement)

---

## Complete Workflow

### Step 1: Maintain Facilities (facilities repo)

```bash
cd /path/to/facilities

# Import new facilities from reports
python scripts/import_from_report.py new_report.txt --country AUS

# Backfill missing coordinates
python scripts/backfill.py geocode --country AUS --interactive

# Clean duplicates (if any)
python scripts/tools/deduplicate_facilities.py --country AUS --dry-run
```

### Step 2: Export to Parquet

```bash
cd /path/to/facilities

# Generate all 3 parquet files
python scripts/export_to_parquet.py

# Verify files were created
ls -lh *.parquet
```

### Step 3: Populate Database

```bash
cd /path/to/tal

# Dry run first to check
python -m scripts.db.populate_all_facilities --dry-run

# Run for real
python -m scripts.db.populate_all_facilities

# Or specify directory
python -m scripts.db.populate_all_facilities /tmp/facilities_export
```

### Step 4: Verify in Database

```sql
-- Check facility counts
SELECT COUNT(*) FROM ENTITY.FACILITY;

-- Check material relationships
SELECT COUNT(*) FROM ENTITY.FACILITY_MATERIAL;

-- Sample with materials
SELECT
    f.NAME,
    m.NAME as MATERIAL,
    fm.RANK
FROM ENTITY.FACILITY f
JOIN ENTITY.FACILITY_MATERIAL fm ON f.ID = fm.FACILITY_ID
JOIN ENTITY.MATERIAL m ON fm.MATERIAL_ID = m.ID
WHERE f.COUNTRY_ID = (SELECT ID FROM ENTITY.COUNTRY WHERE ISO3 = 'USA')
LIMIT 10;
```

---

## Troubleshooting

### Issue: Materials not matching (9,149 instead of 14,377)

**Cause:** Material names in facilities don't match `ENTITY.MATERIAL` table.

**Common mismatches:**
- Case: "Gold" vs "gold"
- Aliases: "thermal coal" vs "coal"
- Compounds: "Iron Ore" not in MATERIAL table

**Solutions:**
1. Check unmatched materials:
```python
import pandas as pd
materials_df = pd.read_parquet('facility_materials.parquet')
print(materials_df['material_name'].value_counts().head(20))
```

2. Add missing materials to `ENTITY.MATERIAL`:
```sql
INSERT INTO ENTITY.MATERIAL (NAME, NAME_NORM, SYMBOL)
VALUES ('thermal coal', 'coal', 'COAL');
```

3. Update normalize logic in populate script

### Issue: Column name mismatch

**Error:** `KeyError: 'canonical_name'` or similar

**Cause:** Populate script expects different column names than parquet has.

**Fix:** Update column mappings in `populate_all_facilities.py` around lines 250-265:
```python
'CANONICAL_KEY': row['facility_id'],  # Not 'canonical_name'
'NAME': row['name'],                   # Not 'canonical_name'
'ADMIN1': row['location_region'],      # Not 'region'
```

### Issue: Duplicate facilities (10,557 instead of 10,537)

**Cause:** MERGE operation creating duplicates instead of updating.

**Check:** Verify CANONICAL_KEY is unique:
```sql
SELECT CANONICAL_KEY, COUNT(*) as cnt
FROM ENTITY.FACILITY
GROUP BY CANONICAL_KEY
HAVING COUNT(*) > 1;
```

**Fix:** Ensure parquet has unique facility_ids:
```python
df = pd.read_parquet('facilities.parquet')
print(df['facility_id'].value_counts()[df['facility_id'].value_counts() > 1])
```

### Issue: Parquet files not found

**Error:** `No parquet directory specified, auto-detecting... Error: Could not find parquet files`

**Cause:** Parquets not in expected location.

**Fix:**
1. Check parquets exist in facilities repo:
```bash
ls -lh /path/to/facilities/*.parquet
```

2. Or specify directory explicitly:
```bash
python -m scripts.db.populate_all_facilities /path/to/parquets
```

### Issue: Git merge conflicts in JSON files

**Error:** `Error loading facilities: Expecting property name enclosed in double quotes`

**Cause:** Unresolved git merge conflict markers in JSON files.

**Fix:** Use the merge conflict fixer:
```bash
cd /path/to/facilities
python scripts/fix_merge_conflicts.py --dry-run  # Check first
python scripts/fix_merge_conflicts.py             # Fix them
```

---

## Performance Notes

- **Export time:** ~5-10 seconds for 10,537 facilities
- **Parquet size:** 1.48 MB total (highly compressed)
- **Database load time:** ~30-60 seconds for all data
- **Material resolution:** ~100ms per material (simple lookup)

---

## Key Files Reference

| File | Purpose | Location |
|------|---------|----------|
| `export_to_parquet.py` | Export JSONs → Parquet | `facilities/scripts/` |
| `populate_all_facilities.py` | Load Parquet → Database | `tal/scripts/db/` |
| `facilities.parquet` | Main facility data | `facilities/` (generated) |
| `facility_materials.parquet` | Material relationships | `facilities/` (generated) |
| `facility_companies.parquet` | Company relationships | `facilities/` (generated) |
| `facility.schema.json` | JSON validation schema | `facilities/schemas/` |

---

## Version History

- **2025-12-08**: Parquet-based pipeline with relationship export
- **2025-10-27**: Enhanced table detection (v2.1.1)
- **2025-10-21**: Geocoding & backfill system (v2.1.0)
- **2025-10-20**: EntityIdentity integration (v2.0.0)

---

## Related Documentation

- **User Guide:** `facilities/README.md` - Comprehensive facility management guide
- **Schema:** `facilities/schemas/facility.schema.json` - JSON schema v2.0.0
- **Changelog:** `facilities/CHANGELOG.md` - Version history
- **tal README:** `tal/README.md` - Database and system overview

---

## Contact & Support

For issues or questions:
1. Check this guide first
2. Review error messages for specific troubleshooting steps
3. Check related documentation links above
4. Verify data integrity with provided SQL queries
