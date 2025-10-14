# Facilities Scripts

Scripts for managing the facilities database.

## Quick Start: Import Research Reports

Import facilities from deep research reports in two simple steps:

```bash
# Step 1: Save your report to a file
cat > algeria_report.txt
[Paste your full report text, then press Ctrl+D]

# Step 2: Import
python import_from_report.py algeria_report.txt --country DZA --source "Algeria Mining Report 2025"
```

**Alternative methods:**

```bash
# If report is already saved
python import_from_report.py report.txt --country AFG --source "Afghanistan Mineral Report"

# From clipboard (Mac)
pbpaste > report.txt
python import_from_report.py report.txt --country DZA --source "Report Name"

# From clipboard (Linux with xclip)
xclip -o > report.txt
python import_from_report.py report.txt --country DZA --source "Report Name"

# From stdin pipe
pbpaste | python import_from_report.py - --country DZA --source "Report Name"
```

### What it does

1. Automatically extracts facility tables from markdown text
2. Normalizes metals, facility types, and operational status
3. **Checks for duplicates** by name, location, and aliases
4. Generates schema-compliant JSON files
5. Creates detailed import report

### Output

Files created in: `../config/facilities/{COUNTRY}/`

Example: `../config/facilities/DZA/dza-gara-djebilet-fac.json`

Import report: `../output/import_logs/import_report_{COUNTRY}_{timestamp}.json`

### Example output

```
INFO: Processing report for DZA...
INFO: Found 1 facility tables
INFO: Loaded 1234 existing facilities for duplicate detection
INFO: Found 48 new facilities, skipped 3 duplicates

============================================================
IMPORT COMPLETE
============================================================
Country: DZA
New facilities: 48
Duplicates skipped: 3
Files written: 48
============================================================

Duplicates found:
  - 'Gara Djebilet Mine' (exists as dza-gara-djebilet-fac)
  - 'El-Hadjar Steel Complex' (exists as dza-el-hadjar-steel-fac)
  - 'Ouenza Mine' (exists as dza-ouenza-mine-fac)
```

### Duplicate Detection

Won't create duplicates if:
- Exact facility ID match
- Same name + location within ~1km (0.01°)
- Same name when no coordinates (conservatively assumes duplicate)
- Name matches an existing facility's alias

**Tested and verified:** Run `python test_dedup.py` to verify duplicate detection is working.

### Country Codes

Use ISO 3166-1 alpha-3 codes:

| Country | Code | Country | Code |
|---------|------|---------|------|
| Algeria | DZA | South Africa | ZAF |
| Afghanistan | AFG | Chile | CHL |
| Australia | AUS | Peru | PER |
| Canada | CAN | Brazil | BRA |
| China | CHN | India | IND |
| USA | USA | Russia | RUS |

### Input Format

The script automatically extracts markdown tables with these columns (flexible column names):

- **Site/Mine Name** (required) - Facility name
- **Latitude, Longitude** (recommended) - Decimal coordinates
- **Primary Commodity** - Main metal/mineral
- **Other Commodities** - Secondary metals/minerals
- **Asset Type** - Mine, Smelter, Plant, etc.
- **Operational Status** - Operating, Construction, Planned, Closed, etc.
- **Operator(s)** - Company names
- **Synonyms/Aliases** - Alternative names
- **Notes** - Additional information

### Normalization

**Metals:** `Cu` → `copper`, `Au` → `gold`, `REE` → `rare earths`, `Fe` → `iron`

**Types:** `Open Pit Mine` → `mine`, `Cement Factory` → `plant`, `Copper Smelter` → `smelter`

**Status:** `Operational` → `operating`, `In Development` → `construction`, `Proposed` → `planned`

## Other Scripts

### migrate_facilities.py

Original migration script for importing from `Mines.csv` to structured JSON format.

```bash
python migrate_facilities.py
```

### test_dedup.py

Test suite for duplicate detection logic.

```bash
python test_dedup.py
```

Expected output:
```
============================================================
Results: 6 passed, 0 failed
============================================================
```

## Troubleshooting

**"Ctrl+D / Cmd+D not working after paste"**
- **Solution:** Use the two-step method (recommended):
  ```bash
  cat > report.txt
  [Paste, Ctrl+D]
  python import_from_report.py report.txt --country DZA --source "Report"
  ```
- Or use clipboard directly: `pbpaste > report.txt` (Mac) or `xclip -o > report.txt` (Linux)

**"Report seems very small / paste was cut off"**
- Terminal paste has limits (typically 4-16KB)
- **Solution:** Save your report in a text editor first, then:
  ```bash
  python import_from_report.py /path/to/report.txt --country DZA --source "Report"
  ```
- Or use clipboard: `pbpaste > report.txt` bypasses terminal paste limits

**"No facility tables found in report"**
- Ensure your report contains markdown tables with `|` separators
- Tables need headers like "Mine Name", "Commodity", "Location"
- Check that the full report was saved (use `wc -l report.txt` to verify)

**"Too many duplicates detected"**
- Check the import report JSON for details on what matched
- This is working correctly - it's preventing duplicate entries
- Review `../output/import_logs/import_report_*.json`

**"Coordinates not parsing"**
- Use decimal degrees: `34.267` not `34° 16' N`
- Converter: https://www.fcc.gov/media/radio/dms-decimal

## Schema

Facility files follow the schema: `../schemas/facility.schema.json`

Key fields:
- `facility_id`: Unique ID in format `{iso3}-{slug}-fac`
- `name`: Primary facility name
- `country_iso3`: ISO 3166-1 alpha-3 country code
- `location`: Coordinates and precision
- `types`: Array of facility types
- `commodities`: Array of metals with primary flag
- `status`: Operational status
- `verification`: Confidence and source information

## Development

All scripts log to `.log` files in the scripts directory for debugging.

For detailed logging: Check `research_import.log` or `migration.log`
