# Research Report Import Workflow

This directory contains scripts for importing facilities from deep research reports into the structured facilities database.

## Overview

The import process has two main steps:
1. **Extract** tables from research reports → CSV/JSON
2. **Import** structured data → facility JSON files (with duplicate detection)

## Scripts

### 1. `extract_tables_from_report.py`

Extracts facility tables from markdown/text research reports and converts them to CSV or JSON.

**Usage:**
```bash
# Extract to CSV (recommended for manual review)
python extract_tables_from_report.py algeria_report.txt --output algeria_facilities.csv

# Extract to JSON
python extract_tables_from_report.py afghanistan_report.md --output afghanistan.json --format json
```

**What it does:**
- Parses markdown tables from research text
- Identifies tables containing facility data (mines, plants, etc.)
- Normalizes column headers to standard schema
- Outputs clean CSV/JSON ready for import

### 2. `import_research_facilities.py`

Imports facility data from CSV/JSON into the facilities database with duplicate detection.

**Usage:**
```bash
# Import Algeria facilities
python import_research_facilities.py algeria_facilities.csv \
  --country DZA \
  --source "Algeria Mining Sector Report 2025"

# Import Afghanistan facilities
python import_research_facilities.py afghanistan.json \
  --country AFG \
  --format json \
  --source "Afghanistan Mineral Inventory 2025"
```

**What it does:**
- Reads structured CSV/JSON input
- Normalizes metals, facility types, and status
- **Checks for duplicates** by:
  - Exact facility ID match
  - Name similarity + location proximity (within ~1km)
  - Alias matching
- Generates facility JSON files in `/config/facilities/{COUNTRY}/`
- Creates detailed import reports with duplicate information

## Complete Workflow Example

### Step 1: Save your research report

Save your research text to a file:
```bash
# Copy your Algeria report text
cat > algeria_report.txt
[paste text]
Ctrl+D
```

### Step 2: Extract tables to CSV

```bash
python extract_tables_from_report.py algeria_report.txt --output algeria_facilities.csv
```

**Output:** `algeria_facilities.csv` with columns like:
- Site/Mine Name
- Latitude, Longitude
- Primary Commodity, Other Commodities
- Asset Type
- Operational Status
- Operator(s) / Key Stakeholders
- Analyst Notes & Key Snippet IDs

### Step 3: Review and edit CSV (optional but recommended)

Open `algeria_facilities.csv` in your favorite editor/spreadsheet to:
- Fix any parsing issues
- Add missing information
- Verify operator names
- Clean up notes

### Step 4: Import to facilities database

```bash
python import_research_facilities.py algeria_facilities.csv \
  --country DZA \
  --source "Algeria Mining Sector Deep Research - October 2025"
```

**Output:**
- Facility JSON files: `/config/facilities/DZA/*.json`
- Import report: `/output/import_logs/import_report_DZA_20251014_*.json`

### Step 5: Review import report

Check the import report to see:
- How many facilities were created
- Which were skipped as duplicates
- Any errors or warnings

Example report excerpt:
```json
{
  "summary": {
    "new_facilities": 45,
    "duplicates_skipped": 3,
    "files_written": 45,
    "errors": 0
  },
  "duplicates_found": [
    {
      "row": 12,
      "name": "Gara Djebilet Mine",
      "existing_id": "dza-gara-djebilet-fac"
    }
  ]
}
```

## Expected Data Format

### CSV Format

The extractor produces CSV with these standard columns:

| Column | Description | Example |
|--------|-------------|---------|
| Site/Mine Name | Primary facility name | `Aynak Copper Project` |
| Synonyms | Alternative names | `Mes Aynak` |
| Latitude | Decimal degrees | `34.267` |
| Longitude | Decimal degrees | `69.317` |
| Primary Commodity | Main mineral/product | `Copper (Cu)` |
| Other Commodities | Secondary minerals | `Cobalt (Co)` |
| Asset Type | Facility type | `Mine, Smelter` |
| Operational Status | Current status | `In Development` |
| Operator(s) / Key Stakeholders | Companies | `MCC, Jiangxi Copper` |
| Analyst Notes & Key Snippet IDs | Additional notes | `World-class deposit...` |

### JSON Format

```json
{
  "facilities": [
    {
      "Site/Mine Name": "Aynak Copper Project",
      "Synonyms": "Mes Aynak",
      "Latitude": "34.267",
      "Longitude": "69.317",
      "Primary Commodity": "Copper (Cu)",
      "Other Commodities": "Cobalt (Co)",
      "Asset Type": "Mine",
      "Operational Status": "In Development",
      "Operator(s) / Key Stakeholders": "MCC, Jiangxi Copper",
      "Analyst Notes & Key Snippet IDs": "World-class sediment-hosted stratiform copper deposit"
    }
  ]
}
```

## Duplicate Detection

The importer uses multiple strategies to detect duplicates:

1. **Exact ID Match**: `dza-aynak-copper-fac` already exists
2. **Name + Location**: Same name within ~1km (0.01°)
3. **Alias Match**: Facility name matches an existing alias

When a duplicate is found:
- The facility is **skipped** (not imported)
- Details are logged in the import report
- The existing facility ID is reported

## Normalization Rules

### Metals/Commodities
- Chemical symbols expanded: `Cu` → `copper`, `Au` → `gold`
- Variants normalized: `aluminium` → `aluminum`, `REE` → `rare earths`
- Gemstones grouped: `emerald`, `ruby` → `precious stones`

### Facility Types
- All mining types → `mine` (open pit, underground, surface, coal mine, etc.)
- Processing facilities → `plant` (steel complex, cement factory, etc.)
- Specialized: `smelter`, `refinery`, `concentrator`, `mill`

### Status
- Active variants → `operating` (operational, active, in production)
- Development → `construction` (under construction, in development)
- Future → `planned` (proposed, contracted, undeveloped)
- Inactive → `closed`, `suspended`, `care_and_maintenance`

## Tips for Best Results

### For Report Text
- Keep markdown table formatting intact
- Ensure tables have clear headers
- Use `|` separators consistently

### For Manual CSV Editing
- Use `-` or empty string for unknown values (not `N/A` or `Unknown`)
- Separate multiple values with commas or semicolons
- Include coordinates whenever possible (improves duplicate detection)
- Use standard status terms: `Operating`, `Construction`, `Planned`, `Closed`

### For Operator Names
- Use full legal names when known
- Separate multiple operators with commas
- For JVs, list all partners: `Company A, Company B (51%)`

## Troubleshooting

### "No facility tables found in report"
- Check that your tables use markdown format with `|` separators
- Ensure tables have headers like "Mine Name", "Commodity", "Location"
- Try saving the report with explicit table formatting

### "Duplicate found" for a new facility
- Check if the facility already exists with a different name
- Review the import report to see which existing ID matched
- If it's truly different, you may need to rename one facility

### "Could not parse coordinates"
- Ensure latitude/longitude are decimal numbers (not DMS format)
- Example: `34.267` not `34° 16' 01" N`

### Metals not normalizing correctly
- Check `METAL_NORMALIZE_MAP` in `import_research_facilities.py`
- Add custom mappings as needed
- Use standard names: `iron` not `ferrous`, `zinc` not `Zn`

## Output Files

### Facility JSON Files
Location: `/config/facilities/{COUNTRY}/{facility-id}.json`

Example: `/config/facilities/DZA/dza-gara-djebilet-fac.json`

### Import Reports
Location: `/output/import_logs/import_report_{COUNTRY}_{timestamp}.json`

Contains:
- Summary statistics
- List of created facilities
- Duplicates skipped
- Errors encountered

### Log Files
- `research_import.log` - Detailed import process log
- `migration.log` - Original migration script log

## Next Steps After Import

1. **Review Generated Files**: Check a few facility JSON files to verify accuracy

2. **Update Missing Data**: Some facilities may need manual enrichment:
   - Ownership details (owner_links)
   - Production data (products)
   - More precise coordinates
   - Additional sources

3. **Link Companies**: The `operator_link` field requires company IDs from entityidentity

4. **Regenerate Indexes**: Run metal index generation if needed

5. **Version Control**: Commit the new facility files with a descriptive message

## Country Codes Reference

Common ISO 3166-1 alpha-3 codes:

| Country | ISO3 | Country | ISO3 |
|---------|------|---------|------|
| Algeria | DZA | Indonesia | IDN |
| Afghanistan | AFG | Philippines | PHL |
| Australia | AUS | South Africa | ZAF |
| Canada | CAN | Chile | CHL |
| China | CHN | Peru | PER |
| USA | USA | Brazil | BRA |
| Russia | RUS | India | IND |

Full list: https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3

## Support

For issues or questions:
1. Check the log files for detailed error messages
2. Review the import report JSON for specifics
3. Verify your input CSV/JSON format matches expectations
4. Consult the facility schema: `/schemas/facility.schema.json`
