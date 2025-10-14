# Quick Start: Import Algeria Facilities

This guide shows you exactly how to import the Algeria research report.

## Step 1: Save the Research Report

Create a file with your Algeria research text:

```bash
cd /Users/willb/Github/GSMC/facilities/scripts
cat > algeria_report.txt
# Paste the entire Algeria research text here
# Then press Ctrl+D (or Cmd+D on Mac)
```

## Step 2: Extract Tables to CSV

```bash
python extract_tables_from_report.py algeria_report.txt --output algeria_facilities.csv
```

Expected output:
```
Extracting tables from algeria_report.txt...
Found 1 tables
Found 1 facility tables
Wrote 51 facilities to algeria_facilities.csv
```

## Step 3: Review the CSV (Optional)

```bash
# Quick preview
head -20 algeria_facilities.csv

# Or open in your editor
code algeria_facilities.csv
# or
open -a "Microsoft Excel" algeria_facilities.csv
```

## Step 4: Import to Database

```bash
python import_research_facilities.py algeria_facilities.csv \
  --country DZA \
  --source "Algeria Mineral Sector Comprehensive Analysis 2025"
```

Expected output:
```
============================================================
IMPORT SUMMARY
============================================================
Country: DZA
Source: Algeria Mineral Sector Comprehensive Analysis 2025
New facilities imported: 48
Duplicates skipped: 3
Files written: 48
============================================================
```

## Step 5: Verify Results

```bash
# Check created files
ls -la ../config/facilities/DZA/ | head -10

# View a sample facility
cat ../config/facilities/DZA/dza-gara-djebilet-mine-fac.json | jq
```

## What if I Just Want to Manually Create the CSV?

If table extraction doesn't work perfectly, you can create a CSV manually with these columns:

**Required columns:**
- `Site/Mine Name` - The facility name
- `Latitude` - Decimal degrees (optional but recommended)
- `Longitude` - Decimal degrees (optional but recommended)

**Highly recommended columns:**
- `Primary Commodity` - Main metal/mineral (e.g., "Iron", "Copper, Gold")
- `Asset Type` - Facility type (e.g., "Mine", "Smelter", "Plant")
- `Operational Status` - Current status (e.g., "Operating", "Construction", "Planned")

**Optional columns:**
- `Synonyms` - Alternative names (comma-separated)
- `Other Commodities` - Secondary metals/minerals
- `Operator(s) / Key Stakeholders` - Company names
- `Analyst Notes & Key Snippet IDs` - Any additional notes

Example CSV content:
```csv
Site/Mine Name,Latitude,Longitude,Primary Commodity,Other Commodities,Asset Type,Operational Status,Operator(s) / Key Stakeholders
Gara Djebilet Mine,26.766,-7.333,Iron Ore,,Mine,In Development,FERAAL (JV with CMH China)
Aynak Copper Mine,34.267,69.317,Copper,Cobalt,Mine,Stalled,MCC (China)
Bled El Hadba Mine,,,Phosphate,,Mine,In Development,ACFC (Asmidal/Manal 56%)
```

Then import:
```bash
python import_research_facilities.py your_manual.csv \
  --country XXX \
  --source "Your Source Name"
```

## Troubleshooting

### Problem: "No facility tables found"

**Solution:** Your report text doesn't have properly formatted markdown tables. Create a CSV manually instead.

### Problem: "Duplicate found" for a new facility

**Solution:** Check the import report to see which existing facility matched:
```bash
cat ../output/import_logs/import_report_DZA_*.json | jq '.duplicates_found'
```

If it's truly a duplicate, great! If not, you may need to rename the facility to make it distinct.

### Problem: Coordinates not parsing

**Solution:** Use decimal degrees format:
- ✅ Good: `35.75` and `68.617`
- ❌ Bad: `35° 45' N` and `68° 37' E`

Convert DMS to decimal first: https://www.fcc.gov/media/radio/dms-decimal

## Next Steps

After import, you may want to:

1. **Enrich facility data** - Add production figures, ownership details, etc.
2. **Link companies** - Update `operator_link` with proper company IDs
3. **Add sources** - Include specific URLs or documents in the `sources` array
4. **Verify coordinates** - Check and improve location precision
5. **Update metal indexes** - Regenerate if you've added new commodities

See `RESEARCH_IMPORT_README.md` for complete documentation.
