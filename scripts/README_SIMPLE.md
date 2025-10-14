# Simple Import: One Command, Paste Report, Done

## The Easy Way

```bash
cd /Users/willb/Github/GSMC/facilities/scripts

# Just run this and paste your report:
python import_from_report.py --country DZA --source "Algeria Mining Report 2025"
```

Then:
1. **Paste** your entire research report
2. Press **Ctrl+D** (or **Cmd+D** on Mac)
3. **Done!** Facilities are imported with automatic duplicate detection

## From a File

```bash
# Save your report to a file first
cat > my_report.txt
[paste text]
^D

# Then import
python import_from_report.py my_report.txt --country DZA --source "Algeria Report 2025"
```

## What It Does

1. ✅ Extracts all facility tables from your report
2. ✅ Normalizes metals, types, and status
3. ✅ Checks for duplicates (won't create duplicates!)
4. ✅ Generates proper JSON files
5. ✅ Shows you a summary

## Output Example

```
INFO: Processing report for DZA...
INFO: Extracting facility tables from report...
INFO: Found 1 facility tables
INFO: Loaded 1234 existing facilities for duplicate detection
INFO: Processed 51 rows
INFO: Found 48 new facilities
INFO: Skipped 3 duplicates
INFO: Wrote 48 facility files

============================================================
IMPORT COMPLETE
============================================================
Country: DZA
Source: Algeria Mining Report 2025
New facilities: 48
Duplicates skipped: 3
Files written: 48
============================================================

Duplicates found (skipped 3 existing facilities):
  - 'Gara Djebilet Mine' (exists as dza-gara-djebilet-fac)
  - 'El-Hadjar Steel Complex' (exists as dza-el-hadjar-steel-fac)
  - 'Ouenza Mine' (exists as dza-ouenza-mine-fac)
```

## Country Codes

| Country | Code | Country | Code |
|---------|------|---------|------|
| Algeria | DZA  | Chile   | CHL  |
| Afghanistan | AFG | Peru | PER  |
| Australia | AUS | South Africa | ZAF |
| Canada | CAN | Brazil | BRA |
| China | CHN | India | IND |
| USA | USA | Russia | RUS |

## That's It!

No intermediate files. No multi-step process. Just paste and go.

The script automatically:
- Finds all tables in your report
- Extracts facility data
- Checks for duplicates by name and location
- Creates proper JSON files
- Tells you what happened

Files are created in: `/config/facilities/{COUNTRY}/`

Report is saved to: `/output/import_logs/import_report_{COUNTRY}_{timestamp}.json`
