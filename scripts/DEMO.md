# Live Demo: Import Algeria Facilities

## Step 1: Run the command

```bash
cd /Users/willb/Github/GSMC/facilities/scripts
python import_from_report.py --country DZA --source "Algeria Mining Sector Report 2025"
```

## Step 2: Paste your report

When you see:
```
Paste your report text below, then press Ctrl+D (Cmd+D on Mac) when done:
------------------------------------------------------------
```

Just paste your entire Algeria report text (all the tables and text), then press **Ctrl+D** (or **Cmd+D** on Mac).

## Step 3: Watch it work

You'll see output like:
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
Source: Algeria Mining Sector Report 2025
New facilities: 48
Duplicates skipped: 3
Files written: 48
============================================================

Duplicates found (skipped 3 existing facilities):
  - 'Gara Djebilet Mine' (exists as dza-gara-djebilet-fac)
  - 'El-Hadjar Steel Complex' (exists as dza-el-hadjar-steel-fac)
  - 'Ouenza Mine' (exists as dza-ouenza-mine-fac)
```

## Done! Check the results

```bash
# See what was created
ls -la ../config/facilities/DZA/ | head -20

# View a sample facility
cat ../config/facilities/DZA/dza-aynak-copper-fac.json | jq

# Check the detailed report
cat ../output/import_logs/import_report_DZA_*.json | jq
```

## Duplicate Detection - Verified Working ✅

The deduplication has been tested and works by:

1. **Exact ID match** - `dza-gara-djebilet-fac` already exists ✅
2. **Name + location** - Same name within ~1km (0.01°) ✅
3. **Name without coords** - Same name, no location data (assumes duplicate) ✅
4. **Alias match** - Your name matches an existing facility's alias ✅
5. **NOT duplicate** - Same name but >1km away (treated as separate facility) ✅

Test results:
```
============================================================
Results: 6 passed, 0 failed
============================================================
```

## Alternative: From a file

If you prefer to save the report first:

```bash
# Save report
cat > algeria_report.txt
[paste text]
^D

# Then import
python import_from_report.py algeria_report.txt --country DZA --source "Algeria Report"
```

## Or pipe it

```bash
cat algeria_report.txt | python import_from_report.py --country DZA --source "Algeria Report"
```

## All three methods work the same way:
1. Extract tables automatically
2. Check for duplicates (won't create duplicates!)
3. Generate JSON files
4. Show summary

**That's it!** One command, paste report, done.
