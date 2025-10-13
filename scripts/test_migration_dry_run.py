#!/usr/bin/env python3
"""
Dry run test of the migration script with first N rows.

Tests the migration process without writing to production locations.
"""

import csv
import json
import sys
import pathlib
import tempfile
import shutil
from datetime import datetime

# Add entityidentity to path
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent.parent / 'entityidentity'))

# Import the migrator class
sys.path.insert(0, str(pathlib.Path(__file__).parent))
from migrate_facilities import FacilityMigrator, MINES_CSV

# Constants
ROOT = pathlib.Path(__file__).parent.parent
TEST_ROWS = 10  # Number of rows to test


class DryRunMigrator(FacilityMigrator):
    """Test version that writes to temporary directory."""

    def __init__(self, temp_dir: pathlib.Path, max_rows: int = None):
        super().__init__()
        self.temp_dir = temp_dir
        self.max_rows = max_rows

        # Override output directories to use temp
        global FACILITIES_DIR, SUPPLY_DIR, MAPPINGS_DIR, MIGRATION_LOGS_DIR
        from migrate_facilities import CONFIG_DIR, OUTPUT_DIR

        self.facilities_dir = temp_dir / "facilities"
        self.supply_dir = temp_dir / "supply"
        self.mappings_dir = temp_dir / "mappings"
        self.logs_dir = temp_dir / "logs"

        # Create temp directories
        for dir_path in [self.facilities_dir, self.supply_dir, self.mappings_dir, self.logs_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)

    def migrate_csv(self):
        """Override to limit row count."""
        print(f"Starting DRY RUN migration from {MINES_CSV}")
        print(f"Processing first {self.max_rows} rows only\n")

        if not MINES_CSV.exists():
            print(f"ERROR: Mines.csv not found at {MINES_CSV}")
            return False

        with open(MINES_CSV, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            # Show header info
            print(f"CSV Columns: {', '.join(reader.fieldnames)}\n")

            for row_num, row in enumerate(reader, start=2):
                if self.max_rows and row_num > self.max_rows + 1:  # +1 for header
                    break

                facility = self.parse_csv_row(row, row_num)
                if facility:
                    self.facilities.append(facility)
                    # Print details for dry run
                    print(f"Row {row_num}: {facility['name']}")
                    print(f"  - ID: {facility['facility_id']}")
                    print(f"  - Country: {facility['country_iso3']}")
                    print(f"  - Location: ({facility['location']['lat']}, {facility['location']['lon']})")
                    print(f"  - Types: {', '.join(facility['types'])}")
                    print(f"  - Commodities: {', '.join([c['metal'] for c in facility['commodities']])}")
                    print()

        print(f"Parsed {len(self.facilities)} facilities from CSV\n")
        return True

    def write_facilities(self):
        """Write to temp directory."""
        print("Writing facility JSON files to temp directory...")

        for facility in self.facilities:
            country_iso3 = facility['country_iso3']
            country_dir = self.facilities_dir / country_iso3
            country_dir.mkdir(parents=True, exist_ok=True)

            facility_file = country_dir / f"{facility['facility_id']}.json"
            with open(facility_file, 'w', encoding='utf-8') as f:
                json.dump(facility, f, ensure_ascii=False, indent=2)

            self.stats['files_written'] += 1

        print(f"Wrote {self.stats['files_written']} facility files")

        # Show sample file
        if self.facilities:
            sample = self.facilities[0]
            sample_file = self.facilities_dir / sample['country_iso3'] / f"{sample['facility_id']}.json"
            print(f"\nSample facility file: {sample_file.name}")
            with open(sample_file, 'r') as f:
                print(json.dumps(json.load(f), indent=2))

    def write_metal_indexes(self):
        """Write indexes to temp directory."""
        print("\n" + "="*60)
        print("Writing metal index files to temp directory...")

        for metal, facility_ids in self.per_metal_facilities.items():
            metal_slug = self.slugify(metal)
            metal_dir = self.supply_dir / metal_slug
            metal_dir.mkdir(parents=True, exist_ok=True)

            index_data = {
                "generated": datetime.now().isoformat(),
                "metal": metal,
                "total_facilities": len(facility_ids),
                "facilities": sorted(list(facility_ids))
            }

            index_file = metal_dir / "facilities.index.json"
            with open(index_file, 'w', encoding='utf-8') as f:
                json.dump(index_data, f, ensure_ascii=False, indent=2)

            print(f"  {metal:20s} -> {len(facility_ids):3d} facilities in {metal_slug}/facilities.index.json")

    def write_mappings(self):
        """Write mappings to temp directory."""
        print("\n" + "="*60)
        print("Writing mapping files to temp directory...")

        # Company mappings
        if self.company_mapping:
            company_map_file = self.mappings_dir / "company_canonical.json"
            with open(company_map_file, 'w', encoding='utf-8') as f:
                json.dump(self.company_mapping, f, ensure_ascii=False, indent=2)
            print(f"Wrote {len(self.company_mapping)} company mappings")

        # Country mappings
        country_map_file = self.mappings_dir / "country_canonical.json"
        with open(country_map_file, 'w', encoding='utf-8') as f:
            json.dump(self.country_cache, f, ensure_ascii=False, indent=2)
        print(f"Wrote {len(self.country_cache)} country mappings")

        # Show country mapping sample
        print("\nSample country mappings:")
        for country, iso3 in sorted(self.country_cache.items())[:5]:
            print(f"  {country:30s} -> {iso3}")

        # Metal mappings
        metal_map_file = self.mappings_dir / "metal_canonical.json"
        with open(metal_map_file, 'w', encoding='utf-8') as f:
            json.dump(self.metal_cache, f, ensure_ascii=False, indent=2)
        print(f"\nWrote {len(self.metal_cache)} metal mappings")

        # Show metal mapping sample
        print("\nSample metal mappings:")
        for metal, canonical in sorted(self.metal_cache.items())[:10]:
            print(f"  {metal:30s} -> {canonical}")

    def write_migration_report(self):
        """Write test report."""
        report = {
            "test_type": "DRY_RUN",
            "timestamp": datetime.now().isoformat(),
            "rows_tested": self.max_rows,
            "statistics": dict(self.stats),
            "errors": self.errors,
            "temp_directory": str(self.temp_dir)
        }

        report_file = self.logs_dir / "dry_run_report.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        print("\n" + "="*60)
        print("DRY RUN SUMMARY")
        print("="*60)
        print(f"Rows tested: {self.max_rows}")
        print(f"Facilities created: {self.stats['total_facilities']}")
        print(f"Countries: {len([k for k in self.stats.keys() if k.startswith('country_')])}")
        print(f"Metals: {len([k for k in self.stats.keys() if k.startswith('metal_')])}")
        print(f"Files written: {self.stats['files_written']}")
        print(f"Errors: {len(self.errors)}")
        print(f"\nTemp directory: {self.temp_dir}")
        print("="*60)


def test_dry_run():
    """Run dry run test."""
    print("="*80)
    print("FACILITIES MIGRATION DRY RUN TEST")
    print("="*80)
    print(f"Testing with first {TEST_ROWS} rows of Mines.csv\n")

    # Create temp directory
    temp_dir = pathlib.Path(tempfile.mkdtemp(prefix="talloy_migration_test_"))
    print(f"Created temporary directory: {temp_dir}\n")

    try:
        # Run migration
        migrator = DryRunMigrator(temp_dir, max_rows=TEST_ROWS)
        success = migrator.run()

        if success:
            print("\n" + "="*80)
            print("DRY RUN TEST: PASSED")
            print("="*80)
            print("\nThe migration script is working correctly!")
            print("You can inspect the test output in:")
            print(f"  {temp_dir}")
            print("\nWhen ready to run full migration:")
            print("  python3 scripts/migrate_facilities.py")
            print("="*80)
        else:
            print("\n" + "="*80)
            print("DRY RUN TEST: FAILED")
            print("="*80)
            print("Please review errors above before running full migration.")
            print("="*80)

        # Ask if user wants to keep temp files
        print(f"\nTemp directory will be kept for inspection: {temp_dir}")
        print("You can delete it manually when done reviewing.")

        return success

    except Exception as e:
        print(f"\nERROR during dry run: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main entry point."""
    success = test_dry_run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
