#!/usr/bin/env python3
"""
Pre-migration validation script for Mines.csv.

Checks data quality and generates a comprehensive report before running the migration.
"""

import csv
import json
import sys
import pathlib
from collections import defaultdict, Counter
from datetime import datetime
from typing import Dict, List, Tuple

# Paths
ROOT = pathlib.Path(__file__).parent.parent
MINES_CSV = ROOT / "Mines.csv"
OUTPUT_DIR = ROOT / "output" / "migration_logs"

# Ensure output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


class ValidationReport:
    """Collects and reports validation findings."""

    def __init__(self):
        self.total_rows = 0
        self.missing_names = []
        self.missing_countries = []
        self.invalid_coordinates = []
        self.missing_coordinates = []
        self.missing_commodities = []
        self.country_counter = Counter()
        self.metal_counter = Counter()
        self.asset_type_counter = Counter()
        self.confidence_counter = Counter()
        self.rows_with_issues = set()

    def add_row(self, row_num: int, row: Dict):
        """Validate and track a single row."""
        self.total_rows += 1

        # Check mine name
        name = row.get("Mine Name", "").strip() or row.get("Mine Name ", "").strip()
        if not name:
            self.missing_names.append(row_num)
            self.rows_with_issues.add(row_num)

        # Check country
        country = row.get("Country or Region", "").strip()
        if not country:
            self.missing_countries.append(row_num)
            self.rows_with_issues.add(row_num)
        else:
            self.country_counter[country] += 1

        # Check coordinates
        lat_str = row.get("Latitude", "").strip()
        lon_str = row.get("Longitude", "").strip()

        if not lat_str or not lon_str:
            self.missing_coordinates.append(row_num)
        else:
            try:
                lat = float(lat_str)
                lon = float(lon_str)
                # Basic sanity checks
                if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
                    self.invalid_coordinates.append((row_num, lat, lon))
                    self.rows_with_issues.add(row_num)
            except ValueError:
                self.invalid_coordinates.append((row_num, lat_str, lon_str))
                self.rows_with_issues.add(row_num)

        # Check commodities
        primary = row.get("Primary Commodity", "").strip()
        secondary = row.get("Secondary Commodity", "").strip()
        other = row.get("Other Commodities", "").strip()

        if not primary and not secondary and not other:
            self.missing_commodities.append(row_num)
            self.rows_with_issues.add(row_num)

        # Count metals
        all_metals = []
        if primary:
            all_metals.append(primary)
        if secondary:
            all_metals.extend([m.strip() for m in secondary.split(';') if m.strip()])
        if other:
            all_metals.extend([m.strip() for m in other.split(';') if m.strip()])

        for metal in all_metals:
            self.metal_counter[metal.lower()] += 1

        # Count asset types
        asset_types = row.get("Asset Type", "").strip()
        if asset_types:
            for asset_type in asset_types.split(';'):
                self.asset_type_counter[asset_type.strip().lower()] += 1

        # Count confidence levels
        confidence = row.get("Confidence Factor", "").strip()
        if confidence:
            self.confidence_counter[confidence] += 1

    def generate_report(self) -> Dict:
        """Generate comprehensive validation report."""
        report = {
            "timestamp": datetime.now().isoformat(),
            "csv_file": str(MINES_CSV),
            "summary": {
                "total_rows": self.total_rows,
                "rows_with_issues": len(self.rows_with_issues),
                "issue_rate": f"{len(self.rows_with_issues)/self.total_rows*100:.2f}%"
            },
            "required_columns": {
                "mine_name": {
                    "missing_count": len(self.missing_names),
                    "missing_rows": self.missing_names[:50]  # First 50 for brevity
                },
                "country": {
                    "missing_count": len(self.missing_countries),
                    "missing_rows": self.missing_countries[:50]
                },
                "commodities": {
                    "missing_count": len(self.missing_commodities),
                    "missing_rows": self.missing_commodities[:50]
                }
            },
            "coordinates": {
                "missing_count": len(self.missing_coordinates),
                "invalid_count": len(self.invalid_coordinates),
                "coverage_rate": f"{(self.total_rows - len(self.missing_coordinates))/self.total_rows*100:.2f}%",
                "sample_invalid": self.invalid_coordinates[:10]  # First 10 invalid
            },
            "distributions": {
                "countries": {
                    "unique_count": len(self.country_counter),
                    "top_10": dict(self.country_counter.most_common(10))
                },
                "metals": {
                    "unique_count": len(self.metal_counter),
                    "top_15": dict(self.metal_counter.most_common(15))
                },
                "asset_types": {
                    "unique_count": len(self.asset_type_counter),
                    "all": dict(self.asset_type_counter.most_common())
                },
                "confidence_levels": {
                    "distribution": dict(self.confidence_counter.most_common())
                }
            },
            "recommendations": []
        }

        # Add recommendations based on findings
        if self.missing_names:
            report["recommendations"].append({
                "severity": "HIGH",
                "issue": f"{len(self.missing_names)} rows missing mine names",
                "action": "These rows will be skipped during migration"
            })

        if self.missing_countries:
            report["recommendations"].append({
                "severity": "HIGH",
                "issue": f"{len(self.missing_countries)} rows missing country",
                "action": "These rows will use 'UNK' as country code"
            })

        if len(self.missing_coordinates) > self.total_rows * 0.5:
            report["recommendations"].append({
                "severity": "MEDIUM",
                "issue": f"{len(self.missing_coordinates)} rows missing coordinates ({len(self.missing_coordinates)/self.total_rows*100:.1f}%)",
                "action": "Consider geocoding enhancement phase after migration"
            })

        if self.invalid_coordinates:
            report["recommendations"].append({
                "severity": "MEDIUM",
                "issue": f"{len(self.invalid_coordinates)} rows with invalid coordinates",
                "action": "These will be stored as null and flagged for correction"
            })

        if self.missing_commodities:
            report["recommendations"].append({
                "severity": "MEDIUM",
                "issue": f"{len(self.missing_commodities)} rows missing all commodity information",
                "action": "These facilities will have empty commodities array"
            })

        if not report["recommendations"]:
            report["recommendations"].append({
                "severity": "INFO",
                "issue": "No critical data quality issues found",
                "action": "Ready to proceed with migration"
            })

        return report

    def print_summary(self):
        """Print human-readable summary to console."""
        print("\n" + "="*80)
        print("PRE-MIGRATION VALIDATION REPORT")
        print("="*80)
        print(f"\nCSV File: {MINES_CSV}")
        print(f"Total Rows: {self.total_rows:,}")
        print(f"Rows with Issues: {len(self.rows_with_issues):,} ({len(self.rows_with_issues)/self.total_rows*100:.2f}%)")

        print("\n" + "-"*80)
        print("DATA COMPLETENESS")
        print("-"*80)
        print(f"Missing Names:        {len(self.missing_names):>6} rows")
        print(f"Missing Countries:    {len(self.missing_countries):>6} rows")
        print(f"Missing Coordinates:  {len(self.missing_coordinates):>6} rows ({len(self.missing_coordinates)/self.total_rows*100:.1f}%)")
        print(f"Invalid Coordinates:  {len(self.invalid_coordinates):>6} rows")
        print(f"Missing Commodities:  {len(self.missing_commodities):>6} rows")

        print("\n" + "-"*80)
        print("DATA DISTRIBUTIONS")
        print("-"*80)
        print(f"Unique Countries:     {len(self.country_counter):>6}")
        print(f"Unique Metals:        {len(self.metal_counter):>6}")
        print(f"Unique Asset Types:   {len(self.asset_type_counter):>6}")

        print("\nTop 10 Countries:")
        for country, count in self.country_counter.most_common(10):
            print(f"  {country:30s} {count:>5} facilities")

        print("\nTop 15 Metals/Commodities:")
        for metal, count in self.metal_counter.most_common(15):
            print(f"  {metal:30s} {count:>5} occurrences")

        print("\nAsset Types:")
        for asset_type, count in self.asset_type_counter.most_common():
            print(f"  {asset_type:30s} {count:>5} facilities")

        print("\nConfidence Levels:")
        for level, count in self.confidence_counter.most_common():
            print(f"  {level:30s} {count:>5} facilities")

        print("\n" + "="*80)
        print("READINESS ASSESSMENT")
        print("="*80)

        if len(self.rows_with_issues) == 0:
            print("STATUS: EXCELLENT - No data quality issues detected")
        elif len(self.rows_with_issues) < self.total_rows * 0.05:
            print("STATUS: GOOD - Minor issues (<5% of rows), safe to proceed")
        elif len(self.rows_with_issues) < self.total_rows * 0.20:
            print("STATUS: FAIR - Some issues present, review recommended")
        else:
            print("STATUS: NEEDS ATTENTION - Significant data quality issues")

        print("\nRecommended Actions:")
        if self.missing_names:
            print(f"  - {len(self.missing_names)} rows will be SKIPPED (no mine name)")
        if self.missing_countries:
            print(f"  - {len(self.missing_countries)} rows will use 'UNK' country code")
        if len(self.missing_coordinates) > self.total_rows * 0.5:
            print(f"  - Consider post-migration geocoding for {len(self.missing_coordinates)} facilities")
        if not any([self.missing_names, self.missing_countries, len(self.missing_coordinates) > self.total_rows * 0.5]):
            print("  - Ready to proceed with full migration")

        print("="*80 + "\n")


def validate_csv():
    """Main validation function."""
    print(f"Starting validation of {MINES_CSV}")

    if not MINES_CSV.exists():
        print(f"ERROR: Mines.csv not found at {MINES_CSV}")
        return None

    validator = ValidationReport()

    try:
        with open(MINES_CSV, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            # Verify required columns exist
            fieldnames = reader.fieldnames
            print(f"\nFound {len(fieldnames)} columns in CSV")
            print("Columns:", ", ".join(fieldnames))

            required_columns = ["Mine Name", "Country or Region", "Primary Commodity"]
            missing_columns = [col for col in required_columns if col not in fieldnames and col + " " not in fieldnames]

            if missing_columns:
                print(f"\nWARNING: Missing required columns: {missing_columns}")

            # Process all rows
            print("\nProcessing rows...")
            for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
                validator.add_row(row_num, row)

                if row_num % 500 == 0:
                    print(f"  Processed {row_num:,} rows...")

        print(f"\nValidation complete. Processed {validator.total_rows:,} rows.")

        # Generate report
        report = validator.generate_report()

        # Save report to file
        report_file = OUTPUT_DIR / f"pre_migration_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        print(f"Detailed report saved to: {report_file}")

        # Print summary
        validator.print_summary()

        return validator

    except Exception as e:
        print(f"ERROR during validation: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    """Main entry point."""
    validator = validate_csv()
    sys.exit(0 if validator else 1)


if __name__ == "__main__":
    main()
