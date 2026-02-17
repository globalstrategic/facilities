#!/usr/bin/env python3
"""
Facilities Database Manager - Interactive CLI

A unified interactive tool for managing the facilities database.
Run this file and follow the prompts.

Usage:
    python facilities.py
"""

import json
import os
import sys
from pathlib import Path
from typing import Optional, List, Dict, Tuple

# Add scripts to path
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

# Import utilities
from scripts.utils.facility_loader import (
    load_facilities_from_country,
    load_all_facilities_list,
    save_facility,
    iter_country_dirs,
    get_facilities_dir,
    get_country_facility_count,
)
from scripts.utils.country_utils import normalize_country_to_iso3, iso3_to_country_name


# =============================================================================
# TERMINAL UTILITIES
# =============================================================================

def clear_screen():
    """Clear the terminal screen."""
    os.system('cls' if os.name == 'nt' else 'clear')


def print_header(title: str):
    """Print a formatted header."""
    width = 60
    print("\n" + "=" * width)
    print(f"  {title}")
    print("=" * width + "\n")


def print_menu(options: List[str], title: str = "Options"):
    """Print a numbered menu."""
    print(f"\n{title}:")
    print("-" * 40)
    for i, opt in enumerate(options, 1):
        print(f"  {i}. {opt}")
    print(f"  0. Back / Exit")
    print()


def prompt(message: str, default: str = "") -> str:
    """Prompt for input with optional default."""
    if default:
        result = input(f"{message} [{default}]: ").strip()
        return result if result else default
    return input(f"{message}: ").strip()


def prompt_yes_no(message: str, default: bool = True) -> bool:
    """Prompt for yes/no confirmation."""
    suffix = "[Y/n]" if default else "[y/N]"
    result = input(f"{message} {suffix}: ").strip().lower()
    if not result:
        return default
    return result in ('y', 'yes')


def prompt_choice(options: List[str], title: str = "Choose an option") -> int:
    """Prompt user to choose from numbered options. Returns 0 for back/exit."""
    print_menu(options, title)
    while True:
        try:
            choice = input("Enter choice: ").strip()
            if not choice:
                continue
            num = int(choice)
            if 0 <= num <= len(options):
                return num
            print(f"Please enter a number between 0 and {len(options)}")
        except ValueError:
            print("Please enter a valid number")


def prompt_country() -> Optional[str]:
    """Prompt for a country code and validate it."""
    while True:
        country = prompt("Enter country code or name (or 'list' to see all)")

        if country.lower() == 'list':
            counts = get_country_facility_count()
            print(f"\nCountries with facilities ({len(counts)} total):")
            for iso3, count in sorted(counts.items()):
                name = iso3_to_country_name(iso3) or iso3
                print(f"  {iso3}: {name} ({count} facilities)")
            print()
            continue

        if not country:
            return None

        iso3 = normalize_country_to_iso3(country)
        if iso3:
            name = iso3_to_country_name(iso3) or iso3
            print(f"  -> {iso3} ({name})")
            return iso3
        else:
            print(f"  Could not resolve '{country}'. Try again or enter 'list'.")


def wait_for_enter():
    """Wait for user to press Enter."""
    input("\nPress Enter to continue...")


# =============================================================================
# DATABASE STATISTICS
# =============================================================================

def show_statistics():
    """Display database statistics."""
    print_header("DATABASE STATISTICS")

    print("Loading facilities...")
    facilities, errors = load_all_facilities_list(include_path=False)

    if errors:
        print(f"  (Note: {errors} files had loading errors)")

    print(f"\nTotal facilities: {len(facilities):,}")

    # Country breakdown
    by_country = {}
    with_coords = 0
    with_commodities = 0
    by_status = {}

    for fac in facilities:
        country = fac.get('country_iso3', 'Unknown')
        by_country[country] = by_country.get(country, 0) + 1

        if fac.get('location', {}).get('lat'):
            with_coords += 1

        if fac.get('commodities'):
            with_commodities += 1

        status = fac.get('status', 'unknown')
        by_status[status] = by_status.get(status, 0) + 1

    print(f"Countries: {len(by_country)}")
    print(f"With coordinates: {with_coords:,} ({100*with_coords/len(facilities):.1f}%)")
    print(f"With commodities: {with_commodities:,} ({100*with_commodities/len(facilities):.1f}%)")

    print("\nStatus breakdown:")
    for status, count in sorted(by_status.items(), key=lambda x: -x[1]):
        print(f"  {status}: {count:,}")

    print("\nTop 10 countries by facility count:")
    sorted_countries = sorted(by_country.items(), key=lambda x: -x[1])[:10]
    for iso3, count in sorted_countries:
        name = iso3_to_country_name(iso3) or iso3
        print(f"  {iso3} ({name}): {count:,}")

    wait_for_enter()


# =============================================================================
# IMPORT FACILITIES
# =============================================================================

def import_facilities():
    """Interactive facility import from reports."""
    print_header("IMPORT FACILITIES")

    # List available reports
    reports_dir = ROOT / "data" / "import_reports"
    if reports_dir.exists():
        reports = list(reports_dir.glob("*.txt")) + list(reports_dir.glob("*.md"))
        if reports:
            print(f"Found {len(reports)} reports in data/import_reports/")
            if prompt_yes_no("Show available reports?", default=False):
                for r in sorted(reports)[:20]:
                    print(f"  {r.name}")
                if len(reports) > 20:
                    print(f"  ... and {len(reports) - 20} more")
                print()

    # Get report path
    report_path = prompt("Enter path to report file")
    if not report_path:
        print("No path provided, cancelling.")
        return

    report_path = Path(report_path)
    if not report_path.exists():
        # Try relative to data/import_reports
        alt_path = reports_dir / report_path.name
        if alt_path.exists():
            report_path = alt_path
        else:
            print(f"File not found: {report_path}")
            return

    print(f"Using: {report_path}")

    # Get country
    country = prompt_country()
    if not country:
        print("No country specified, will try to auto-detect.")

    # Dry run?
    dry_run = prompt_yes_no("Dry run (preview only)?", default=True)

    # Run import
    print("\nStarting import...")
    try:
        # Import the import function
        from scripts.import_from_report import main as import_main

        # Build args
        args = [str(report_path)]
        if country:
            args.extend(['--country', country])
        if dry_run:
            args.append('--dry-run')

        # Temporarily override sys.argv
        old_argv = sys.argv
        sys.argv = ['import_from_report.py'] + args

        try:
            import_main()
        finally:
            sys.argv = old_argv

    except ImportError as e:
        print(f"Error importing import module: {e}")
    except Exception as e:
        print(f"Import error: {e}")

    wait_for_enter()


# =============================================================================
# BACKFILL DATA
# =============================================================================

def backfill_menu():
    """Backfill missing data submenu."""
    while True:
        print_header("BACKFILL MISSING DATA")

        choice = prompt_choice([
            "Geocode (add coordinates)",
            "Companies (resolve company mentions)",
            "Metals (add chemical formulas)",
            "Towns (add town names)",
            "Canonical names (generate display names)",
            "All (run all backfills)",
        ], "Backfill type")

        if choice == 0:
            return

        backfill_types = ['geocode', 'companies', 'metals', 'towns', 'canonical_names', 'all']
        backfill_type = backfill_types[choice - 1]

        # Get scope
        print("\nScope:")
        scope_choice = prompt_choice([
            "Single country",
            "All countries",
        ], "Choose scope")

        if scope_choice == 0:
            continue

        country = None
        if scope_choice == 1:
            country = prompt_country()
            if not country:
                continue

        # Options
        dry_run = prompt_yes_no("Dry run (preview only)?", default=True)
        interactive = False
        if backfill_type in ('geocode', 'towns'):
            interactive = prompt_yes_no("Interactive mode (confirm each)?", default=False)

        # Run backfill
        print(f"\nRunning {backfill_type} backfill...")
        try:
            from scripts.backfill import main as backfill_main

            args = [backfill_type]
            if country:
                args.extend(['--country', country])
            else:
                args.append('--all')
            if dry_run:
                args.append('--dry-run')
            if interactive:
                args.append('--interactive')

            old_argv = sys.argv
            sys.argv = ['backfill.py'] + args

            try:
                backfill_main()
            finally:
                sys.argv = old_argv

        except Exception as e:
            print(f"Backfill error: {e}")

        wait_for_enter()


# =============================================================================
# EXPORT DATA
# =============================================================================

def export_data():
    """Export facilities data."""
    print_header("EXPORT DATA")

    choice = prompt_choice([
        "CSV (Mines.csv format)",
        "Parquet (with relationship tables)",
    ], "Export format")

    if choice == 0:
        return

    fmt = 'csv' if choice == 1 else 'parquet'

    # Scope
    print("\nScope:")
    scope_choice = prompt_choice([
        "All facilities",
        "Single country",
        "Filter by metal",
    ], "Choose scope")

    if scope_choice == 0:
        return

    country = None
    metal = None

    if scope_choice == 2:
        country = prompt_country()
        if not country:
            return
    elif scope_choice == 3:
        metal = prompt("Enter metal name (e.g., lithium, copper, REE)")
        if not metal:
            return

    # Output path
    default_output = f"output/facilities.{fmt}"
    output_path = prompt("Output path", default_output)

    # Run export
    print(f"\nExporting to {output_path}...")
    try:
        from scripts.export import main as export_main

        args = ['--format', fmt, '--output', output_path]
        if country:
            args.extend(['--country', country])
        elif scope_choice == 1:
            args.append('--all')
        if metal:
            args.extend(['--metal', metal])

        old_argv = sys.argv
        sys.argv = ['export.py'] + args

        try:
            export_main()
        finally:
            sys.argv = old_argv

    except Exception as e:
        print(f"Export error: {e}")

    wait_for_enter()


# =============================================================================
# AUDIT / QC
# =============================================================================

def audit_menu():
    """Audit and QC submenu."""
    while True:
        print_header("AUDIT & QUALITY CONTROL")

        choice = prompt_choice([
            "Full audit (all issues)",
            "QC report (coverage statistics)",
            "Find specific issue type",
        ], "Audit type")

        if choice == 0:
            return

        if choice == 1:
            # Full audit
            print("\nScope:")
            scope_choice = prompt_choice(["All countries", "Single country"])

            if scope_choice == 0:
                continue

            country = None
            if scope_choice == 2:
                country = prompt_country()
                if not country:
                    continue

            print("\nRunning audit...")
            try:
                from scripts.tools.audit import main as audit_main

                args = []
                if country:
                    args.extend(['--country', country])

                old_argv = sys.argv
                sys.argv = ['audit.py'] + args

                try:
                    audit_main()
                finally:
                    sys.argv = old_argv

            except Exception as e:
                print(f"Audit error: {e}")

        elif choice == 2:
            # QC Report
            print("\nGenerating QC report...")
            try:
                from scripts.reporting.facility_qc_report import main as qc_main
                qc_main()
            except Exception as e:
                print(f"QC report error: {e}")

        elif choice == 3:
            # Find specific issue
            issues = [
                "numeric_name",
                "generic_name",
                "no_coordinates",
                "no_commodities",
                "no_operator_owner",
                "unknown_status",
                "low_confidence",
            ]

            issue_choice = prompt_choice(issues, "Issue type")
            if issue_choice == 0:
                continue

            issue_type = issues[issue_choice - 1]

            try:
                from scripts.tools.audit import main as audit_main

                old_argv = sys.argv
                sys.argv = ['audit.py', '--issue', issue_type, '--limit', '20']

                try:
                    audit_main()
                finally:
                    sys.argv = old_argv

            except Exception as e:
                print(f"Audit error: {e}")

        wait_for_enter()


# =============================================================================
# DEDUPLICATE
# =============================================================================

def deduplicate():
    """Find and merge duplicate facilities."""
    print_header("DEDUPLICATE FACILITIES")

    print("This will find and merge duplicate facilities based on:")
    print("  - Coordinate proximity (within ~1-11km)")
    print("  - Name similarity (>60-85% match)")
    print("  - Alias matching")
    print()

    # Scope
    scope_choice = prompt_choice([
        "Single country",
        "All countries",
    ], "Scope")

    if scope_choice == 0:
        return

    country = None
    if scope_choice == 1:
        country = prompt_country()
        if not country:
            return

    dry_run = prompt_yes_no("Dry run (preview only)?", default=True)

    print("\nSearching for duplicates...")
    try:
        from scripts.tools.deduplicate import main as dedupe_main

        args = []
        if country:
            args.extend(['--country', country])
        else:
            args.append('--all')
        if dry_run:
            args.append('--dry-run')

        old_argv = sys.argv
        sys.argv = ['deduplicate.py'] + args

        try:
            dedupe_main()
        finally:
            sys.argv = old_argv

    except Exception as e:
        print(f"Deduplication error: {e}")

    wait_for_enter()


# =============================================================================
# FIX ISSUES
# =============================================================================

def fix_issues():
    """Fix known issues in facilities."""
    print_header("FIX ISSUES")

    choice = prompt_choice([
        "Fix coordinate issues",
        "Fix wrong country assignments",
        "Validate coordinates (check for out-of-bounds)",
    ], "Fix type")

    if choice == 0:
        return

    # Scope
    scope_choice = prompt_choice([
        "Single country",
        "All countries",
    ], "Scope")

    if scope_choice == 0:
        return

    country = None
    if scope_choice == 1:
        country = prompt_country()
        if not country:
            return

    dry_run = prompt_yes_no("Dry run (preview only)?", default=True)

    print("\nAnalyzing issues...")
    try:
        if choice in (1, 2):
            from scripts.tools.fix import main as fix_main

            fix_type = 'coords' if choice == 1 else 'country'
            args = [fix_type]
            if country:
                args.extend(['--country', country])
            else:
                args.append('--all')
            if dry_run:
                args.append('--dry-run')

            old_argv = sys.argv
            sys.argv = ['fix.py'] + args

            try:
                fix_main()
            finally:
                sys.argv = old_argv

        elif choice == 3:
            from scripts.tools.validate import main as validate_main

            args = []
            if country:
                args.extend(['--country', country])
            else:
                args.append('--all')

            old_argv = sys.argv
            sys.argv = ['validate.py'] + args

            try:
                validate_main()
            finally:
                sys.argv = old_argv

    except ImportError as e:
        print(f"Module not available: {e}")
    except Exception as e:
        print(f"Fix error: {e}")

    wait_for_enter()


# =============================================================================
# BROWSE FACILITIES
# =============================================================================

def browse_facilities():
    """Browse and search facilities."""
    print_header("BROWSE FACILITIES")

    choice = prompt_choice([
        "Browse by country",
        "Search by name",
        "Search by metal/commodity",
    ], "Browse method")

    if choice == 0:
        return

    if choice == 1:
        # Browse by country
        country = prompt_country()
        if not country:
            return

        facilities = load_facilities_from_country(country, include_path=False)
        print(f"\nFound {len(facilities)} facilities in {country}:")
        print("-" * 60)

        for i, fac in enumerate(facilities[:50], 1):
            name = fac.get('name', 'Unknown')
            status = fac.get('status', '?')
            commodities = ', '.join(c.get('metal', '?') for c in fac.get('commodities', [])[:3])
            print(f"{i:3}. {name[:40]:40} [{status}] {commodities}")

        if len(facilities) > 50:
            print(f"... and {len(facilities) - 50} more")

    elif choice == 2:
        # Search by name
        query = prompt("Enter search term").lower()
        if not query:
            return

        print(f"\nSearching for '{query}'...")
        facilities, _ = load_all_facilities_list(include_path=False)

        matches = []
        for fac in facilities:
            name = fac.get('name', '').lower()
            aliases = [a.lower() for a in fac.get('aliases', [])]
            if query in name or any(query in a for a in aliases):
                matches.append(fac)

        print(f"Found {len(matches)} matches:")
        print("-" * 60)

        for fac in matches[:30]:
            name = fac.get('name', 'Unknown')
            country = fac.get('country_iso3', '?')
            print(f"  [{country}] {name}")

        if len(matches) > 30:
            print(f"... and {len(matches) - 30} more")

    elif choice == 3:
        # Search by metal
        metal = prompt("Enter metal/commodity name").lower()
        if not metal:
            return

        print(f"\nSearching for '{metal}' facilities...")
        facilities, _ = load_all_facilities_list(include_path=False)

        matches = []
        for fac in facilities:
            commodities = [c.get('metal', '').lower() for c in fac.get('commodities', [])]
            if any(metal in c for c in commodities):
                matches.append(fac)

        print(f"Found {len(matches)} facilities with {metal}:")
        print("-" * 60)

        for fac in matches[:30]:
            name = fac.get('name', 'Unknown')
            country = fac.get('country_iso3', '?')
            print(f"  [{country}] {name}")

        if len(matches) > 30:
            print(f"... and {len(matches) - 30} more")

    wait_for_enter()


# =============================================================================
# MAIN MENU
# =============================================================================

def main_menu():
    """Main interactive menu."""
    while True:
        clear_screen()
        print_header("FACILITIES DATABASE MANAGER")

        # Quick stats
        counts = get_country_facility_count()
        total = sum(counts.values())
        print(f"Database: {total:,} facilities across {len(counts)} countries\n")

        choice = prompt_choice([
            "View statistics",
            "Browse / Search facilities",
            "Import facilities from report",
            "Backfill missing data",
            "Export data",
            "Audit / QC",
            "Deduplicate",
            "Fix issues",
        ], "Main Menu")

        if choice == 0:
            print("\nGoodbye!")
            break
        elif choice == 1:
            show_statistics()
        elif choice == 2:
            browse_facilities()
        elif choice == 3:
            import_facilities()
        elif choice == 4:
            backfill_menu()
        elif choice == 5:
            export_data()
        elif choice == 6:
            audit_menu()
        elif choice == 7:
            deduplicate()
        elif choice == 8:
            fix_issues()


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    try:
        main_menu()
    except KeyboardInterrupt:
        print("\n\nInterrupted. Goodbye!")
        sys.exit(0)
