#!/usr/bin/env python3
"""
Unified facilities management CLI.

Subcommands:
  import    - Import facilities from text reports (with optional --enhanced mode)
  research  - Enrich facilities with Gemini Deep Research
  test      - Run test suites
  sync      - Synchronize with entityidentity parquet format (export/import/status)
  resolve   - Test entity resolution (country/metal/company)
"""

import sys
import argparse
from pathlib import Path

# Add current directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))


def import_command(args):
    """Import facilities from text reports."""
    if args.enhanced:
        # Enhanced mode with entity resolution
        print("Note: --enhanced flag is set, but enhanced import is not yet implemented.")
        print("Enhanced mode will use entity resolution for:")
        print("  - Country auto-detection")
        print("  - Metal normalization")
        print("  - Company resolution")
        print("\nFalling back to standard import mode...")
        print()

    # Import the actual implementation
    from import_from_report import main as import_main

    # Set up args for the original script
    sys.argv = ['import_from_report.py', args.input_file, '--country', args.country]
    if args.source:
        sys.argv.extend(['--source', args.source])

    return import_main()


def research_command(args):
    """Enrich facilities with Gemini Deep Research."""
    from deep_research_integration import main as research_main

    # Build argv for the original script
    sys.argv = ['deep_research_integration.py']

    if args.generate_prompt:
        sys.argv.append('--generate-prompt')
        if args.country:
            sys.argv.extend(['--country', args.country])
        if args.metal:
            sys.argv.extend(['--metal', args.metal])
        if args.limit:
            sys.argv.extend(['--limit', str(args.limit)])
    elif args.process:
        sys.argv.extend(['--process', args.process])
        if args.country:
            sys.argv.extend(['--country', args.country])
        if args.metal:
            sys.argv.extend(['--metal', args.metal])
    elif args.batch:
        sys.argv.extend(['--batch', args.batch])

    return research_main()


def test_command(args):
    """Run test suites."""
    import subprocess

    if args.suite == 'dedup' or args.suite == 'all':
        print("Running duplicate detection tests...")
        result = subprocess.run([sys.executable, 'tests/test_dedup.py'], cwd=Path(__file__).parent)
        if result.returncode != 0:
            return 1

    if args.suite == 'migration' or args.suite == 'all':
        print("\nRunning migration tests...")
        result = subprocess.run([sys.executable, 'tests/test_migration_dry_run.py'], cwd=Path(__file__).parent)
        if result.returncode != 0:
            return 1

    return 0


def sync_command(args):
    """Synchronize facilities with entityidentity parquet format."""
    try:
        from utils.facility_sync import FacilitySyncManager
    except ImportError as e:
        print(f"Error: Could not import FacilitySyncManager: {e}", file=sys.stderr)
        print("\nPlease ensure all dependencies are installed:", file=sys.stderr)
        print("  pip install pandas pycountry", file=sys.stderr)
        return 1

    try:
        manager = FacilitySyncManager()

        if args.export:
            # Export facilities to parquet
            output_dir = Path(args.output) if args.output else Path('output/entityidentity_export')
            output_dir.mkdir(parents=True, exist_ok=True)

            print(f"Exporting facilities to entityidentity parquet format...")
            output_file = manager.export_to_entityidentity_format(output_dir)

            # Print statistics
            import pandas as pd
            df = pd.read_parquet(output_file)
            file_size_mb = output_file.stat().st_size / 1024 / 1024

            print(f"\nExport complete!")
            print(f"  Facilities exported: {len(df)}")
            print(f"  Output file: {output_file}")
            print(f"  File size: {file_size_mb:.2f} MB")

            # Show breakdown by country
            if 'country_iso2' in df.columns:
                print(f"\nBreakdown by country:")
                country_counts = df['country_iso2'].value_counts().head(10)
                for country, count in country_counts.items():
                    print(f"  {country}: {count} facilities")

        elif args.import_file:
            # Import facilities from parquet
            parquet_path = Path(args.import_file)
            if not parquet_path.exists():
                print(f"Error: Parquet file not found: {parquet_path}", file=sys.stderr)
                return 1

            print(f"Importing facilities from {parquet_path}...")
            if args.overwrite:
                print("Warning: --overwrite flag set, existing facilities will be overwritten")

            stats = manager.import_from_entityidentity(parquet_path, overwrite=args.overwrite)

            print(f"\nImport complete!")
            print(f"  Imported: {stats['imported']} facilities")
            print(f"  Skipped: {stats['skipped']} (already exist)")
            print(f"  Failed: {stats['failed']}")

        elif args.status:
            # Show sync status
            print("Facility Database Status")
            print("=" * 60)

            # Count local facilities
            facilities_dir = Path(__file__).parent.parent / "facilities"
            local_count = 0
            country_counts = {}

            for country_dir in facilities_dir.iterdir():
                if not country_dir.is_dir():
                    continue
                count = len(list(country_dir.glob("*.json")))
                if count > 0:
                    country_counts[country_dir.name] = count
                    local_count += count

            print(f"\nLocal database:")
            print(f"  Total facilities: {local_count}")
            print(f"  Countries: {len(country_counts)}")
            print(f"  Top countries:")
            for country, count in sorted(country_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
                print(f"    {country}: {count} facilities")

            # Check for entityidentity parquets
            ei_path = Path(__file__).parent.parent.parent / "entityidentity" / "tables" / "facilities"
            if ei_path.exists():
                print(f"\nEntityIdentity parquets found at: {ei_path}")
                parquet_files = list(ei_path.glob("facilities_*.parquet"))
                if parquet_files:
                    latest = max(parquet_files, key=lambda p: p.stat().st_mtime)
                    print(f"  Latest file: {latest.name}")

                    import pandas as pd
                    df = pd.read_parquet(latest)
                    print(f"  Facilities in parquet: {len(df)}")
                else:
                    print("  No facility parquet files found")
            else:
                print(f"\nEntityIdentity parquets not found at: {ei_path}")

        return 0

    except Exception as e:
        print(f"Error during sync operation: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


def resolve_command(args):
    """Test entity resolution using entityidentity directly."""
    if args.entity_type == 'country':
        # Resolve country using entityidentity
        try:
            sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'entityidentity'))
            from entityidentity import country_identifier
        except ImportError as e:
            print(f"Error: Could not import entityidentity: {e}", file=sys.stderr)
            print("\nPlease ensure entityidentity is installed:", file=sys.stderr)
            print("  Option 1: Clone entityidentity repo to parent directory", file=sys.stderr)
            print("    git clone https://github.com/globalstrategic/entityidentity.git ../entityidentity", file=sys.stderr)
            print("  Option 2: Install as package", file=sys.stderr)
            print("    pip install git+https://github.com/globalstrategic/entityidentity.git", file=sys.stderr)
            return 1

        country_name = args.name
        print(f"Resolving country: {country_name}")
        print("-" * 60)

        try:
            # country_identifier returns ISO2 code as string
            iso2 = country_identifier(country_name)

            if iso2:
                print(f"  Result: SUCCESS")
                print(f"  ISO2: {iso2}")

                # Get additional info from pycountry if available
                try:
                    import pycountry
                    country = pycountry.countries.get(alpha_2=iso2)
                    if country:
                        print(f"  ISO3: {country.alpha_3}")
                        print(f"  Country name: {country.name}")
                        if hasattr(country, 'official_name'):
                            print(f"  Official name: {country.official_name}")
                except ImportError:
                    pass  # pycountry not available, just show ISO2
            else:
                print(f"  Result: FAILED")
                print(f"  Could not resolve country: {country_name}")
                return 1

        except Exception as e:
            print(f"  Result: FAILED")
            print(f"  Error: {e}")
            import traceback
            traceback.print_exc()
            return 1

    elif args.entity_type == 'metal':
        # Resolve metal using entityidentity
        try:
            sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'entityidentity'))
            from entityidentity import metal_identifier
        except ImportError as e:
            print(f"Error: Could not import entityidentity: {e}", file=sys.stderr)
            print("\nPlease ensure entityidentity is installed:", file=sys.stderr)
            print("  Option 1: Clone entityidentity repo to parent directory", file=sys.stderr)
            print("    git clone https://github.com/globalstrategic/entityidentity.git ../entityidentity", file=sys.stderr)
            print("  Option 2: Install as package", file=sys.stderr)
            print("    pip install git+https://github.com/globalstrategic/entityidentity.git", file=sys.stderr)
            return 1

        metal_name = args.name
        print(f"Resolving metal: {metal_name}")
        print("-" * 60)

        try:
            result = metal_identifier(metal_name)

            if result:
                print(f"  Result: SUCCESS")
                print(f"  Normalized name: {result.get('name', 'N/A')}")
                print(f"  Symbol: {result.get('symbol', 'N/A')}")
                print(f"  Chemical formula: {result.get('formula', 'N/A')}")
                print(f"  Category: {result.get('category', 'unknown')}")

                # Show additional fields if present
                if result.get('atomic_number'):
                    print(f"  Atomic number: {result['atomic_number']}")
                if result.get('aliases'):
                    print(f"  Aliases: {', '.join(result['aliases'])}")
            else:
                print(f"  Result: FAILED")
                print(f"  Could not resolve metal: {metal_name}")
                return 1

        except Exception as e:
            print(f"  Result: FAILED")
            print(f"  Error: {e}")
            import traceback
            traceback.print_exc()
            return 1

    elif args.entity_type == 'company':
        # Resolve company using entityidentity
        try:
            sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'entityidentity'))
            from entityidentity.companies import EnhancedCompanyMatcher
        except ImportError as e:
            print(f"Error: Could not import entityidentity: {e}", file=sys.stderr)
            print("\nPlease ensure entityidentity is installed:", file=sys.stderr)
            print("  Option 1: Clone entityidentity repo to parent directory", file=sys.stderr)
            print("    git clone https://github.com/globalstrategic/entityidentity.git ../entityidentity", file=sys.stderr)
            print("  Option 2: Install as package", file=sys.stderr)
            print("    pip install git+https://github.com/globalstrategic/entityidentity.git", file=sys.stderr)
            return 1

        company_name = args.name
        country_hint = args.country if hasattr(args, 'country') else None

        print(f"Resolving company: {company_name}")
        if country_hint:
            print(f"Country hint: {country_hint}")
        print("-" * 60)

        try:
            matcher = EnhancedCompanyMatcher()
            results = matcher.match_best(company_name, limit=1, min_score=70)

            if results and len(results) > 0:
                best = results[0]
                print(f"  Result: SUCCESS")
                print(f"  Company name: {best.get('original_name', best.get('brief_name', 'N/A'))}")
                print(f"  Canonical name: {best.get('canonical_name', 'N/A')}")
                print(f"  LEI: {best.get('lei', 'N/A')}")
                print(f"  Match score: {best.get('score', 0)}/100")
                print(f"  Country: {best.get('country', 'N/A')}")
                print(f"  Category: {best.get('category', 'N/A')}")
            else:
                print(f"  Result: NO MATCH FOUND")
                print(f"  No company match found above minimum threshold (70)")

        except Exception as e:
            print(f"  Result: ERROR")
            print(f"  Error: {e}")
            import traceback
            traceback.print_exc()
            return 1

    return 0


def main():
    parser = argparse.ArgumentParser(
        description='Unified facilities management CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Import facilities from text report
  python facilities.py import report.txt --country DZ
  python facilities.py import report.txt --country DZ --source "Algeria Report 2025"
  python facilities.py import report.txt --country DZ --enhanced  # Use enhanced mode with entity resolution

  # Generate Deep Research prompt
  python facilities.py research --generate-prompt --country ZAF --metal platinum --limit 50

  # Process Deep Research output
  python facilities.py research --process output.json --country ZAF --metal platinum
  python facilities.py research --batch batch.jsonl

  # Run tests
  python facilities.py test
  python facilities.py test --suite dedup
  python facilities.py test --suite migration

  # Sync with entityidentity parquet format
  python facilities.py sync --export  # Export to output/entityidentity_export/
  python facilities.py sync --export --output /custom/path
  python facilities.py sync --import facilities.parquet
  python facilities.py sync --import facilities.parquet --overwrite
  python facilities.py sync --status  # Show database status

  # Test entity resolution
  python facilities.py resolve country "Algeria"
  python facilities.py resolve country DZ
  python facilities.py resolve metal "Cu"
  python facilities.py resolve metal "lithium carbonate"
  python facilities.py resolve company "BHP"
  python facilities.py resolve company "Sibanye-Stillwater" --country ZAF
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    subparsers.required = True

    # Import subcommand
    import_parser = subparsers.add_parser('import', help='Import facilities from text reports')
    import_parser.add_argument('input_file', help='Input report file')
    import_parser.add_argument('--country', required=True, help='Country code (e.g., DZ, AFG)')
    import_parser.add_argument('--source', help='Source name (optional, auto-generated if not provided)')
    import_parser.add_argument('--enhanced', action='store_true',
                              help='Use enhanced import mode with entity resolution (requires entityidentity)')
    import_parser.set_defaults(func=import_command)

    # Research subcommand
    research_parser = subparsers.add_parser('research', help='Enrich facilities with Gemini Deep Research')
    research_group = research_parser.add_mutually_exclusive_group(required=True)
    research_group.add_argument('--generate-prompt', action='store_true', help='Generate research prompt')
    research_group.add_argument('--process', metavar='FILE', help='Process research output file')
    research_group.add_argument('--batch', metavar='FILE', help='Process batch JSONL file')
    research_parser.add_argument('--country', help='Country code')
    research_parser.add_argument('--metal', help='Metal/commodity')
    research_parser.add_argument('--limit', type=int, help='Limit number of facilities (for prompt generation)')
    research_parser.set_defaults(func=research_command)

    # Test subcommand
    test_parser = subparsers.add_parser('test', help='Run test suites')
    test_parser.add_argument('--suite', choices=['dedup', 'migration', 'all'], default='all',
                            help='Which test suite to run (default: all)')
    test_parser.set_defaults(func=test_command)

    # Sync subcommand
    sync_parser = subparsers.add_parser('sync', help='Synchronize with entityidentity parquet format')
    sync_group = sync_parser.add_mutually_exclusive_group(required=True)
    sync_group.add_argument('--export', action='store_true',
                           help='Export facilities to entityidentity parquet format')
    sync_group.add_argument('--import', dest='import_file', metavar='PARQUET_PATH',
                           help='Import facilities from entityidentity parquet file')
    sync_group.add_argument('--status', action='store_true',
                           help='Show database status and compare with entityidentity')
    sync_parser.add_argument('--output', metavar='PATH',
                            help='Output directory for export (default: output/entityidentity_export/)')
    sync_parser.add_argument('--overwrite', action='store_true',
                            help='Overwrite existing facilities during import (default: skip)')
    sync_parser.set_defaults(func=sync_command)

    # Resolve subcommand
    resolve_parser = subparsers.add_parser('resolve', help='Test entity resolution')
    resolve_subparsers = resolve_parser.add_subparsers(dest='entity_type', help='Entity type to resolve')
    resolve_subparsers.required = True

    # Country resolution
    country_parser = resolve_subparsers.add_parser('country', help='Resolve country code')
    country_parser.add_argument('name', help='Country name or code (e.g., "Algeria", "DZ", "DZA")')

    # Metal resolution
    metal_parser = resolve_subparsers.add_parser('metal', help='Normalize metal/commodity name')
    metal_parser.add_argument('name', help='Metal name or symbol (e.g., "Cu", "Platinum", "lithium carbonate")')

    # Company resolution
    company_parser = resolve_subparsers.add_parser('company', help='Resolve company name')
    company_parser.add_argument('name', help='Company name (e.g., "BHP", "Sibanye-Stillwater")')
    company_parser.add_argument('--country', help='Country hint for better matching (ISO2 or ISO3)')

    resolve_parser.set_defaults(func=resolve_command)

    args = parser.parse_args()

    try:
        return args.func(args)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
