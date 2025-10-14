#!/usr/bin/env python3
"""
Unified facilities management CLI.

Subcommands:
  import    - Import facilities from text reports
  research  - Enrich facilities with Gemini Deep Research
  test      - Run test suites
"""

import sys
import argparse
from pathlib import Path

# Add current directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))


def import_command(args):
    """Import facilities from text reports."""
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


def main():
    parser = argparse.ArgumentParser(
        description='Unified facilities management CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Import facilities from text report
  python facilities.py import report.txt --country DZ
  python facilities.py import report.txt --country DZ --source "Algeria Report 2025"

  # Generate Deep Research prompt
  python facilities.py research --generate-prompt --country ZAF --metal platinum --limit 50

  # Process Deep Research output
  python facilities.py research --process output.json --country ZAF --metal platinum
  python facilities.py research --batch batch.jsonl

  # Run tests
  python facilities.py test
  python facilities.py test --suite dedup
  python facilities.py test --suite migration
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    subparsers.required = True

    # Import subcommand
    import_parser = subparsers.add_parser('import', help='Import facilities from text reports')
    import_parser.add_argument('input_file', help='Input report file')
    import_parser.add_argument('--country', required=True, help='Country code (e.g., DZ, AFG)')
    import_parser.add_argument('--source', help='Source name (optional, auto-generated if not provided)')
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

    args = parser.parse_args()

    try:
        return args.func(args)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
