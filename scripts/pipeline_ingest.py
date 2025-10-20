#!/usr/bin/env python3
"""
Unified Deep Research Ingest Pipeline
Chains: Parse TXT → Normalize → Resolve → Review → Metrics

Usage:
    python scripts/pipeline_ingest.py --txt research.txt --country ZAF --metal platinum
    python scripts/pipeline_ingest.py --txt research.txt --country BRA --dry-run
"""

import argparse
import subprocess
import sys
import json
import os
from datetime import datetime
from pathlib import Path


def run_command(cmd, description, dry_run=False):
    """Execute command and track progress."""
    print(f"\n{'[DRY-RUN] ' if dry_run else ''}📍 {description}...")
    print(f"   Command: {' '.join(cmd)}")

    if dry_run:
        print("   Skipped (dry-run)")
        return True

    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"❌ Failed: {result.stderr}")
            return False
        print(f"✅ Success")
        if result.stdout:
            print(f"   Output: {result.stdout[:200]}...")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Deep Research Ingest Pipeline")
    parser.add_argument("--txt", required=True, help="Path to research TXT file")
    parser.add_argument("--country", required=True, help="ISO3 country code")
    parser.add_argument("--metal", help="Metal name for context")
    parser.add_argument("--dry-run", action="store_true", help="Show commands without executing")
    parser.add_argument("--skip-normalize", action="store_true", help="Skip normalization step")
    parser.add_argument("--skip-resolve", action="store_true", help="Skip resolution step")
    parser.add_argument("--skip-metrics", action="store_true", help="Skip metrics reporting")

    args = parser.parse_args()

    # Validate inputs
    if not Path(args.txt).exists():
        print(f"❌ File not found: {args.txt}")
        sys.exit(1)

    if len(args.country) != 3:
        print(f"❌ Country must be ISO3 code (got: {args.country})")
        sys.exit(1)

    print(f"""
╔══════════════════════════════════════════════╗
║   DEEP RESEARCH INGEST PIPELINE             ║
╠══════════════════════════════════════════════╣
║   Input:   {args.txt:<34}║
║   Country: {args.country:<34}║
║   Metal:   {(args.metal or 'all'):<34}║
║   Mode:    {'DRY-RUN' if args.dry_run else 'LIVE':<34}║
╚══════════════════════════════════════════════╝
    """)

    # Track success through pipeline
    pipeline_success = True

    # Step 1: Parse TXT → structured research JSON
    research_json = f"output/research_{args.country}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    cmd = ["python", "scripts/deep_research_integration.py",
           "--process", args.txt,
           "--country", args.country,
           "--output", research_json]
    if args.metal:
        cmd.extend(["--metal", args.metal])

    if not run_command(cmd, "Step 1: Parse TXT → structured research", args.dry_run):
        pipeline_success = False
        if not args.dry_run:
            print("⚠️  Pipeline halted at Step 1")
            sys.exit(1)

    # Step 2: Normalize → write mentions to facilities
    if not args.skip_normalize:
        cmd = ["python", "scripts/normalize_mentions.py",
               "--countries", args.country,
               "--force"]  # Force refresh from research

        if not run_command(cmd, "Step 2: Normalize mentions → facility JSONs", args.dry_run):
            pipeline_success = False
            if not args.dry_run:
                print("⚠️  Pipeline halted at Step 2")
                sys.exit(1)
    else:
        print("\n⏩ Skipping normalization (--skip-normalize)")

    # Step 3: Resolve → create/update relationships
    if not args.skip_resolve:
        cmd = ["python", "scripts/enrich_companies.py",
               "--countries", args.country]

        if not run_command(cmd, "Step 3: Resolve mentions → relationships parquet", args.dry_run):
            pipeline_success = False
            if not args.dry_run:
                print("⚠️  Pipeline halted at Step 3")
                sys.exit(1)
    else:
        print("\n⏩ Skipping resolution (--skip-resolve)")

    # Step 4: Export review pack (if review items exist)
    review_pack = f"output/review_pack_{args.country}_{datetime.now().strftime('%Y%m%d')}.csv"
    cmd = ["python", "scripts/export_review_pack.py",
           "--countries", args.country,
           "--out", review_pack]

    if not run_command(cmd, "Step 4: Export review pack", args.dry_run):
        print("⚠️  No review items exported (may be normal if all auto-accepted)")
    else:
        if not args.dry_run and Path(review_pack).exists():
            with open(review_pack) as f:
                review_count = len(f.readlines()) - 1  # Minus header
            if review_count > 0:
                print(f"   📋 {review_count} items need review: {review_pack}")
                print(f"   Next: Review CSV, then run: python scripts/import_review_decisions.py --csv {review_pack}")

    # Step 5: Run metrics
    if not args.skip_metrics:
        cmd = ["python", "migration/wave_metrics.py",
               "--countries", args.country]

        if not run_command(cmd, "Step 5: Generate coverage metrics", args.dry_run):
            print("⚠️  Metrics generation failed (non-critical)")
        else:
            # Try to parse and display key metrics
            try:
                # Re-run to capture output for display
                if not args.dry_run:
                    result = subprocess.run(cmd, capture_output=True, text=True)
                    if "Coverage" in result.stdout:
                        for line in result.stdout.split('\n'):
                            if "Coverage" in line or "Auto-accept" in line or "Total relationships" in line:
                                print(f"   📊 {line.strip()}")
            except:
                pass
    else:
        print("\n⏩ Skipping metrics (--skip-metrics)")

    # Final report
    print(f"""
╔══════════════════════════════════════════════╗
║   PIPELINE {'COMPLETE' if pipeline_success else 'FAILED':<32}║
╚══════════════════════════════════════════════╝
""")

    if pipeline_success and not args.dry_run:
        print(f"""
✅ Next steps:
   1. Review items in: {review_pack} (if any)
   2. Import decisions: python scripts/import_review_decisions.py --csv {review_pack}
   3. Check metrics: python migration/wave_metrics.py --countries {args.country}
   4. Verify gates: python scripts/check_gates.py --countries {args.country}
""")

    sys.exit(0 if pipeline_success else 1)


if __name__ == "__main__":
    main()