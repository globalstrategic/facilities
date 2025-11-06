#!/usr/bin/env python3
"""
End-to-End Test for LLM Measurables System

Tests the complete workflow:
1. Load a sample facility
2. Tag with features
3. Route measurables
4. Compose prompts
5. (Optional) Execute queries if API key provided

Usage:
    # Without API key (test up to prompt composition)
    python scripts/llm_measurables/test_end_to_end.py \
      --facility facilities/ZAF/zaf-venetia-mine-fac.json

    # With API key (full test including LLM queries)
    export PERPLEXITY_API_KEY="pplx-..."
    python scripts/llm_measurables/test_end_to_end.py \
      --facility facilities/ZAF/zaf-venetia-mine-fac.json \
      --run-queries
"""

import json
import os
import sys
from pathlib import Path
import argparse
import logging

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts.llm_measurables import (
    FacilityFeatureTagger,
    MeasurablesRouter,
    PromptComposer,
    MeasurablesOrchestrator
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def test_end_to_end(facility_path: str, run_queries: bool = False):
    """Run end-to-end test of the measurables system."""

    print("\n" + "="*80)
    print("LLM MEASURABLES SYSTEM - END-TO-END TEST")
    print("="*80)

    # =========================================================================
    # Step 1: Load Facility
    # =========================================================================
    print("\n[Step 1/5] Loading facility...")

    with open(facility_path, "r") as f:
        facility = json.load(f)

    facility_id = facility.get("facility_id")
    canonical_name = facility.get("canonical_name") or facility.get("name")

    print(f"✓ Loaded: {canonical_name} ({facility_id})")

    # =========================================================================
    # Step 2: Tag with Features
    # =========================================================================
    print("\n[Step 2/5] Tagging with features...")

    tagger = FacilityFeatureTagger()
    features = tagger.tag_facility(facility)

    facility["facility_features"] = features  # Add to facility JSON

    print(f"✓ Tagged facility with features:")
    print(f"  - Process Type: {features.get('process_type')}")
    print(f"  - Mine Method: {features.get('mine_method')}")
    print(f"  - Acid Dependency: {features.get('acid_dependency')}")
    print(f"  - Power Intensity: {features.get('power_intensity')}")
    print(f"  - Climate Zone: {features.get('climate_zone')}")
    print(f"  - Country Risk: {features.get('country_risk_bucket')}/5")
    print(f"  - FCS: {features.get('consequentiality_score'):.2f}/100")

    # =========================================================================
    # Step 3: Route Measurables
    # =========================================================================
    print("\n[Step 3/5] Routing measurables...")

    router = MeasurablesRouter()
    json_ids = router.route_facility(facility)
    cadence = router.get_cadence(facility)

    routing_summary = router.get_facility_routing_summary(facility)

    print(f"✓ Routed {len(json_ids)} measurables:")
    print(f"  - Cadence: {cadence}")
    print(f"  - Selected Packs: {', '.join(routing_summary['selected_packs'])}")
    print(f"\n  Measurables:")
    for i, json_id in enumerate(json_ids[:10], 1):  # Show first 10
        print(f"    {i}. {json_id}")
    if len(json_ids) > 10:
        print(f"    ... and {len(json_ids) - 10} more")

    # =========================================================================
    # Step 4: Compose Prompts
    # =========================================================================
    print("\n[Step 4/5] Composing prompts...")

    composer = PromptComposer()

    # Compose first 3 prompts as examples
    sample_json_ids = json_ids[:3]
    prompts = []

    for json_id in sample_json_ids:
        prompt, prompt_hash = composer.compose_prompt(facility, json_id)
        prompts.append((json_id, prompt, prompt_hash))

    print(f"✓ Composed {len(prompts)} sample prompts:")
    for json_id, prompt, prompt_hash in prompts:
        print(f"\n  {json_id}:")
        print(f"    Hash: {prompt_hash[:16]}...")
        print(f"    Length: {len(prompt)} chars")
        # Show first 200 chars of prompt
        preview = prompt[:200].replace("\n", " ")
        print(f"    Preview: {preview}...")

    # =========================================================================
    # Step 5: Execute Queries (Optional)
    # =========================================================================
    if run_queries:
        print("\n[Step 5/5] Executing LLM queries...")

        # Check for API key
        api_key = os.getenv("PERPLEXITY_API_KEY")
        if not api_key:
            print("✗ PERPLEXITY_API_KEY not set. Skipping query execution.")
            print("  Set PERPLEXITY_API_KEY to test full end-to-end workflow.")
        else:
            orchestrator = MeasurablesOrchestrator(
                provider="perplexity",
                api_key=api_key,
                rate_limit_delay=2.0  # Slower for testing
            )

            # Query only first 2 measurables to save costs
            test_json_ids = sample_json_ids[:2]

            print(f"  Running {len(test_json_ids)} queries (limited for testing)...")

            results = orchestrator.run_facility(facility, json_ids=test_json_ids)

            # Print results
            accepted = sum(1 for r in results if r["accepted"])
            provisional = sum(1 for r in results if r["provisional"])
            rejected = sum(1 for r in results if not r["accepted"])

            print(f"\n✓ Query Results:")
            print(f"  - Total: {len(results)}")
            print(f"  - Accepted: {accepted}")
            print(f"  - Provisional: {provisional}")
            print(f"  - Rejected: {rejected}")

            for result in results:
                print(f"\n  {result['json_id']}:")
                print(f"    Value: {result['value']}")
                print(f"    Confidence: {result['confidence']}")
                print(f"    Freshness: {result['freshness_days']} days")
                print(f"    Accepted: {result['accepted']}")
                print(f"    Reason: {result['acceptance_reason']}")
                if result['evidence']:
                    print(f"    Evidence: {len(result['evidence'])} sources")

            # Save results
            output_dir = Path("output/test_results")
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / f"{facility_id}_test.json"

            orchestrator.save_results(results, str(output_file))
            print(f"\n  Results saved to: {output_file}")

    else:
        print("\n[Step 5/5] Skipping query execution (--run-queries not set)")
        print("  To test full workflow, set PERPLEXITY_API_KEY and use --run-queries flag")

    # =========================================================================
    # Summary
    # =========================================================================
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    print(f"Facility: {canonical_name}")
    print(f"FCS: {features.get('consequentiality_score'):.2f}")
    print(f"Cadence: {cadence}")
    print(f"Measurables Routed: {len(json_ids)}")
    print(f"Packs: {', '.join(routing_summary['selected_packs'])}")

    if run_queries and api_key:
        print(f"Queries Executed: {len(results)}")
        print(f"Acceptance Rate: {accepted}/{len(results)} ({100*accepted/len(results):.0f}%)")

    print("\n✓ End-to-end test completed successfully!")
    print("="*80 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="End-to-end test for LLM Measurables System")
    parser.add_argument("--facility", required=True, help="Path to facility JSON file")
    parser.add_argument("--run-queries", action="store_true", help="Execute LLM queries (requires PERPLEXITY_API_KEY)")

    args = parser.parse_args()

    # Validate facility file exists
    if not Path(args.facility).exists():
        print(f"Error: Facility file not found: {args.facility}", file=sys.stderr)
        sys.exit(1)

    try:
        test_end_to_end(args.facility, run_queries=args.run_queries)
    except Exception as e:
        print(f"\n✗ Test failed with error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
