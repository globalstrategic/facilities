#!/usr/bin/env python3
"""
Identify facilities that are likely parsing errors and should be deleted.
Uses LLM to analyze facility names and identify obvious problems.
"""

import json
import os
import sys
import csv
from datetime import datetime
from pathlib import Path
import logging
import pandas as pd

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, os.path.expanduser('~/Github/GSMC/entityidentity'))

import openai
from dotenv import load_dotenv

# Load environment variables
load_dotenv(os.path.expanduser('~/Github/GSMC/entityidentity/.env'))

# Configure OpenAI
openai.api_key = os.getenv('OPENAI_API_KEY')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('output/invalid_facilities.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class FacilityValidator:
    """Identify facilities that are parsing errors or invalid entries"""

    def __init__(self):
        self.client = openai.OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

        # Obvious parsing errors to check first
        self.obvious_errors = [
            "sections)", "not specified", "unknown", "n/a", "various",
            "none", "nil", "null", "-", "--", "...", "?", "tbd",
            "undefined", "unspecified", "other", "misc", "miscellaneous",
            "not applicable", "na", "no name", "unnamed", "blank",
            "test", "example", "sample", "placeholder", "temp",
            "delete", "remove", "ignore", "skip", "todo"
        ]

        # Patterns that suggest parsing errors
        self.error_patterns = [
            r'^\).*',  # Starts with closing bracket
            r'.*\)$',  # Ends with closing bracket without opening
            r'^\d+$',  # Just numbers
            r'^[a-z]$',  # Single lowercase letter
            r'^\W+$',  # Only special characters
            r'^page\s*\d+',  # Page numbers
            r'^table\s*\d+',  # Table references
            r'^figure\s*\d+',  # Figure references
            r'^appendix',  # Appendix references
            r'^section\s*\d+',  # Section references
            r'^\(.*\)$',  # Just parentheses content
            r'^\[.*\]$',  # Just bracket content
        ]

    def check_obvious_errors(self, name: str) -> tuple:
        """Quick check for obvious parsing errors"""
        name_lower = name.lower().strip()

        # Check exact matches
        if name_lower in self.obvious_errors:
            return True, "obvious_error"

        # Check patterns
        import re
        for pattern in self.error_patterns:
            if re.match(pattern, name_lower):
                return True, f"matches_error_pattern"

        # Check for very short names that don't make sense
        if len(name) <= 2 and not name.isupper():  # Allow 2-letter codes like "CR"
            return True, "too_short"

        # Check for unmatched brackets
        if name.count('(') != name.count(')'):
            if ')' in name and '(' not in name:
                return True, "unmatched_closing_bracket"

        return False, None

    def analyze_batch(self, facilities: list) -> list:
        """Analyze a batch of facilities using LLM"""

        # Prepare the batch for LLM analysis
        facility_list = []
        for f in facilities:
            facility_list.append({
                'id': f['facility_id'],
                'name': f['name'],
                'country': f['country_iso3'],
                'metal': f.get('primary_metal', 'unknown')
            })

        prompt = f"""Analyze these facility names and identify which are likely parsing errors or invalid entries.

For each facility, determine if it's:
1. A valid facility name (mine, smelter, refinery, quarry, plant, etc.)
2. A parsing error (truncated text, metadata, table headers, etc.)
3. Too generic to be useful (like "Mine" or "Smelter" alone)

Be LENIENT - if it could plausibly be a real facility, mark it as valid.

Facilities to analyze:
{json.dumps(facility_list, indent=2)}

Respond with a JSON array where each item has:
{{
    "id": "facility_id",
    "is_valid": true/false,
    "reason": "valid_facility|parsing_error|too_generic|truncated|metadata|etc",
    "confidence": 0.0-1.0,
    "notes": "optional explanation"
}}"""

        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are analyzing mining facility names to identify parsing errors."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                response_format={"type": "json_object"},
                max_tokens=2000
            )

            result = json.loads(response.choices[0].message.content)

            # Handle different response formats
            if isinstance(result, dict):
                if 'facilities' in result:
                    return result['facilities']
                elif 'results' in result:
                    return result['results']
                else:
                    # Wrap single result in array
                    return [result]
            elif isinstance(result, list):
                return result
            else:
                logger.error(f"Unexpected response format: {type(result)}")
                return []

        except Exception as e:
            logger.error(f"LLM analysis failed: {e}")
            return []


def main():
    """Main execution"""

    # Load facilities
    csv_file = 'output/Mines_Missing_Coords.csv'
    if not os.path.exists(csv_file):
        logger.error(f"File not found: {csv_file}")
        return

    df = pd.read_csv(csv_file)
    logger.info(f"Loaded {len(df)} facilities from CSV")

    # Initialize validator
    validator = FacilityValidator()

    # Results storage
    invalid_facilities = []
    valid_facilities = []

    # First pass: Check for obvious errors
    logger.info("\n=== PASS 1: Checking for obvious parsing errors ===")

    for idx, row in df.iterrows():
        name = row['name']
        facility_id = row['facility_id']

        is_error, reason = validator.check_obvious_errors(name)

        if is_error:
            invalid_facilities.append({
                'facility_id': facility_id,
                'name': name,
                'country_iso3': row['country_iso3'],
                'reason': reason,
                'confidence': 0.95,
                'method': 'pattern_matching'
            })
            logger.info(f"  ✗ {facility_id}: {name} - {reason}")

    logger.info(f"\nFound {len(invalid_facilities)} obvious parsing errors")

    # Second pass: Use LLM for more complex validation
    logger.info("\n=== PASS 2: LLM Analysis of Remaining Facilities ===")

    # Get facilities not already marked as invalid
    invalid_ids = {f['facility_id'] for f in invalid_facilities}
    remaining_df = df[~df['facility_id'].isin(invalid_ids)]

    # Process in batches
    batch_size = 20
    total_batches = (len(remaining_df) + batch_size - 1) // batch_size

    logger.info(f"Processing {len(remaining_df)} facilities in {total_batches} batches...")

    for i in range(0, min(len(remaining_df), 100), batch_size):  # Limit to first 100 for testing
        batch_df = remaining_df.iloc[i:i+batch_size]
        batch = batch_df.to_dict('records')

        logger.info(f"\nBatch {i//batch_size + 1}/{min(5, total_batches)}:")

        results = validator.analyze_batch(batch)

        for result in results:
            if result and 'id' in result:
                facility_row = batch_df[batch_df['facility_id'] == result['id']].iloc[0]

                if not result.get('is_valid', True):
                    invalid_facilities.append({
                        'facility_id': result['id'],
                        'name': facility_row['name'],
                        'country_iso3': facility_row['country_iso3'],
                        'reason': result.get('reason', 'unknown'),
                        'confidence': result.get('confidence', 0.5),
                        'method': 'llm_analysis',
                        'notes': result.get('notes', '')
                    })
                    logger.info(f"  ✗ {result['id']}: {facility_row['name']} - {result.get('reason')}")
                else:
                    valid_facilities.append({
                        'facility_id': result['id'],
                        'name': facility_row['name']
                    })

    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Save invalid facilities
    if invalid_facilities:
        invalid_file = f'output/facilities_to_delete/invalid_facilities_{timestamp}.csv'
        os.makedirs('output/facilities_to_delete', exist_ok=True)

        invalid_df = pd.DataFrame(invalid_facilities)
        invalid_df.to_csv(invalid_file, index=False)

        # Also save as JSON for more details
        invalid_json = f'output/facilities_to_delete/invalid_facilities_{timestamp}.json'
        with open(invalid_json, 'w') as f:
            json.dump(invalid_facilities, f, indent=2)

    # Summary
    print("\n" + "="*60)
    print("FACILITY VALIDATION SUMMARY")
    print("="*60)
    print(f"Total facilities analyzed: {min(120, len(df))}")  # We limited to 120 for testing
    print(f"Invalid facilities found: {len(invalid_facilities)}")
    print(f"Valid facilities: {len(valid_facilities)}")

    if invalid_facilities:
        print("\n" + "-"*60)
        print("TOP INVALID FACILITIES TO DELETE:")
        print("-"*60)

        # Sort by confidence and show top 20
        sorted_invalid = sorted(invalid_facilities, key=lambda x: x['confidence'], reverse=True)[:20]

        for f in sorted_invalid:
            print(f"  {f['facility_id']}: {f['name']}")
            print(f"    Reason: {f['reason']} (confidence: {f['confidence']:.2f})")
            if f.get('notes'):
                print(f"    Notes: {f['notes']}")

        print(f"\n✅ Full list saved to: {invalid_file}")

        # Group by reason
        print("\n" + "-"*60)
        print("BREAKDOWN BY REASON:")
        print("-"*60)

        from collections import Counter
        reasons = Counter(f['reason'] for f in invalid_facilities)
        for reason, count in reasons.most_common():
            print(f"  {reason}: {count} facilities")

    print("\n" + "="*60)


if __name__ == "__main__":
    main()