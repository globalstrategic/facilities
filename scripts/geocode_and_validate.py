#!/usr/bin/env python3
"""
Combined geocoding and validation script:
1. Uses AdvancedGeocoder for actual geocoding
2. Uses LLM to identify obvious parsing errors for deletion
3. Processes facilities missing coordinates
"""

import json
import os
import sys
import csv
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, os.path.expanduser('~/Github/GSMC/entityidentity'))

# Import the existing geocoder
from scripts.utils.geocoding import AdvancedGeocoder, GeocodingResult

# Import OpenAI for validation
import openai
from dotenv import load_dotenv

# Load environment variables
load_dotenv(os.path.expanduser('~/Github/GSMC/entityidentity/.env'))

# Configure OpenAI
openai.api_key = os.getenv('OPENAI_API_KEY')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('output/geocode_validate.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class FacilityValidator:
    """Use LLM to identify obvious parsing errors"""

    def __init__(self, model="gpt-4o-mini", temperature=0.1):
        self.model = model
        self.temperature = temperature
        self.client = openai.OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

    def is_valid_facility(self, name: str, country: str, metal: str = None) -> Tuple[bool, str]:
        """
        Check if a facility name is likely valid or a parsing error.
        Returns: (is_valid, reason)
        """

        # Quick checks for obvious parsing errors
        obvious_errors = [
            "sections)", "not specified", "unknown", "N/A", "various",
            "none", "nil", "null", "-", "--", "...", "?", "TBD",
            "undefined", "unspecified", "other", "misc", "miscellaneous"
        ]

        name_lower = name.lower().strip()
        if name_lower in obvious_errors:
            return False, "obvious_parsing_error"

        # Single word checks (unless it's a known mine name pattern)
        if len(name.split()) == 1 and len(name) < 5:
            return False, "single_short_word"

        # Check for special characters suggesting parsing errors
        if any(char in name for char in [')', ']', '}', '>', '<', '|', '\\']):
            if not any(char in name for char in ['(', '[', '{']):
                return False, "unmatched_brackets"

        # Use LLM for more complex validation
        prompt = f"""
Is this a valid facility name or a parsing error?

Name: "{name}"
Country: {country}
{"Metal: " + metal if metal else ""}

Respond with JSON:
{{
    "is_valid": true/false,
    "reason": "valid_facility" or "parsing_error/truncated/metadata/etc"
}}

Be lenient - if it could plausibly be a facility name, mark as valid.
"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are validating facility names from parsed data."},
                    {"role": "user", "content": prompt}
                ],
                temperature=self.temperature,
                response_format={"type": "json_object"},
                max_tokens=100
            )

            result = json.loads(response.choices[0].message.content)
            return result.get('is_valid', True), result.get('reason', 'unknown')

        except Exception as e:
            logger.warning(f"LLM validation failed for '{name}': {e}")
            # Default to keeping it if LLM fails
            return True, "llm_error"


def process_facility(row: dict, geocoder: AdvancedGeocoder, validator: FacilityValidator) -> dict:
    """Process a single facility: validate and geocode"""

    facility_id = row['facility_id']
    name = row['name']
    country = row['country_iso3']
    metal = row.get('primary_metal', '')

    result = {
        'facility_id': facility_id,
        'name': name,
        'country_iso3': country,
        'status': 'pending',
        'latitude': None,
        'longitude': None,
        'precision': None,
        'confidence': 0.0,
        'is_valid': True,
        'validation_reason': '',
        'geocoding_source': None,
        'error': None
    }

    # Step 1: Validate the facility name
    logger.info(f"Validating: {facility_id}")
    is_valid, reason = validator.is_valid_facility(name, country, metal)

    if not is_valid:
        result['is_valid'] = False
        result['validation_reason'] = reason
        result['status'] = 'invalid'
        logger.info(f"  Invalid facility: {reason}")
        return result

    # Step 2: Try to geocode the facility
    logger.info(f"Geocoding: {facility_id}")

    try:
        # Prepare commodities list
        commodities = []
        if metal:
            commodities = [metal]

        # Use the advanced geocoder
        geo_result = geocoder.geocode_facility(
            facility_name=name,
            country_iso3=country,
            commodities=commodities,
            aliases=[]  # Could parse from aliases field if available
        )

        if geo_result and geo_result.lat and geo_result.lon:
            result['latitude'] = geo_result.lat
            result['longitude'] = geo_result.lon
            result['precision'] = geo_result.precision
            result['confidence'] = geo_result.confidence
            result['geocoding_source'] = geo_result.source
            result['status'] = 'geocoded'
            logger.info(f"  ✓ Geocoded: {geo_result.lat:.4f}, {geo_result.lon:.4f} ({geo_result.source})")
        else:
            result['status'] = 'not_found'
            logger.info(f"  ✗ No coordinates found")

    except Exception as e:
        result['status'] = 'error'
        result['error'] = str(e)
        logger.error(f"  Error geocoding: {e}")

    return result


def update_facility_json(result: dict):
    """Update the facility JSON file with geocoding results"""

    facility_id = result['facility_id']
    country = result['country_iso3']
    file_path = f"facilities/{country}/{facility_id}.json"

    if not os.path.exists(file_path):
        logger.warning(f"File not found: {file_path}")
        return

    with open(file_path, 'r') as f:
        data = json.load(f)

    # Update location if geocoded
    if result['status'] == 'geocoded' and result['latitude'] and result['longitude']:
        data['location'] = {
            'lat': result['latitude'],
            'lon': result['longitude'],
            'precision': result['precision']
        }

        # Update verification
        data['verification'] = data.get('verification', {})
        data['verification']['last_checked'] = datetime.now().isoformat() + 'Z'
        data['verification']['checked_by'] = 'advanced_geocoding'
        data['verification']['confidence'] = result['confidence']

        # Add source
        if 'sources' not in data:
            data['sources'] = []
        data['sources'].append({
            'type': 'geocoding',
            'id': result['geocoding_source'],
            'date': datetime.now().isoformat()
        })

        # Write back
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"Updated {file_path}")


def main():
    """Main execution"""

    # Load facilities needing geocoding
    csv_file = 'output/Mines_Missing_Coords.csv'

    if not os.path.exists(csv_file):
        logger.error(f"File not found: {csv_file}")
        logger.info("Please run: python scripts/find_missing_coords.py")
        return

    df = pd.read_csv(csv_file)

    # Filter to only facilities without coordinates
    # Skip the ones we may have already updated
    df_todo = df[df['latitude'].isna()].copy()

    logger.info(f"Found {len(df_todo)} facilities needing geocoding")

    # Initialize components
    geocoder = AdvancedGeocoder(
        use_overpass=True,
        use_wikidata=True,
        use_mindat=False,  # Requires API key
        use_web_search=False,  # Requires API keys
        cache_results=True
    )

    validator = FacilityValidator()

    # Process in batches
    batch_size = 10
    test_batch = df_todo.head(batch_size).to_dict('records')

    logger.info(f"\nProcessing test batch of {batch_size} facilities...")
    print("="*60)

    results = []
    for i, row in enumerate(test_batch):
        print(f"\n[{i+1}/{batch_size}] {row['facility_id']}")
        result = process_facility(row, geocoder, validator)
        results.append(result)

        # Small delay to be respectful to services
        time.sleep(0.5)

    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # All results
    all_results_file = f'output/geocoding_results/combined_{timestamp}.json'
    os.makedirs('output/geocoding_results', exist_ok=True)
    with open(all_results_file, 'w') as f:
        json.dump(results, f, indent=2)

    # Invalid facilities for deletion
    invalid = [r for r in results if not r['is_valid']]
    if invalid:
        invalid_file = f'output/facilities_to_delete/invalid_{timestamp}.csv'
        os.makedirs('output/facilities_to_delete', exist_ok=True)
        pd.DataFrame(invalid).to_csv(invalid_file, index=False)

    # Successfully geocoded
    geocoded = [r for r in results if r['status'] == 'geocoded']
    if geocoded:
        geocoded_file = f'output/geocoding_results/geocoded_{timestamp}.csv'
        pd.DataFrame(geocoded).to_csv(geocoded_file, index=False)

    # Summary
    print("\n" + "="*60)
    print("GEOCODING & VALIDATION SUMMARY")
    print("="*60)

    valid_count = len([r for r in results if r['is_valid']])
    invalid_count = len(invalid)
    geocoded_count = len(geocoded)
    not_found_count = len([r for r in results if r['status'] == 'not_found'])

    print(f"Total processed: {len(results)}")
    print(f"Valid facilities: {valid_count}")
    print(f"Invalid (to delete): {invalid_count}")
    print(f"Successfully geocoded: {geocoded_count}")
    print(f"Could not geocode: {not_found_count}")

    if invalid:
        print("\n" + "-"*60)
        print("FACILITIES TO DELETE:")
        for r in invalid:
            print(f"  - {r['facility_id']}: {r['name']}")
            print(f"    Reason: {r['validation_reason']}")

    if geocoded:
        print("\n" + "-"*60)
        print("SUCCESSFULLY GEOCODED:")
        for r in geocoded:
            print(f"  - {r['facility_id']}: {r['name']}")
            print(f"    Coords: {r['latitude']:.4f}, {r['longitude']:.4f}")
            print(f"    Source: {r['geocoding_source']}")

    print("\n" + "="*60)

    # Ask to update JSON files
    if geocoded:
        response = input(f"\nUpdate {len(geocoded)} facility JSON files with coordinates? (y/n): ")
        if response.lower() == 'y':
            for result in geocoded:
                update_facility_json(result)
            print(f"Updated {len(geocoded)} facility files")

    # Ask to continue with more
    remaining = len(df_todo) - batch_size
    if remaining > 0:
        response = input(f"\nContinue with {remaining} more facilities? (y/n): ")
        if response.lower() == 'y':
            # Process remaining in larger batches
            all_results = results.copy()
            batch_size = 50

            remaining_rows = df_todo.iloc[len(results):].to_dict('records')

            for i in range(0, len(remaining_rows), batch_size):
                batch = remaining_rows[i:i+batch_size]
                print(f"\n\nProcessing batch {i//batch_size + 1} ({len(batch)} facilities)")
                print("="*60)

                for j, row in enumerate(batch):
                    print(f"[{i+j+1}/{len(remaining_rows)}] {row['facility_id']}")
                    result = process_facility(row, geocoder, validator)
                    all_results.append(result)

                    # Update JSON immediately if geocoded
                    if result['status'] == 'geocoded':
                        update_facility_json(result)

                    # Respect rate limits
                    time.sleep(0.5)

                # Save intermediate results
                with open(all_results_file, 'w') as f:
                    json.dump(all_results, f, indent=2)

            # Final summary
            print("\n" + "="*60)
            print("FINAL SUMMARY")
            print("="*60)

            geocoded_final = [r for r in all_results if r['status'] == 'geocoded']
            invalid_final = [r for r in all_results if not r['is_valid']]

            print(f"Total processed: {len(all_results)}")
            print(f"Successfully geocoded: {len(geocoded_final)}")
            print(f"Invalid facilities: {len(invalid_final)}")
            print(f"Success rate: {len(geocoded_final)/len(all_results)*100:.1f}%")


if __name__ == "__main__":
    main()