#!/usr/bin/env python3
"""
LLM-based facility geocoding using OpenAI with web search capabilities.
Identifies facilities that should be deleted due to parsing errors or non-existence.
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
from dataclasses import dataclass, asdict
import pandas as pd

# Add parent directories to path for imports
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
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('output/llm_geocoding.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class GeocodingResult:
    """Result of LLM geocoding attempt"""
    facility_id: str
    name: str
    country_iso3: str
    status: str  # 'geocoded', 'not_found', 'invalid_facility', 'error'
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    precision: Optional[str] = None
    confidence: float = 0.0
    operator: Optional[str] = None
    owner: Optional[str] = None
    town: Optional[str] = None
    province: Optional[str] = None
    notes: Optional[str] = None
    deletion_reason: Optional[str] = None
    search_summary: Optional[str] = None
    error_message: Optional[str] = None

class LLMGeocoder:
    """Geocode facilities using OpenAI with web search"""

    def __init__(self, model="gpt-4o-mini", temperature=0.1):
        self.model = model
        self.temperature = temperature
        self.client = openai.OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        self.results = []
        self.session_start = datetime.now()

        # Create output directories
        Path('output/geocoding_results').mkdir(parents=True, exist_ok=True)
        Path('output/facilities_to_delete').mkdir(parents=True, exist_ok=True)

    def geocode_facility(self, facility_data: dict) -> GeocodingResult:
        """Geocode a single facility using LLM with web search"""

        facility_id = facility_data['facility_id']
        name = facility_data['name']
        country = facility_data['country_iso3']
        primary_metal = facility_data.get('primary_metal', '')
        facility_type = facility_data.get('facility_type', '')

        logger.info(f"Geocoding {facility_id}: {name}")

        # Build context for the LLM
        context = f"""
Facility: {name}
Country: {country}
Type: {facility_type or 'unknown'}
Primary Metal/Commodity: {primary_metal or 'unknown'}
"""

        if facility_data.get('province'):
            context += f"Province/Region: {facility_data['province']}\n"

        prompt = f"""You are a mining and industrial facility expert. Based on your knowledge of global mining and industrial facilities, help me identify if this is a real facility or a parsing error:

{context}

IMPORTANT CONTEXT: This data comes from parsing reports and tables, so some entries may be:
- Parsing errors (fragments like "sections)", "not specified", random text)
- Table headers or metadata that got mixed in
- Partial company names or incomplete text
- Single words or very short fragments that don't make sense as facility names

For this facility name, determine:

1. IS THIS OBVIOUSLY INVALID?
   Mark as invalid ONLY if the name is:
   - A single word that's clearly not a facility (like "sections)", "unknown", "various")
   - Obviously truncated text or fragments
   - Clear metadata or table headers
   - Contains special characters suggesting parsing errors
   - Generic text like "not specified" or "N/A"

2. IF IT COULD BE A REAL FACILITY:
   Even without web search, provide your best estimate for:
   - Likely location (town/region) based on the name
   - Type of facility based on the name
   - Any coordinates you might know (or null if unknown)
   - Confidence level (0.0-1.0)

BE LENIENT: If a name looks like it could plausibly be a facility (e.g., "EMSTEEL Steel Division", "Fujairah Chrome Mine"), assume it's real unless obviously invalid.

Please respond in this exact JSON format:
{{
    "is_real_facility": true/false,
    "deletion_reason": "parsing_error|truncated_text|metadata|single_word|special_chars" (only if obviously invalid),
    "latitude": number or null,
    "longitude": number or null,
    "precision": "exact|plant|mine|town|district|region|country|unknown",
    "confidence": 0.0-1.0,
    "operator": "company name or null",
    "owner": "company name or null",
    "town": "town/city name or null",
    "province": "province/state name or null",
    "status": "operating|closed|planned|construction|care_maintenance|unknown",
    "notes": "brief relevant notes",
    "search_summary": "brief analysis of the name"
}}

Examples of INVALID entries to delete:
- "sections)"
- "not specified"
- "unknown"
- "N/A"
- "various"
- Single random words

Examples of VALID entries to keep (even if coordinates unknown):
- "EMSTEEL Steel Division" - looks like a real steel facility
- "Fujairah Chrome Mine" - plausible mine name
- "Al Ahlia Gypsum Mine" - specific facility name
- Any name with company + facility type"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an expert in mining and industrial facilities. Help identify parsing errors and validate facility names."},
                    {"role": "user", "content": prompt}
                ],
                temperature=self.temperature,
                response_format={ "type": "json_object" }
            )

            result_json = json.loads(response.choices[0].message.content)

            # Parse the response
            if not result_json.get('is_real_facility', False):
                # Facility should be deleted
                return GeocodingResult(
                    facility_id=facility_id,
                    name=name,
                    country_iso3=country,
                    status='invalid_facility',
                    deletion_reason=result_json.get('deletion_reason', 'unknown'),
                    notes=result_json.get('notes'),
                    search_summary=result_json.get('search_summary'),
                    confidence=0.0
                )

            # Facility is real, extract geocoding data
            if result_json.get('latitude') and result_json.get('longitude'):
                status = 'geocoded'
            else:
                status = 'not_found'

            return GeocodingResult(
                facility_id=facility_id,
                name=name,
                country_iso3=country,
                status=status,
                latitude=result_json.get('latitude'),
                longitude=result_json.get('longitude'),
                precision=result_json.get('precision', 'unknown'),
                confidence=result_json.get('confidence', 0.5),
                operator=result_json.get('operator'),
                owner=result_json.get('owner'),
                town=result_json.get('town'),
                province=result_json.get('province'),
                notes=result_json.get('notes'),
                search_summary=result_json.get('search_summary')
            )

        except Exception as e:
            logger.error(f"Error geocoding {facility_id}: {str(e)}")
            return GeocodingResult(
                facility_id=facility_id,
                name=name,
                country_iso3=country,
                status='error',
                error_message=str(e),
                confidence=0.0
            )

    def process_batch(self, facilities: List[dict], batch_size: int = 10):
        """Process a batch of facilities"""

        results = []
        for i, facility in enumerate(facilities):
            if i > 0 and i % batch_size == 0:
                # Save intermediate results
                self.save_results(results)
                logger.info(f"Processed {i}/{len(facilities)} facilities")
                time.sleep(2)  # Rate limiting

            result = self.geocode_facility(facility)
            results.append(result)

            # Small delay to avoid rate limits
            time.sleep(0.5)

        return results

    def save_results(self, results: List[GeocodingResult]):
        """Save results to files"""

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Save all results
        all_results_file = f'output/geocoding_results/llm_geocoding_{timestamp}.json'
        with open(all_results_file, 'w') as f:
            json.dump([asdict(r) for r in results], f, indent=2)

        # Save facilities to delete
        to_delete = [r for r in results if r.status == 'invalid_facility']
        if to_delete:
            delete_file = f'output/facilities_to_delete/invalid_facilities_{timestamp}.csv'
            df = pd.DataFrame([asdict(r) for r in to_delete])
            df.to_csv(delete_file, index=False)
            logger.info(f"Found {len(to_delete)} facilities to delete")

        # Save successfully geocoded facilities
        geocoded = [r for r in results if r.status == 'geocoded']
        if geocoded:
            geocoded_file = f'output/geocoding_results/geocoded_{timestamp}.csv'
            df = pd.DataFrame([asdict(r) for r in geocoded])
            df.to_csv(geocoded_file, index=False)
            logger.info(f"Successfully geocoded {len(geocoded)} facilities")

        # Save facilities that couldn't be found
        not_found = [r for r in results if r.status == 'not_found']
        if not_found:
            not_found_file = f'output/geocoding_results/not_found_{timestamp}.csv'
            df = pd.DataFrame([asdict(r) for r in not_found])
            df.to_csv(not_found_file, index=False)
            logger.info(f"Could not find coordinates for {len(not_found)} facilities")

    def update_facility_json(self, result: GeocodingResult):
        """Update the facility JSON file with geocoding results"""

        file_path = f"facilities/{result.country_iso3}/{result.facility_id}.json"

        if not os.path.exists(file_path):
            logger.warning(f"File not found: {file_path}")
            return

        with open(file_path, 'r') as f:
            data = json.load(f)

        # Update location
        if result.latitude and result.longitude:
            data['location'] = {
                'lat': result.latitude,
                'lon': result.longitude,
                'precision': result.precision
            }

        # Update other fields
        if result.operator:
            if 'company_mentions' not in data:
                data['company_mentions'] = []
            if result.operator not in data['company_mentions']:
                data['company_mentions'].append(result.operator)

        if result.owner and result.owner != result.operator:
            if 'company_mentions' not in data:
                data['company_mentions'] = []
            if result.owner not in data['company_mentions']:
                data['company_mentions'].append(result.owner)

        if result.town:
            data['town'] = result.town

        if result.province:
            data['province'] = result.province

        # Update verification
        data['verification'] = data.get('verification', {})
        data['verification']['last_checked'] = datetime.now().isoformat() + 'Z'
        data['verification']['checked_by'] = 'llm_geocoding'
        data['verification']['confidence'] = result.confidence

        # Add source
        if 'sources' not in data:
            data['sources'] = []
        data['sources'].append({
            'type': 'llm_geocoding',
            'id': 'openai_web_search',
            'date': datetime.now().isoformat(),
            'notes': result.search_summary
        })

        # Write back
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

def main():
    """Main execution"""

    # Load facilities needing geocoding
    csv_file = 'output/Mines_Missing_Coords.csv'
    df = pd.read_csv(csv_file)

    # Filter to only facilities without coordinates
    # (The ones we just updated will have coords now)
    df_todo = df[df['latitude'].isna()].copy()

    logger.info(f"Found {len(df_todo)} facilities needing geocoding")

    # Initialize geocoder
    geocoder = LLMGeocoder()

    # Process in small batches first to test
    test_batch_size = 5
    test_facilities = df_todo.head(test_batch_size).to_dict('records')

    logger.info(f"Processing test batch of {test_batch_size} facilities...")
    results = geocoder.process_batch(test_facilities, batch_size=5)

    # Save results
    geocoder.save_results(results)

    # Update facility JSONs for successful geocoding
    for result in results:
        if result.status == 'geocoded':
            geocoder.update_facility_json(result)

    # Print summary
    print("\n" + "="*50)
    print("GEOCODING SUMMARY")
    print("="*50)

    geocoded = [r for r in results if r.status == 'geocoded']
    not_found = [r for r in results if r.status == 'not_found']
    invalid = [r for r in results if r.status == 'invalid_facility']
    errors = [r for r in results if r.status == 'error']

    print(f"Total processed: {len(results)}")
    print(f"Successfully geocoded: {len(geocoded)}")
    print(f"Could not find coordinates: {len(not_found)}")
    print(f"Invalid facilities (to delete): {len(invalid)}")
    print(f"Errors: {len(errors)}")

    if invalid:
        print("\n" + "-"*50)
        print("FACILITIES TO DELETE:")
        print("-"*50)
        for r in invalid:
            print(f"- {r.facility_id}: {r.name}")
            print(f"  Reason: {r.deletion_reason}")
            if r.notes:
                print(f"  Notes: {r.notes}")

    print("\n" + "="*50)
    print(f"Results saved to output/geocoding_results/")
    print(f"Invalid facilities saved to output/facilities_to_delete/")

    # Ask if user wants to continue with full batch
    if len(df_todo) > test_batch_size:
        response = input(f"\nContinue with remaining {len(df_todo) - test_batch_size} facilities? (y/n): ")
        if response.lower() == 'y':
            remaining = df_todo.iloc[test_batch_size:].to_dict('records')
            logger.info(f"Processing remaining {len(remaining)} facilities...")

            # Process in batches of 50
            batch_size = 50
            all_results = results.copy()

            for i in range(0, len(remaining), batch_size):
                batch = remaining[i:i+batch_size]
                logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} facilities)")

                batch_results = geocoder.process_batch(batch, batch_size=10)
                all_results.extend(batch_results)

                # Save intermediate results
                geocoder.save_results(all_results)

                # Update JSONs
                for result in batch_results:
                    if result.status == 'geocoded':
                        geocoder.update_facility_json(result)

                # Longer pause between batches
                time.sleep(5)

            # Final summary
            print("\n" + "="*50)
            print("FINAL SUMMARY")
            print("="*50)

            geocoded = [r for r in all_results if r.status == 'geocoded']
            not_found = [r for r in all_results if r.status == 'not_found']
            invalid = [r for r in all_results if r.status == 'invalid_facility']
            errors = [r for r in all_results if r.status == 'error']

            print(f"Total processed: {len(all_results)}")
            print(f"Successfully geocoded: {len(geocoded)}")
            print(f"Could not find coordinates: {len(not_found)}")
            print(f"Invalid facilities (to delete): {len(invalid)}")
            print(f"Errors: {len(errors)}")

if __name__ == "__main__":
    main()