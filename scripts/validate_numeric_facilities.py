#!/usr/bin/env python3
"""
Validate numeric USA facilities using web search before deletion.
"""

import json
import os
import sys
from pathlib import Path
from tavily import TavilyClient
import openai
from typing import Dict, Optional

# Facilities to validate
FACILITIES_TO_VALIDATE = [
    "usa-1-fac.json",
    "usa-1-strip-mine-fac.json",
    "usa-1-surface-002-section-fac.json",
    "usa-2-c-mine-fac.json",
    "usa-3163-coal-mine-fac.json",
    "usa-5-coal-mine-fac.json",
    "usa-6-coal-mine-fac.json",
    "usa-9-miner-fac.json",
]


def load_facility(filepath: str) -> Dict:
    """Load facility JSON."""
    with open(filepath) as f:
        return json.load(f)


def search_facility(facility: Dict, tavily_client) -> Optional[Dict]:
    """
    Search for facility information using Tavily.
    Returns enriched data or None if not found.
    """
    name = facility.get('name', '')
    lat = facility.get('location', {}).get('lat')
    lon = facility.get('location', {}).get('lon')

    print(f"\n  Facility: {name}")
    print(f"  Coords: {lat:.3f}, {lon:.3f}")

    # Construct search query
    search_query = f"{name} United States coal mine operator owner"
    print(f"  Query: {search_query}")

    try:
        # Search using Tavily
        results = tavily_client.search(
            query=search_query,
            max_results=10,
            include_raw_content=False
        )

        if not results.get('results'):
            print("  ✗ No search results")
            return None

        # Use GPT to extract info from search results
        context = "\n\n".join([
            f"Title: {r.get('title', '')}\nContent: {r.get('content', '')}"
            for r in results['results'][:5]
        ])

        prompt = f"""Given the following search results about "{name}" (coordinates: {lat}, {lon}):

{context}

Extract:
1. The real/full name of this facility (if different from "{name}")
2. The operator/owner company names
3. The location (town, county, state)
4. Whether this is a real, identifiable facility

Return JSON format:
{{
    "is_real": true/false,
    "real_name": "name or null",
    "companies": ["company1", "company2"],
    "location": "town, county, state",
    "confidence": "high/medium/low",
    "reason": "explanation"
}}"""

        client = openai.OpenAI()
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0
        )

        result = json.loads(response.choices[0].message.content)

        print(f"  Result: {result.get('is_real')} - {result.get('confidence')} confidence")
        if result.get('real_name'):
            print(f"  Real name: {result.get('real_name')}")
        if result.get('companies'):
            print(f"  Companies: {', '.join(result.get('companies', []))}")
        if result.get('location'):
            print(f"  Location: {result.get('location')}")

        return result

    except Exception as e:
        print(f"  ✗ Error: {e}")
        return None


def main():
    # Check for API keys
    tavily_key = os.environ.get('TAVILY_API_KEY')
    openai_key = os.environ.get('OPENAI_API_KEY')

    if not tavily_key or not openai_key:
        print("Error: TAVILY_API_KEY and OPENAI_API_KEY environment variables required")
        return 1

    # Change to USA facilities directory
    script_dir = Path(__file__).parent.parent
    usa_dir = script_dir / 'facilities' / 'USA'

    if not usa_dir.exists():
        print(f"Error: Directory not found: {usa_dir}")
        return 1

    os.chdir(usa_dir)

    # Initialize Tavily client
    tavily_client = TavilyClient(api_key=tavily_key)

    print("=" * 70)
    print("VALIDATING NUMERIC USA FACILITIES")
    print("=" * 70)

    results = {
        'validated': [],
        'not_real': [],
        'errors': []
    }

    for filepath in FACILITIES_TO_VALIDATE:
        print(f"\n{'='*70}")
        print(f"Checking: {filepath}")
        print('=' * 70)

        try:
            facility = load_facility(filepath)
            result = search_facility(facility, tavily_client)

            if result:
                if result.get('is_real') and result.get('confidence') in ['high', 'medium']:
                    results['validated'].append({
                        'file': filepath,
                        'facility': facility,
                        'validation': result
                    })
                else:
                    results['not_real'].append({
                        'file': filepath,
                        'facility': facility,
                        'validation': result
                    })
            else:
                results['errors'].append({
                    'file': filepath,
                    'error': 'No search results'
                })

        except Exception as e:
            print(f"  ✗ Error processing: {e}")
            results['errors'].append({
                'file': filepath,
                'error': str(e)
            })

    # Summary
    print("\n" + "=" * 70)
    print("VALIDATION SUMMARY")
    print("=" * 70)
    print(f"\nValidated (real facilities): {len(results['validated'])}")
    for item in results['validated']:
        print(f"  ✓ {item['file']}")
        val = item['validation']
        if val.get('real_name'):
            print(f"    → {val['real_name']}")
        if val.get('companies'):
            print(f"    → {', '.join(val['companies'])}")

    print(f"\nNot real or low confidence: {len(results['not_real'])}")
    for item in results['not_real']:
        print(f"  ✗ {item['file']}")
        val = item['validation']
        print(f"    → {val.get('reason', 'No reason')}")

    print(f"\nErrors: {len(results['errors'])}")
    for item in results['errors']:
        print(f"  ✗ {item['file']}: {item['error']}")

    print("\n" + "=" * 70)
    print("RECOMMENDATION:")
    print("=" * 70)
    print(f"Keep and enrich: {len(results['validated'])} facilities")
    print(f"Safe to delete: {len(results['not_real'])} facilities")
    print("=" * 70)

    return 0


if __name__ == "__main__":
    sys.exit(main())
