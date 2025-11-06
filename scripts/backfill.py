#!/usr/bin/env python3
"""
Unified backfill system for enriching existing facilities.

Backfills missing or incomplete data:
- Geocoding: Add coordinates to facilities
- Companies: Resolve company_mentions to canonical IDs
- Metals: Add chemical formulas and categories to commodities
- Mentions: Extract company_mentions from Mines.csv
- Towns: Add town/city names to location field
- Canonical Names: Generate canonical facility names

Usage:
    # Backfill geocoding
    python scripts/backfill.py geocode --country ARE
    python scripts/backfill.py geocode --country ARE --interactive
    python scripts/backfill.py geocode --all --dry-run

    # Backfill company resolution
    python scripts/backfill.py companies --country IND
    python scripts/backfill.py companies --country IND --profile strict
    python scripts/backfill.py companies --all --profile moderate

    # Backfill metal normalization
    python scripts/backfill.py metals --country CHN
    python scripts/backfill.py metals --all

    # Extract company mentions from Mines.csv
    python scripts/backfill.py mentions --country BRA
    python scripts/backfill.py mentions --all --force

    # Backfill town names
    python scripts/backfill.py towns --country ZAF
    python scripts/backfill.py towns --country ZAF --interactive
    python scripts/backfill.py towns --all --dry-run

    # Generate canonical names
    python scripts/backfill.py canonical_names --country USA
    python scripts/backfill.py canonical_names --all

    # Backfill everything
    python scripts/backfill.py all --country ARE --interactive
    python scripts/backfill.py all --all --dry-run

    # Batch mode (multiple countries)
    python scripts/backfill.py geocode --countries ARE,IND,CHN

    # Dry run (preview changes)
    python scripts/backfill.py all --country ARE --dry-run
"""

import argparse
import csv
import glob
import json
import os
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Set, Optional, Tuple
from collections import defaultdict
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

# Add utils to path
sys.path.insert(0, str(Path(__file__).parent))

try:
    from utils.geocoding import AdvancedGeocoder, GeocodingResult
    from utils.country_utils import normalize_country_to_iso3, iso3_to_country_name
    from utils.name_canonicalizer import FacilityNameCanonicalizer, choose_town_from_address
    from utils.geocode_cache import GeocodeCache
    from utils.geo import encode_geohash
except ImportError as e:
    logger.error(f"Failed to import utilities: {e}")
    sys.exit(1)

# Initialize global geocoder (reused across facilities)
_GEOCODER = None

def get_geocoder():
    """Get or create global geocoder instance."""
    global _GEOCODER
    if _GEOCODER is None:
        _GEOCODER = AdvancedGeocoder(
            use_overpass=True,
            use_wikidata=True,
            use_nominatim=True,
            cache_results=True
        )
    return _GEOCODER

# Try to import optional dependencies
try:
    from entityidentity import metal_identifier
    METAL_IDENTIFIER_AVAILABLE = True
except ImportError:
    METAL_IDENTIFIER_AVAILABLE = False
    logger.warning("metal_identifier not available (entityidentity library)")

try:
    from utils.company_resolver import CompanyResolver
    COMPANY_RESOLVER_AVAILABLE = True
except ImportError:
    COMPANY_RESOLVER_AVAILABLE = False
    logger.warning("CompanyResolver not available")

# Paths
ROOT = Path(__file__).parent.parent
FACILITIES_DIR = ROOT / "facilities"
CSV_PATH = ROOT / "gt" / "Mines.csv"


class BackfillStats:
    """Track backfill statistics."""
    def __init__(self):
        self.total = 0
        self.processed = 0
        self.updated = 0
        self.skipped = 0
        self.failed = 0
        self.details = []

    def add_result(self, facility_id: str, status: str, details: str = ""):
        """Add a processing result."""
        self.processed += 1
        if status == "updated":
            self.updated += 1
        elif status == "skipped":
            self.skipped += 1
        elif status == "failed":
            self.failed += 1

        self.details.append({
            'facility_id': facility_id,
            'status': status,
            'details': details
        })

    def print_summary(self, backfill_type: str):
        """Print summary statistics."""
        print(f"\n{'='*60}")
        print(f"BACKFILL SUMMARY: {backfill_type}")
        print(f"{'='*60}")
        print(f"Total facilities: {self.total}")
        print(f"Processed: {self.processed}")
        print(f"Updated: {self.updated}")
        print(f"Skipped: {self.skipped}")
        print(f"Failed: {self.failed}")
        if self.processed > 0:
            print(f"Success rate: {self.updated/self.processed*100:.1f}%")
        print(f"{'='*60}")


def build_global_slug_map(root: str = "facilities") -> Dict[str, str]:
    """
    Scan all facilities to build a global slug → facility_id map.

    Used with --global-dedupe to ensure slug uniqueness across all countries.

    Args:
        root: Root directory containing country subdirectories

    Returns:
        Dictionary mapping canonical_slug → facility_id
    """
    slug_map: Dict[str, str] = {}
    root_path = Path(root)

    for p in root_path.glob("*/*.json"):
        try:
            with open(p, "r", encoding="utf-8") as f:
                doc = json.load(f)
            slug = doc.get("canonical_slug")
            fid = doc.get("facility_id")
            if slug and fid and slug not in slug_map:
                slug_map[slug] = fid
        except Exception as e:
            logger.debug(f"Slug-scan skip {p}: {e}")

    logger.info(f"Seeded {len(slug_map)} slugs from {root}/**/*.json")
    return slug_map


def load_facilities_for_country(country_iso3: str) -> List[Dict]:
    """Load all facility JSONs for a country."""
    facilities = []
    country_dir = FACILITIES_DIR / country_iso3

    if not country_dir.exists():
        logger.error(f"No facilities directory found for {country_iso3}")
        return facilities

    for facility_file in country_dir.glob("*.json"):
        try:
            with open(facility_file, 'r') as f:
                facility = json.load(f)
                facility['_path'] = facility_file
                facilities.append(facility)
        except Exception as e:
            logger.warning(f"Could not load {facility_file}: {e}")

    return facilities


def save_facility(facility: Dict, dry_run: bool = False) -> None:
    """Save facility JSON to disk."""
    if dry_run:
        return

    facility_path = facility.get('_path')
    if not facility_path:
        logger.error(f"No path for facility {facility.get('facility_id')}")
        return

    # Remove internal fields
    facility_copy = {k: v for k, v in facility.items() if not k.startswith('_')}

    with open(facility_path, 'w') as f:
        json.dump(facility_copy, f, indent=2, ensure_ascii=False)
        f.write('\n')


def backfill_geocoding(
    facilities: List[Dict],
    country_iso3: str,
    interactive: bool = False,
    dry_run: bool = False
) -> BackfillStats:
    """Backfill missing coordinates."""
    stats = BackfillStats()
    stats.total = len(facilities)

    country_name = iso3_to_country_name(country_iso3)
    logger.info(f"Backfilling geocoding for {country_name} ({country_iso3})")

    # Filter to facilities needing geocoding
    to_geocode = []
    for facility in facilities:
        location = facility.get('location', {})
        if location.get('lat') is None or location.get('lon') is None:
            to_geocode.append(facility)

    logger.info(f"Found {len(to_geocode)}/{len(facilities)} facilities needing geocoding")

    if not to_geocode:
        return stats

    # Import validation functions
    from scripts.utils.geocoding import (
        geocode_via_nominatim, is_valid_coord, in_country_bbox, is_sentinel_coord
    )

    # Geocode each facility using simple, safe Nominatim forward geocoding
    for i, facility in enumerate(to_geocode):
        facility_id = facility['facility_id']
        facility_name = facility.get('name', '')
        logger.info(f"[{i+1}/{len(to_geocode)}] {facility_name}")

        # Compose query: "Facility Name, Country Name"
        query = f"{facility_name}, {country_name}"

        # Try forward geocoding via Nominatim
        result = geocode_via_nominatim(query, country_iso3)

        if result and result.get('lat') and result.get('lon'):
            lat, lon = result['lat'], result['lon']

            # VALIDATION GATES - Prevent garbage coordinates
            if is_sentinel_coord(lat, lon):
                logger.warning(f"  ✗ Sentinel coordinates detected ({lat}, {lon}) - skipping write")
                # Mark as failed in data_quality
                dq = facility.get('data_quality') or {}
                dq.setdefault('flags', {})['sentinel_coords_rejected'] = True
                facility['data_quality'] = dq
                stats.add_result(facility_id, "failed", "Sentinel coordinates rejected")
                continue

            if not is_valid_coord(lat, lon):
                logger.warning(f"  ✗ Invalid coordinates ({lat}, {lon}) - skipping write")
                dq = facility.get('data_quality') or {}
                dq.setdefault('flags', {})['invalid_coords'] = True
                facility['data_quality'] = dq
                stats.add_result(facility_id, "failed", "Invalid coordinates")
                continue

            if not in_country_bbox(lat, lon, country_iso3):
                logger.warning(f"  ✗ Out-of-country coordinates ({lat}, {lon}) - skipping write")
                dq = facility.get('data_quality') or {}
                dq.setdefault('flags', {})['out_of_country'] = True
                facility['data_quality'] = dq
                stats.add_result(facility_id, "failed", f"Coordinates outside {country_iso3} bbox")
                continue

            # All validations passed - safe to write
            facility['location'] = {
                'lat': lat,
                'lon': lon,
                'precision': 'town'  # Conservative default for Nominatim
            }

            # Update verification
            if 'verification' not in facility:
                facility['verification'] = {}

            facility['verification']['last_checked'] = datetime.now().isoformat()
            notes = f"Forward geocoded via Nominatim: {query}"
            facility['verification']['notes'] = notes

            # Save
            save_facility(facility, dry_run=dry_run)

            action = "Would update" if dry_run else "Updated"
            logger.info(f"  ✓ {action}: {lat}, {lon}")
            stats.add_result(facility_id, "updated", f"{lat}, {lon}")
        else:
            logger.warning(f"  ✗ Failed to geocode - no results from Nominatim")
            # Mark as failed in data_quality (but don't write bad coords)
            dq = facility.get('data_quality') or {}
            dq.setdefault('flags', {})['geocode_failed'] = True
            facility['data_quality'] = dq
            stats.add_result(facility_id, "failed", "No coordinates found")

    return stats


def backfill_companies(
    facilities: List[Dict],
    country_iso3: str,
    profile: str = "moderate",
    dry_run: bool = False
) -> BackfillStats:
    """Backfill company resolution (Phase 2)."""
    stats = BackfillStats()
    stats.total = len(facilities)

    if not COMPANY_RESOLVER_AVAILABLE:
        logger.error("CompanyResolver not available - cannot backfill companies")
        return stats

    logger.info(f"Backfilling company resolution for {country_iso3}")

    # Initialize CompanyResolver
    config_path = ROOT / "config" / "gate_config.json"
    if config_path.exists():
        resolver = CompanyResolver.from_config(str(config_path), profile=profile)
    else:
        resolver = CompanyResolver()

    # Filter to facilities with company_mentions
    to_resolve = []
    for facility in facilities:
        mentions = facility.get('company_mentions', [])
        if mentions:
            to_resolve.append(facility)

    logger.info(f"Found {len(to_resolve)}/{len(facilities)} facilities with company mentions")

    if not to_resolve:
        return stats

    # Resolve each facility's company mentions
    for i, facility in enumerate(to_resolve):
        facility_id = facility['facility_id']
        mentions = facility.get('company_mentions', [])

        logger.info(f"[{i+1}/{len(to_resolve)}] {facility['name']} ({len(mentions)} mentions)")

        try:
            accepted, review, pending = resolver.resolve_mentions(
                mentions,
                facility=facility,
                country_hint=country_iso3
            )

            updated = False

            # Add operator link if we have high-confidence operator
            for rel in accepted:
                if rel.get('role') == 'operator' and not facility.get('operator_link'):
                    facility['operator_link'] = {
                        'company_id': rel['company_id'],
                        'confidence': rel['confidence']
                    }
                    updated = True

            # Add owner links if we have high-confidence owners
            if accepted:
                if 'owner_links' not in facility:
                    facility['owner_links'] = []

                for rel in accepted:
                    if rel.get('role') in ['owner', 'majority_owner', 'minority_owner']:
                        owner_link = {
                            'company_id': rel['company_id'],
                            'role': rel['role'],
                            'confidence': rel['confidence']
                        }
                        if 'percentage' in rel:
                            owner_link['percentage'] = rel['percentage']

                        facility['owner_links'].append(owner_link)
                        updated = True

            if updated:
                # Update verification
                if 'verification' not in facility:
                    facility['verification'] = {}
                facility['verification']['last_checked'] = datetime.now().isoformat()

                save_facility(facility, dry_run=dry_run)

                action = "Would resolve" if dry_run else "Resolved"
                logger.info(f"  ✓ {action}: {len(accepted)} companies")
                stats.add_result(facility_id, "updated", f"{len(accepted)} resolved")
            else:
                logger.info(f"  → No high-confidence matches")
                stats.add_result(facility_id, "skipped", "No high-confidence matches")

        except Exception as e:
            logger.error(f"  ✗ Error resolving companies: {e}")
            stats.add_result(facility_id, "failed", str(e))

    return stats


def backfill_metals(
    facilities: List[Dict],
    dry_run: bool = False
) -> BackfillStats:
    """Backfill metal chemical formulas and categories."""
    stats = BackfillStats()
    stats.total = len(facilities)

    if not METAL_IDENTIFIER_AVAILABLE:
        logger.error("metal_identifier not available - cannot backfill metals")
        return stats

    logger.info("Backfilling metal normalization")

    # Filter to facilities with commodities missing formulas
    to_enrich = []
    for facility in facilities:
        commodities = facility.get('commodities', [])
        for commodity in commodities:
            if not commodity.get('chemical_formula') or not commodity.get('category'):
                to_enrich.append(facility)
                break

    logger.info(f"Found {len(to_enrich)}/{len(facilities)} facilities needing metal enrichment")

    if not to_enrich:
        return stats

    # Enrich each facility's commodities
    for i, facility in enumerate(to_enrich):
        facility_id = facility['facility_id']
        commodities = facility.get('commodities', [])

        logger.info(f"[{i+1}/{len(to_enrich)}] {facility['name']} ({len(commodities)} commodities)")

        updated = False

        for commodity in commodities:
            metal_name = commodity.get('metal')
            if not metal_name:
                continue

            # Skip if already has formula and category
            if commodity.get('chemical_formula') and commodity.get('category'):
                continue

            try:
                # Use metal_identifier from entityidentity
                result = metal_identifier(metal_name)

                if result and result.get('valid'):
                    if not commodity.get('chemical_formula') and result.get('formula'):
                        commodity['chemical_formula'] = result['formula']
                        updated = True

                    if not commodity.get('category') and result.get('category'):
                        commodity['category'] = result['category']
                        updated = True

                    logger.info(f"  ✓ {metal_name} → {result.get('formula')} ({result.get('category')})")

            except Exception as e:
                logger.debug(f"  Could not normalize {metal_name}: {e}")

        if updated:
            # Update verification
            if 'verification' not in facility:
                facility['verification'] = {}
            facility['verification']['last_checked'] = datetime.now().isoformat()

            save_facility(facility, dry_run=dry_run)

            action = "Would enrich" if dry_run else "Enriched"
            stats.add_result(facility_id, "updated", "Metals enriched")
        else:
            stats.add_result(facility_id, "skipped", "No updates needed")

    return stats


def load_mines_csv() -> Dict[int, Dict]:
    """Load Mines.csv indexed by row number."""
    csv_data = {}

    if not CSV_PATH.exists():
        logger.error(f"Mines.csv not found at {CSV_PATH}")
        return csv_data

    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        # Row numbers start at 2 (after header)
        for idx, row in enumerate(reader, start=2):
            csv_data[idx] = row

    logger.info(f"Loaded {len(csv_data)} rows from Mines.csv")
    return csv_data


def parse_group_names(group_names: str) -> List[str]:
    """
    Parse semicolon-separated company names from Group Names field.

    Returns list of unique, cleaned company names.
    """
    if not group_names or not group_names.strip():
        return []

    # Split on semicolon
    names = [n.strip() for n in group_names.split(';') if n.strip()]

    # Remove duplicates while preserving order
    seen = set()
    unique_names = []
    for name in names:
        # Normalize for deduplication (case-insensitive)
        name_lower = name.lower()
        if name_lower not in seen:
            seen.add(name_lower)
            unique_names.append(name)

    return unique_names


def get_csv_row_from_facility(facility: Dict) -> Optional[int]:
    """Extract CSV row number from facility sources."""
    for source in facility.get('sources', []):
        if source.get('type') == 'mines_csv':
            return source.get('row')
    return None


def create_company_mention(name: str, csv_row: int, import_timestamp: str) -> Dict:
    """
    Create a company_mentions entry from CSV Group Name.

    Structure follows the schema expected by enrich_companies.py:
    - name: company name
    - role: 'unknown' (will be mapped to operator during enrichment)
    - source: provenance information
    - confidence: 0.5 (moderate - needs resolution)
    - first_seen: when it was originally imported
    """
    return {
        "name": name,
        "role": "unknown",  # Will be converted to 'operator' by enrich_companies.py
        "source": f"mines_csv_row_{csv_row}",
        "confidence": 0.5,
        "first_seen": import_timestamp,
        "evidence": "Extracted from Mines.csv 'Group Names' field during backfill"
    }


def backfill_mentions_for_facility(
    facility: Dict,
    csv_data: Dict[int, Dict],
    force: bool = False,
    empty_only: bool = True
) -> Tuple[bool, int, str]:
    """
    Backfill company_mentions for a single facility from Mines.csv.

    Returns:
        (modified, mentions_added, status_message)
    """
    facility_id = facility.get('facility_id', '???')

    # Check if facility already has mentions
    existing_mentions = facility.get('company_mentions', [])

    if existing_mentions and empty_only and not force:
        return False, 0, f"Already has {len(existing_mentions)} mentions (skipped)"

    # Get CSV row
    csv_row = get_csv_row_from_facility(facility)
    if not csv_row:
        return False, 0, "No mines_csv source found"

    # Look up row in CSV
    if csv_row not in csv_data:
        return False, 0, f"CSV row {csv_row} not found"

    # Extract Group Names
    group_names_raw = csv_data[csv_row].get('Group Names', '').strip()
    if not group_names_raw:
        return False, 0, f"No Group Names in CSV row {csv_row}"

    # Parse company names
    company_names = parse_group_names(group_names_raw)
    if not company_names:
        return False, 0, "No valid company names parsed"

    # Get original import timestamp from verification
    import_timestamp = facility.get('verification', {}).get('last_checked', datetime.now().isoformat())

    # Create company_mentions entries
    new_mentions = [
        create_company_mention(name, csv_row, import_timestamp)
        for name in company_names
    ]

    # Merge with existing mentions if force mode
    if force and existing_mentions:
        # Deduplicate by name (case-insensitive)
        existing_names = {m['name'].lower() for m in existing_mentions}
        new_mentions = [m for m in new_mentions if m['name'].lower() not in existing_names]

        if not new_mentions:
            return False, 0, "All mentions already exist"

        final_mentions = existing_mentions + new_mentions
    else:
        final_mentions = new_mentions

    # Apply changes
    facility['company_mentions'] = final_mentions

    return True, len(new_mentions), f"Added {len(new_mentions)} mentions from CSV"


def backfill_mentions(
    facilities: List[Dict],
    csv_data: Dict[int, Dict],
    force: bool = False,
    empty_only: bool = True,
    dry_run: bool = False
) -> BackfillStats:
    """Backfill company_mentions from Mines.csv Group Names field."""
    stats = BackfillStats()
    stats.total = len(facilities)

    logger.info("Backfilling company mentions from Mines.csv")

    if not csv_data:
        logger.error("No CSV data loaded - cannot backfill mentions")
        return stats

    # Process each facility
    for i, facility in enumerate(facilities):
        facility_id = facility['facility_id']

        modified, mentions_added, status = backfill_mentions_for_facility(
            facility,
            csv_data,
            force=force,
            empty_only=empty_only
        )

        if modified:
            # Update verification
            if 'verification' not in facility:
                facility['verification'] = {}
            facility['verification']['last_checked'] = datetime.now().isoformat()

            save_facility(facility, dry_run=dry_run)

            action = "Would add" if dry_run else "Added"
            logger.info(f"  ✓ {action} {mentions_added} mentions: {facility['name']}")
            stats.add_result(facility_id, "updated", f"{mentions_added} mentions added")
        else:
            logger.debug(f"  → Skipped: {facility['name']} - {status}")
            stats.add_result(facility_id, "skipped", status)

    return stats


def backfill_towns(
    facilities: List[Dict],
    country_iso3: str,
    interactive: bool = False,
    dry_run: bool = False,
    geohash_precision: int = 7,
    nominatim_delay: float = 1.0,
    offline: bool = False,
) -> BackfillStats:
    """
    Backfill location.town field using multi-strategy approach.

    Strategies (in order):
    1. Extract from facility name/aliases (e.g., "Olympic Dam (Roxby Downs)")
    2. Industrial zones database lookup
    3. Reverse geocoding via Nominatim
    4. Mining district mapping
    5. Interactive prompting (if --interactive flag enabled)

    Marks as "TODO" if automated methods fail.
    """
    stats = BackfillStats()
    stats.total = len(facilities)

    logger.info("Backfilling town/city names")

    # Filter to facilities missing town
    to_enrich = []
    for facility in facilities:
        location = facility.get('location', {})
        town = location.get('town')
        if not town or town == "TODO":
            to_enrich.append(facility)

    logger.info(f"Found {len(to_enrich)}/{len(facilities)} facilities needing town enrichment")

    if not to_enrich:
        return stats

    # Prepare geocoder for reverse lookups
    geocoder = get_geocoder()

    # Initialize geocode cache
    with GeocodeCache(ttl_days=365) as cache:
        # Process each facility
        for i, facility in enumerate(to_enrich):
            facility_id = facility['facility_id']
            location = facility.get('location', {})
            lat = location.get('lat')
            lon = location.get('lon')

            logger.info(f"[{i+1}/{len(to_enrich)}] {facility['name']}")

            town = None

            # Strategy 1: Extract from name/aliases
            town = extract_town_from_name(facility)
            if town:
                logger.info(f"  → Found town in name: {town}")

            # Strategy 2: Industrial zones (if applicable)
            if not town and lat and lon:
                town = lookup_industrial_zone(lat, lon, country_iso3)
                if town:
                    logger.info(f"  → Found town from industrial zone: {town}")

            # Strategy 3: Reverse geocoding via Nominatim (deterministic selection + cache)
            strategy = None
            if not town and lat and lon:
                # Check cache first
                cached_address = cache.get(lat, lon)
                if cached_address:
                    town_candidate = choose_town_from_address(cached_address)
                    if town_candidate:
                        town = town_candidate
                        strategy = 'reverse_geocode'
                        logger.info(f"  → Found town via cached geocoding: {town}")
                else:
                    # Cache miss - call Nominatim (unless offline mode)
                    if not offline:
                        try:
                            import requests
                            import time
                            url = "https://nominatim.openstreetmap.org/reverse"
                            params = {'lat': lat, 'lon': lon, 'format': 'json', 'addressdetails': 1}
                            ua_contact = os.getenv("OSM_CONTACT_EMAIL", "ops@gsmc.example")
                            headers = {'User-Agent': f'GSMC-Facilities/2.1 (contact: {ua_contact})'}
                            response = requests.get(url, params=params, headers=headers, timeout=10)
                            time.sleep(max(0.0, float(nominatim_delay)))  # OSM policy compliance
                            if response.status_code == 200:
                                address = (response.json() or {}).get('address', {})

                                # Cache the result
                                cache.set(lat, lon, address)

                                town_candidate = choose_town_from_address(address)  # town > city > municipality > village > hamlet
                                if town_candidate:
                                    town = town_candidate
                                    strategy = 'reverse_geocode'
                                    logger.info(f"  → Found town via reverse geocoding: {town}")
                        except Exception as e:
                            logger.debug(f"  Reverse geocoding failed: {e}")

            # Strategy 4: Interactive prompting
            if not town and interactive:
                print(f"\nFacility: {facility['name']} ({facility_id})")
                if lat and lon:
                    print(f"Coordinates: {lat}, {lon}")
                user_input = input("Enter town/city name (or press Enter to skip): ").strip()
                if user_input:
                    town = user_input
                    logger.info(f"  → User provided: {town}")

            # If still unknown, leave null and flag in data_quality
            if not town:
                logger.info("  → No automated town; leaving null (flagging town_missing)")

            # Update facility
            if not dry_run:
                if 'location' not in facility:
                    facility['location'] = {'precision': 'unknown'}
                facility['location']['town'] = town if town else None

                # Fill geohash if missing and coords present
                if lat is not None and lon is not None and not facility['location'].get('geohash'):
                    facility['location']['geohash'] = encode_geohash(
                        float(lat), float(lon), precision=geohash_precision
                    )

                # Record resolution strategy
                if strategy:
                    facility['location']['town_resolution_strategy'] = strategy

                # Flag data quality
                dq = facility.get('data_quality') or {}
                flags = dq.get('flags') or {}
                flags['town_missing'] = (facility['location']['town'] is None)
                dq['flags'] = flags
                facility['data_quality'] = dq

                # Update verification
                set_verification_note(facility, f"Town enriched: {town or 'null'}")

                # Save facility
                save_facility(facility, dry_run=dry_run)
                logger.info(f"  ✓ Updated: {facility_id}")
                stats.add_result(facility_id, "updated", f"Town: {town or 'null'}")
            else:
                logger.info(f"  [DRY RUN] Would set town: {town or 'null'}")
                stats.add_result(facility_id, "skipped", f"Would set town: {town or 'null'}")

        # Print cache statistics
        cache_stats = cache.stats()
        logger.info(f"\n{'='*60}")
        logger.info(f"GEOCODE CACHE STATISTICS")
        logger.info(f"{'='*60}")
        logger.info(f"Cache size: {cache_stats['size']} entries")
        logger.info(f"Cache hits: {cache_stats['hits']}")
        logger.info(f"Cache misses: {cache_stats['misses']}")
        if cache_stats['hits'] + cache_stats['misses'] > 0:
            hit_rate = 100 * cache_stats['hits'] / (cache_stats['hits'] + cache_stats['misses'])
            logger.info(f"Hit rate: {hit_rate:.1f}%")
        logger.info(f"Loads: {cache_stats['loads']}")
        logger.info(f"Saves: {cache_stats['saves']}")
        logger.info(f"Pruned: {cache_stats['pruned']} expired entries")
        logger.info(f"Backend: {cache_stats['backend']}")
        logger.info(f"TTL: {cache_stats['ttl_days']} days")
        logger.info(f"Path: {cache_stats['path']}")
        logger.info(f"{'='*60}\n")

    return stats


def extract_town_from_name(facility: Dict) -> Optional[str]:
    """
    Extract town name from facility name or aliases using pattern matching.

    Looks for patterns like:
    - "Name (Town)"
    - "Name Town"
    - "Town Name Mine"

    Returns:
        Extracted town name or None
    """
    import re

    # Check name and aliases
    names_to_check = [facility['name']]
    if facility.get('aliases'):
        names_to_check.extend(facility['aliases'])

    # Pattern 1: Parenthetical (e.g., "Olympic Dam (Roxby Downs)")
    for name in names_to_check:
        match = re.search(r'\(([^)]+)\)', name)
        if match:
            town = match.group(1).strip()
            # Filter out obvious non-town patterns
            if not any(word in town.lower() for word in ['mine', 'project', 'complex', 'smelter', 'refinery', 'deposit']):
                return town

    # Pattern 2: Common location indicators (e.g., "Rustenburg Two Rivers")
    # This is more complex and requires heuristics
    # TODO: Implement if needed

    return None


def set_verification_note(facility: dict, suffix: str):
    """
    Set verification note in a null-safe, consistent manner.

    Args:
        facility: Facility dict to update
        suffix: Note suffix to append
    """
    if not facility.get('verification'):
        facility['verification'] = {}
    v = facility['verification']
    v['last_checked'] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    prev = v.get('notes') or ''
    v['notes'] = f"{prev} | {suffix}" if prev else suffix


def lookup_industrial_zone(lat: float, lon: float, country_iso3: str) -> Optional[str]:
    """
    Lookup town from industrial zones database.

    Currently supports UAE. Can be extended for other countries.

    Returns:
        Town name if zone matched, None otherwise
    """
    # UAE industrial zones (from existing backfill.py)
    if country_iso3 == "ARE":
        zones = {
            "Khalifa Industrial Zone Abu Dhabi (KIZAD)": (24.086, 52.541),
            "Jebel Ali Free Zone": (25.012, 55.106),
            "Dubai Industrial City": (24.994, 55.149),
            "Al Ain Industrial Area": (24.228, 55.732),
            "Musaffah Industrial Area": (24.354, 54.512),
        }

        # Check if facility is within 5km of any zone
        for zone_name, (zone_lat, zone_lon) in zones.items():
            distance = ((lat - zone_lat)**2 + (lon - zone_lon)**2)**0.5
            if distance < 0.05:  # Roughly 5km
                # Extract city name from zone name
                if "Abu Dhabi" in zone_name:
                    return "Abu Dhabi"
                elif "Dubai" in zone_name or "Jebel Ali" in zone_name:
                    return "Dubai"
                elif "Al Ain" in zone_name:
                    return "Al Ain"

    # Add more countries as needed
    # TODO: Expand industrial zones database

    return None


def backfill_canonical_names(
    facilities: List[Dict],
    country_iso3: str,
    dry_run: bool = False,
    existing_slugs_init: Optional[Dict[str, str]] = None,
    rebuild_slugs: bool = False,
) -> BackfillStats:
    """
    Backfill canonical_name, display_name, canonical_slug, primary_type/type_confidence,
    and data_quality flags using FacilityNameCanonicalizer.

    - Slugs EXCLUDE operator (stable through operator churn)
    - Uniqueness is enforced within the processed set via in-memory slug map
    - Can seed with global slug map via existing_slugs_init for --global-dedupe

    Args:
        facilities: List of facility dicts to process
        country_iso3: ISO3 country code
        dry_run: If True, don't save changes
        existing_slugs_init: Optional pre-seeded slug map from global scan

    Returns:
        BackfillStats with results
    """
    stats = BackfillStats()
    stats.total = len(facilities)

    logger.info("Backfilling canonical names & slugs")

    canonicalizer = FacilityNameCanonicalizer()

    # Build slug map from existing data (collision avoidance)
    # Start with global seed if provided, then add current batch
    existing_slugs: Dict[str, str] = dict(existing_slugs_init or {})
    for fac in facilities:
        slug = fac.get('canonical_slug')
        if slug:
            existing_slugs[slug] = fac.get('facility_id')

    for i, facility in enumerate(facilities):
        facility_id = facility.get('facility_id')
        logger.info(f"[{i+1}/{len(facilities)}] {facility.get('name')} ({facility_id})")

        try:
            # Preserve existing slug unless --rebuild-slugs flag is set
            if facility.get('canonical_slug') and not rebuild_slugs:
                canonical_slug = facility['canonical_slug']
                # Still run canonicalizer for name/type updates
                result = canonicalizer.canonicalize_facility(facility, existing_slugs)
                canonical_name = result['canonical_name'] or None
                display_name = result['display_name'] or None
                # Override slug with existing
                result['canonical_slug'] = canonical_slug
            else:
                result = canonicalizer.canonicalize_facility(facility, existing_slugs)
                canonical_name = result['canonical_name'] or None
                display_name = result['display_name'] or None
                canonical_slug = result['canonical_slug']
            comps = result['canonical_components']  # town, operator_display, core, primary_type
            conf = result['canonicalization_confidence']
            detail = result['canonicalization_detail']  # {'type': .., 'core': .., ...}

            # Flags
            town_missing = ((facility.get('location') or {}).get('town') is None)
            operator_unresolved = bool(
                (facility.get('company_mentions') or facility.get('operators')) and not facility.get('operator_display')
            )
            # Consider canonical "incomplete" if missing primary_type OR (missing town and we have coords)
            has_coords = bool((facility.get('location') or {}).get('lat') and (facility.get('location') or {}).get('lon'))
            canonical_incomplete = (comps.get('primary_type') is None) or (town_missing and has_coords)

            if dry_run:
                logger.info(f"  [DRY RUN] → {canonical_name}  | slug={canonical_slug}  | conf={conf:.2f}")
                stats.add_result(facility_id or "?", "skipped",
                                 f"Would set canonical='{canonical_name}', slug='{canonical_slug}', conf={conf:.2f}")
                # Reserve the slug so future dry-run rows show disambiguation
                existing_slugs[canonical_slug] = facility_id or f"?-{i}"
                continue

            # --- mutate facility ---
            # 1) primary_type / type_confidence
            if not facility.get('primary_type') and comps.get('primary_type'):
                facility['primary_type'] = comps['primary_type']
            if detail.get('type') is not None:
                old_tc = facility.get('type_confidence')
                new_tc = float(detail['type'])
                if old_tc is None or (isinstance(old_tc, (int, float)) and new_tc > float(old_tc)):
                    facility['type_confidence'] = new_tc

            # 2) names & slug
            prev_name = facility.get('canonical_name')
            if prev_name and prev_name != canonical_name:
                # Append to history if schema present; use last_checked as 'from' if available
                history = facility.get('canonical_name_history') or []
                from_ts = facility.get('verification', {}).get('last_checked') or datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
                history.append({
                    "name": prev_name,
                    "from": from_ts,
                    "to": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                    "reason": "data_correction"
                })
                facility['canonical_name_history'] = history

            facility['canonical_name'] = canonical_name
            facility['display_name'] = display_name
            facility['display_name_source'] = 'manual' if facility.get('display_name_override') else 'auto'
            facility['canonical_slug'] = canonical_slug

            # 3) data_quality
            dq = facility.get('data_quality') or {}
            flags = dq.get('flags') or {}
            flags['town_missing'] = town_missing
            flags['operator_unresolved'] = operator_unresolved
            flags['canonical_name_incomplete'] = canonical_incomplete
            dq['flags'] = flags
            dq['canonicalization_confidence'] = conf
            facility['data_quality'] = dq

            # 4) verification
            set_verification_note(facility, "Canonical name+slug generated")

            # Persist
            save_facility(facility, dry_run=dry_run)
            stats.add_result(facility_id or "?", "updated", f"{canonical_name} [{canonical_slug}]")

            # Reserve slug to avoid collisions downstream in this run
            existing_slugs[canonical_slug] = facility_id

        except Exception as e:
            logger.exception(f"  ✗ Error generating canonical for {facility_id}: {e}")
            stats.add_result(facility_id or "?", "failed", str(e))

    # Final intra-batch collision check (backstop for edge cases)
    logger.info("\nPerforming final collision check...")
    seen_slugs = {}
    collisions = []

    for fac in facilities:
        slug = fac.get('canonical_slug')
        fac_id = fac.get('facility_id')
        if not slug or not fac_id:
            continue

        if slug in seen_slugs:
            collisions.append((seen_slugs[slug], fac_id, slug))
        else:
            seen_slugs[slug] = fac_id

    if collisions:
        logger.warning(f"⚠ Found {len(collisions)} intra-batch slug collisions!")
        for fac1, fac2, slug in collisions:
            logger.warning(f"  Collision: {slug} -> {fac1} vs {fac2}")
            # Apply disambiguation fix inline
            fac2_obj = next((f for f in facilities if f.get('facility_id') == fac2), None)
            if fac2_obj:
                loc = fac2_obj.get('location', {})
                reg = loc.get('region') or ''
                town = loc.get('town') or ''
                from scripts.utils.name_parts import slugify, to_ascii

                suffix = slugify(reg) if reg else (slugify(town) if town else to_ascii(fac2)[-4:].lower())
                new_slug = f"{slug}-{suffix}"
                fac2_obj['canonical_slug'] = new_slug

                set_verification_note(fac2_obj, f"Slug collision resolved: {slug} -> {new_slug}")
                save_facility(fac2_obj, dry_run=dry_run)
                logger.info(f"  ✓ Fixed: {fac2} -> {new_slug}")

    return stats


def backfill_all(
    facilities: List[Dict],
    country_iso3: str,
    interactive: bool = False,
    company_profile: str = "moderate",
    dry_run: bool = False
) -> Dict[str, BackfillStats]:
    """Run all backfill operations."""
    results = {}

    logger.info("Running all backfill operations")

    # 1. Geocoding
    logger.info("\n=== STEP 1: GEOCODING ===")
    results['geocoding'] = backfill_geocoding(
        facilities,
        country_iso3,
        interactive=interactive,
        dry_run=dry_run
    )

    # 2. Metal normalization
    logger.info("\n=== STEP 2: METAL NORMALIZATION ===")
    results['metals'] = backfill_metals(
        facilities,
        dry_run=dry_run
    )

    # 3. Company resolution
    logger.info("\n=== STEP 3: COMPANY RESOLUTION ===")
    results['companies'] = backfill_companies(
        facilities,
        country_iso3,
        profile=company_profile,
        dry_run=dry_run
    )

    # 4. Town enrichment
    logger.info("\n=== STEP 4: TOWN ENRICHMENT ===")
    results['towns'] = backfill_towns(
        facilities,
        country_iso3,
        interactive=interactive,
        dry_run=dry_run
    )

    # 5. Canonical names
    logger.info("\n=== STEP 5: CANONICAL NAMES ===")
    results['canonical_names'] = backfill_canonical_names(
        facilities,
        country_iso3,
        dry_run=dry_run
    )

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Unified backfill system for enriching facilities"
    )

    subparsers = parser.add_subparsers(dest='command', required=True)

    # Geocode subcommand
    geocode_parser = subparsers.add_parser('geocode', help='Backfill coordinates')
    geocode_parser.add_argument('--country', help='Country ISO3 code')
    geocode_parser.add_argument('--countries', help='Comma-separated country codes')
    geocode_parser.add_argument('--all', action='store_true', help='Process all countries')
    geocode_parser.add_argument('--interactive', action='store_true', help='Interactive prompting')
    geocode_parser.add_argument('--dry-run', action='store_true', help='Preview changes')

    # Companies subcommand
    companies_parser = subparsers.add_parser('companies', help='Backfill company resolution')
    companies_parser.add_argument('--country', help='Country ISO3 code')
    companies_parser.add_argument('--countries', help='Comma-separated country codes')
    companies_parser.add_argument('--all', action='store_true', help='Process all countries')
    companies_parser.add_argument('--profile', default='moderate', choices=['strict', 'moderate', 'permissive'])
    companies_parser.add_argument('--dry-run', action='store_true', help='Preview changes')

    # Metals subcommand
    metals_parser = subparsers.add_parser('metals', help='Backfill metal normalization')
    metals_parser.add_argument('--country', help='Country ISO3 code')
    metals_parser.add_argument('--countries', help='Comma-separated country codes')
    metals_parser.add_argument('--all', action='store_true', help='Process all countries')
    metals_parser.add_argument('--dry-run', action='store_true', help='Preview changes')

    # Mentions subcommand
    mentions_parser = subparsers.add_parser('mentions', help='Extract company mentions from Mines.csv')
    mentions_parser.add_argument('--country', help='Country ISO3 code')
    mentions_parser.add_argument('--countries', help='Comma-separated country codes')
    mentions_parser.add_argument('--all', action='store_true', help='Process all countries')
    mentions_parser.add_argument('--force', action='store_true', help='Add mentions even if facility already has some')
    mentions_parser.add_argument('--dry-run', action='store_true', help='Preview changes')

    # Towns subcommand
    towns_parser = subparsers.add_parser('towns', help='Backfill town/city names')
    towns_parser.add_argument('--country', help='Country ISO3 code')
    towns_parser.add_argument('--countries', help='Comma-separated country codes')
    towns_parser.add_argument('--all', action='store_true', help='Process all countries')
    towns_parser.add_argument('--interactive', action='store_true', help='Interactive prompting for missing towns')
    towns_parser.add_argument('--dry-run', action='store_true', help='Preview changes')
    towns_parser.add_argument('--geohash-precision', type=int, default=7, help='Geohash precision (default: 7)')
    towns_parser.add_argument('--nominatim-delay', type=float, default=float(os.getenv("NOMINATIM_DELAY_S", "1.0")),
                              help='Delay between Nominatim calls in seconds (default: 1.0 or $NOMINATIM_DELAY_S)')
    towns_parser.add_argument('--offline', action='store_true', help='Offline mode: use cache/heuristics only, no Nominatim calls')

    # Canonical names subcommand
    canonical_parser = subparsers.add_parser('canonical_names', help='Generate canonical facility names')
    canonical_parser.add_argument('--country', help='Country ISO3 code')
    canonical_parser.add_argument('--countries', help='Comma-separated country codes')
    canonical_parser.add_argument('--all', action='store_true', help='Process all countries')
    canonical_parser.add_argument('--dry-run', action='store_true', help='Preview changes')
    canonical_parser.add_argument('--rebuild-slugs', action='store_true',
                                  help='Force regeneration of slugs (default: preserve existing)')
    canonical_parser.add_argument('--global-dedupe', action='store_true',
                                  help='Seed slug map with all existing slugs across facilities/*/*.json')
    canonical_parser.add_argument('--global-scan-root', default='facilities',
                                  help='Root directory to scan when using --global-dedupe (default: facilities)')

    # All subcommand
    all_parser = subparsers.add_parser('all', help='Run all backfill operations')
    all_parser.add_argument('--country', help='Country ISO3 code')
    all_parser.add_argument('--countries', help='Comma-separated country codes')
    all_parser.add_argument('--all', action='store_true', help='Process all countries')
    all_parser.add_argument('--interactive', action='store_true', help='Interactive prompting')
    all_parser.add_argument('--profile', default='moderate', choices=['strict', 'moderate', 'permissive'])
    all_parser.add_argument('--dry-run', action='store_true', help='Preview changes')

    args = parser.parse_args()

    # Determine countries to process
    countries = []
    if hasattr(args, 'all') and args.all:
        # Get all country directories
        countries = [d.name for d in FACILITIES_DIR.iterdir() if d.is_dir()]
    elif hasattr(args, 'countries') and args.countries:
        countries = [c.strip() for c in args.countries.split(',')]
    elif hasattr(args, 'country') and args.country:
        countries = [args.country]
    else:
        logger.error("Must specify --country, --countries, or --all")
        return 1

    # Normalize country codes
    normalized_countries = []
    for country in countries:
        iso3 = normalize_country_to_iso3(country)
        if iso3:
            normalized_countries.append(iso3)
        else:
            logger.warning(f"Could not resolve country: {country}")

    if not normalized_countries:
        logger.error("No valid countries to process")
        return 1

    # Load CSV data if needed for mentions command
    csv_data = {}
    if args.command == 'mentions':
        csv_data = load_mines_csv()
        if not csv_data:
            logger.error("Failed to load Mines.csv - cannot backfill mentions")
            return 1

    # Process each country
    all_stats = {}

    for country_iso3 in normalized_countries:
        country_name = iso3_to_country_name(country_iso3)
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing: {country_name} ({country_iso3})")
        logger.info(f"{'='*60}")

        # Load facilities
        facilities = load_facilities_for_country(country_iso3)
        if not facilities:
            logger.warning(f"No facilities found for {country_iso3}")
            continue

        # Run backfill based on command
        if args.command == 'geocode':
            stats = backfill_geocoding(
                facilities,
                country_iso3,
                interactive=args.interactive,
                dry_run=args.dry_run
            )
            all_stats[country_iso3] = {'geocoding': stats}

        elif args.command == 'companies':
            stats = backfill_companies(
                facilities,
                country_iso3,
                profile=args.profile,
                dry_run=args.dry_run
            )
            all_stats[country_iso3] = {'companies': stats}

        elif args.command == 'metals':
            stats = backfill_metals(
                facilities,
                dry_run=args.dry_run
            )
            all_stats[country_iso3] = {'metals': stats}

        elif args.command == 'mentions':
            stats = backfill_mentions(
                facilities,
                csv_data,
                force=args.force,
                empty_only=not args.force,
                dry_run=args.dry_run
            )
            all_stats[country_iso3] = {'mentions': stats}

        elif args.command == 'towns':
            stats = backfill_towns(
                facilities,
                country_iso3,
                interactive=args.interactive,
                dry_run=args.dry_run,
                geohash_precision=args.geohash_precision,
                nominatim_delay=args.nominatim_delay,
                offline=args.offline,
            )
            all_stats[country_iso3] = {'towns': stats}

        elif args.command == 'canonical_names':
            # ALWAYS preseed with global slugs to prevent collisions (idempotent)
            seed = build_global_slug_map(args.global_scan_root)

            stats = backfill_canonical_names(
                facilities,
                country_iso3,
                dry_run=args.dry_run,
                existing_slugs_init=seed,
                rebuild_slugs=getattr(args, 'rebuild_slugs', False)
            )
            all_stats[country_iso3] = {'canonical_names': stats}

        elif args.command == 'all':
            stats = backfill_all(
                facilities,
                country_iso3,
                interactive=args.interactive,
                company_profile=args.profile,
                dry_run=args.dry_run
            )
            all_stats[country_iso3] = stats

    # Print final summary
    print(f"\n{'='*60}")
    print("FINAL SUMMARY")
    print(f"{'='*60}")

    for country_iso3, stats_dict in all_stats.items():
        country_name = iso3_to_country_name(country_iso3)
        print(f"\n{country_name} ({country_iso3}):")
        for backfill_type, stats in stats_dict.items():
            stats.print_summary(backfill_type)

    return 0


if __name__ == "__main__":
    sys.exit(main())
