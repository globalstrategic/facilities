"""
Deduplication utilities for facilities database.

This module provides the core deduplication logic that can be used both
during import (automatic) and for batch cleanup (manual script).
"""

from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple


def is_duplicate_facility(fac1: Dict, fac2: Dict) -> bool:
    """
    Check if two facilities are duplicates using 4-priority matching.

    Priority 1: Coordinate-based matching (two-tier)
    Priority 2: Exact name match
    Priority 3: Fuzzy name match (similarity + word overlap)
    Priority 4: Alias match

    Args:
        fac1: First facility dict
        fac2: Second facility dict

    Returns:
        True if facilities are duplicates
    """
    # Extract names
    name1 = fac1.get('name', '').lower()
    name2 = fac2.get('name', '').lower()

    # PRIORITY 1: Coordinate-based matching
    lat1 = fac1.get('location', {}).get('lat')
    lon1 = fac1.get('location', {}).get('lon')
    lat2 = fac2.get('location', {}).get('lat')
    lon2 = fac2.get('location', {}).get('lon')

    if lat1 is not None and lon1 is not None and lat2 is not None and lon2 is not None:
        lat_diff = abs(lat1 - lat2)
        lon_diff = abs(lon1 - lon2)

        # Calculate name similarity
        name_similarity = SequenceMatcher(None, name1, name2).ratio()

        # Check if shorter name is contained in longer name
        shorter = name1 if len(name1) < len(name2) else name2
        longer = name2 if len(name1) < len(name2) else name1
        contains_match = shorter in longer

        # Two-tier matching:
        # Tier 1: Very close coords (0.01° ~1km) + moderate name match
        # Tier 2: Close coords (0.1° ~11km) + high name match
        tier1_match = (lat_diff < 0.01 and lon_diff < 0.01) and (name_similarity > 0.6 or contains_match)
        tier2_match = (lat_diff < 0.1 and lon_diff < 0.1) and (name_similarity > 0.85 or contains_match)

        if tier1_match or tier2_match:
            return True

    # PRIORITY 2: Exact name match
    if name1 == name2:
        # If both have coordinates, verify they're close
        if lat1 and lon1 and lat2 and lon2:
            lat_diff = abs(lat1 - lat2)
            lon_diff = abs(lon1 - lon2)
            if lat_diff < 0.01 and lon_diff < 0.01:
                return True
        else:
            # No coords to compare, assume duplicate by exact name
            return True

    # PRIORITY 3: Fuzzy name match
    name_similarity = SequenceMatcher(None, name1, name2).ratio()

    # Word overlap check
    words1 = set(name1.split())
    words2 = set(name2.split())
    if words1 and words2:
        word_overlap = len(words1 & words2) / min(len(words1), len(words2))
    else:
        word_overlap = 0

    # Match if high similarity OR high word overlap
    if name_similarity > 0.85 or word_overlap > 0.8:
        return True

    # PRIORITY 4: Alias match
    aliases1 = [a.lower() for a in fac1.get('aliases', [])]
    aliases2 = [a.lower() for a in fac2.get('aliases', [])]

    if name1 in aliases2 or name2 in aliases1:
        return True

    return False


def score_facility_completeness(fac: Dict) -> float:
    """
    Score a facility by data completeness to determine which to keep in merges.

    Args:
        fac: Facility dictionary

    Returns:
        Completeness score (higher is better)
    """
    score = 0.0

    # Prefer facilities with coordinates
    if fac.get('location', {}).get('lat') is not None:
        score += 10

    # More commodities is better
    score += len(fac.get('commodities', [])) * 2

    # Company mentions
    score += len(fac.get('company_mentions', [])) * 3

    # Products
    score += len(fac.get('products', [])) * 2

    # Aliases
    score += len(fac.get('aliases', []))

    # Known status is better than unknown
    if fac.get('status') != 'unknown':
        score += 5

    # Higher confidence
    score += fac.get('verification', {}).get('confidence', 0) * 10

    # Prefer certain verification statuses
    status = fac.get('verification', {}).get('status', '')
    if status == 'human_verified':
        score += 20
    elif status == 'csv_imported':
        score += 10
    elif status == 'llm_verified':
        score += 5

    return score


def merge_facilities(best: Dict, duplicates: List[Dict]) -> Dict:
    """
    Merge data from duplicate facilities into the best facility.

    Args:
        best: The best facility to keep (will be modified)
        duplicates: List of duplicate facilities to merge from

    Returns:
        The merged facility dictionary
    """
    # Merge aliases from all facilities
    all_aliases = set(best.get('aliases', []))
    for fac in duplicates:
        all_aliases.update(fac.get('aliases', []))
        # Add the facility name as an alias if different from best
        if fac['name'] != best['name']:
            all_aliases.add(fac['name'])

    # Remove the best facility's own name from aliases
    all_aliases.discard(best['name'])

    # Merge sources
    all_sources = list(best.get('sources', []))
    seen_sources = {(s['type'], s['id']) for s in all_sources}

    for fac in duplicates:
        for source in fac.get('sources', []):
            source_key = (source['type'], source['id'])
            if source_key not in seen_sources:
                all_sources.append(source)
                seen_sources.add(source_key)

    # Merge commodities (prefer ones with formulas)
    all_commodities = {}
    for fac in [best] + duplicates:
        for comm in fac.get('commodities', []):
            metal = comm['metal']
            if metal not in all_commodities or comm.get('chemical_formula'):
                all_commodities[metal] = comm

    # Merge company mentions (deduplicate by name, keep highest confidence)
    all_mentions = {}
    for fac in [best] + duplicates:
        for mention in fac.get('company_mentions', []):
            name = mention['name']
            if name not in all_mentions or mention.get('confidence', 0) > all_mentions[name].get('confidence', 0):
                all_mentions[name] = mention

    # Update best facility
    best['aliases'] = sorted(list(all_aliases))
    best['sources'] = all_sources
    best['commodities'] = list(all_commodities.values())
    best['company_mentions'] = list(all_mentions.values())

    # Add note about merge
    notes = best.get('verification', {}).get('notes', '')
    merge_ids = [f['facility_id'] for f in duplicates]
    merge_note = f"Merged from: {', '.join(merge_ids)}"
    if notes:
        best['verification']['notes'] = f"{notes}; {merge_note}"
    else:
        best['verification']['notes'] = merge_note

    return best


def find_duplicate_groups(facilities: List[Dict]) -> List[List[Dict]]:
    """
    Find groups of duplicate facilities.

    Args:
        facilities: List of facility dictionaries

    Returns:
        List of duplicate groups, where each group is a list of facilities
    """
    from collections import defaultdict

    # Build coordinate index for efficient lookup
    coord_index = defaultdict(list)

    for fac in facilities:
        lat = fac.get('location', {}).get('lat')
        lon = fac.get('location', {}).get('lon')
        if lat is not None and lon is not None:
            # Round to 1 decimal place for initial grouping
            coord_key = (round(lat, 1), round(lon, 1))
            coord_index[coord_key].append(fac)

    # Find duplicate groups
    duplicate_groups = []
    processed = set()

    for coord_key, candidates in coord_index.items():
        if len(candidates) < 2:
            continue

        # Check each pair in this coordinate bucket
        for i, fac1 in enumerate(candidates):
            if fac1['facility_id'] in processed:
                continue

            group = [fac1]
            processed.add(fac1['facility_id'])

            for fac2 in candidates[i+1:]:
                if fac2['facility_id'] in processed:
                    continue

                # Check if truly duplicates
                if is_duplicate_facility(fac1, fac2):
                    group.append(fac2)
                    processed.add(fac2['facility_id'])

            if len(group) > 1:
                duplicate_groups.append(group)

    return duplicate_groups


def select_best_facility(facilities: List[Dict]) -> Tuple[Dict, List[Dict]]:
    """
    Select the best facility from a group and return it with the rest as duplicates.

    Args:
        facilities: List of facility dictionaries

    Returns:
        Tuple of (best_facility, list_of_duplicates)
    """
    if not facilities:
        raise ValueError("Cannot select best facility from empty list")

    if len(facilities) == 1:
        return facilities[0], []

    # Score all facilities
    scored = [(fac, score_facility_completeness(fac)) for fac in facilities]

    # Sort by score (descending)
    scored.sort(key=lambda x: x[1], reverse=True)

    # Return best and the rest
    best = scored[0][0]
    duplicates = [fac for fac, _ in scored[1:]]

    return best, duplicates
