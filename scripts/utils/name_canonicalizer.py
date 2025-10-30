"""
Facility Name Canonicalization Utility

Generates canonical facility names following the pattern:
    {Town} {Operator} {Core} {Type}

Examples:
    - "Roxby Downs BHP Olympic Dam Mine"
    - "Morenci Freeport-McMoRan Morenci Mine"
    - "Karee Sibanye-Stillwater Karee Mine"

Missing components are omitted (e.g., no town → "{Operator} {Core} {Type}").

This module integrates with:
- EntityIdentity library for company name resolution
- CompanyResolver for canonical operator names
- Facility schema v2.1.0 with canonical_name field

Author: GSMC Facilities Team
Version: 1.0.0
Date: 2025-10-30
"""

import re
import json
from typing import Dict, List, Optional, Tuple
from pathlib import Path

# Try to import EntityIdentity - graceful fallback if not available
try:
    import sys
    sys.path.insert(0, str(Path(__file__).parents[3] / 'entityidentity'))
    from entityidentity import get_company_by_id
    ENTITYIDENTITY_AVAILABLE = True
except ImportError:
    ENTITYIDENTITY_AVAILABLE = False
    print("Warning: EntityIdentity library not available. Canonical names will not include resolved company names.")


class FacilityNameCanonicalizer:
    """
    Generates canonical facility names for standardization and EntityIdentity resolution.

    Canonical Name Pattern: {Town} {Operator} {Core} {Type}

    Usage:
        canonicalizer = FacilityNameCanonicalizer()
        result = canonicalizer.canonicalize(
            name="Phelps Dodge Safford Project",
            types=["mine"],
            operator_company_id="cmp-LEI_XXX",
            town="Safford"
        )
        # Returns:
        {
            'canonical_name': 'Safford Freeport-McMoRan Safford Mine',
            'display_name': 'Safford',
            'core_name': 'Safford'
        }
    """

    # Common noise words to remove from facility names
    NOISE_WORDS = {
        'project', 'operation', 'operations', 'complex', 'property',
        'facility', 'facilities', 'area', 'district', 'group',
        'the', 'new', 'old', 'east', 'west', 'north', 'south',
        'upper', 'lower', 'main', 'central'
    }

    # Common descriptors that indicate facility type (remove these)
    TYPE_DESCRIPTORS = {
        'mine', 'mines', 'mining',
        'smelter', 'refinery', 'concentrator',
        'plant', 'mill', 'heap leach', 'tailings',
        'sx ew', 'sx/ew', 'leach pad',
        'underground', 'open pit', 'opencast', 'pit'
    }

    # Parenthetical content patterns (usually location hints or clarifications)
    PARENTHETICAL_PATTERN = re.compile(r'\([^)]*\)')

    # Common company name patterns to remove
    COMPANY_SUFFIXES = {
        'inc', 'inc.', 'ltd', 'ltd.', 'llc', 'plc', 'corp', 'corporation',
        'limited', 'company', 'co', 'co.', 'group', 'mining',
        'resources', 'minerals', 'metals'
    }

    def __init__(self):
        """Initialize the canonicalizer."""
        # Load company list from EntityIdentity for name removal
        self.known_companies = self._load_known_companies()

    def _load_known_companies(self) -> List[str]:
        """
        Load list of known company names from EntityIdentity for removal from facility names.

        Returns:
            List of company names (lowercase) to strip from facility names
        """
        if not ENTITYIDENTITY_AVAILABLE:
            # Fallback: common mining company names
            return [
                'bhp', 'bhp billiton', 'rio tinto', 'vale', 'glencore',
                'freeport-mcmoran', 'freeport mcmoran', 'phelps dodge',
                'anglo american', 'angloamerican', 'barrick', 'newmont',
                'southern copper', 'codelco', 'antofagasta', 'first quantum',
                'ivanhoe', 'teck', 'fortescue', 'fmg', 'arcelormittal',
                'sibanye', 'sibanye-stillwater', 'implats', 'amplats',
                'harmony', 'goldfields', 'anglogold ashanti', 'kinross'
            ]

        try:
            # Load from EntityIdentity companies parquet
            import pandas as pd
            ei_path = Path(__file__).parents[3] / 'entityidentity' / 'tables' / 'companies.parquet'
            if ei_path.exists():
                df = pd.read_parquet(ei_path)
                # Get all company names and aliases
                names = df['company_name'].str.lower().tolist() if 'company_name' in df.columns else []
                if 'aliases' in df.columns:
                    for aliases in df['aliases']:
                        if isinstance(aliases, list):
                            names.extend([a.lower() for a in aliases])
                return names
        except Exception as e:
            print(f"Warning: Could not load companies from EntityIdentity: {e}")

        return []

    def extract_core_name(
        self,
        name: str,
        aliases: Optional[List[str]] = None,
        remove_companies: bool = True
    ) -> str:
        """
        Extract the core facility name by removing noise words, company names, and descriptors.

        Args:
            name: Original facility name
            aliases: Optional list of aliases to consider
            remove_companies: Whether to remove known company names

        Returns:
            Clean core name

        Examples:
            >>> canonicalizer.extract_core_name("Phelps Dodge Safford Project")
            'Safford'
            >>> canonicalizer.extract_core_name("Olympic Dam (Roxby Downs) Smelter")
            'Olympic Dam'
            >>> canonicalizer.extract_core_name("Morenci SX EW Mine")
            'Morenci'
        """
        # Start with original name
        core = name.strip()

        # Remove parenthetical content (often location hints)
        core = self.PARENTHETICAL_PATTERN.sub('', core)

        # Convert to lowercase for comparison
        core_lower = core.lower()

        # Remove known company names
        if remove_companies and self.known_companies:
            for company in sorted(self.known_companies, key=len, reverse=True):
                # Use word boundaries to avoid partial matches
                pattern = r'\b' + re.escape(company) + r'\b'
                core_lower = re.sub(pattern, '', core_lower, flags=re.IGNORECASE)

        # Remove type descriptors
        for descriptor in self.TYPE_DESCRIPTORS:
            pattern = r'\b' + re.escape(descriptor) + r'\b'
            core_lower = re.sub(pattern, '', core_lower, flags=re.IGNORECASE)

        # Remove noise words
        words = core_lower.split()
        words = [w for w in words if w not in self.NOISE_WORDS]

        # Remove company suffixes
        words = [w for w in words if w.rstrip('.') not in self.COMPANY_SUFFIXES]

        # Rejoin and clean up
        core = ' '.join(words)
        core = re.sub(r'\s+', ' ', core)  # Collapse multiple spaces
        core = core.strip(' -,')

        # Capitalize properly
        core = self._capitalize_name(core)

        return core if core else name  # Fallback to original if empty

    def _capitalize_name(self, name: str) -> str:
        """
        Capitalize name properly (title case, but preserve acronyms).

        Args:
            name: Name to capitalize

        Returns:
            Properly capitalized name

        Examples:
            >>> canonicalizer._capitalize_name("olympic dam")
            'Olympic Dam'
            >>> canonicalizer._capitalize_name("sx ew")
            'SX EW'
        """
        # Special cases for acronyms
        acronyms = {'sx', 'ew', 'pgm', 'pge', 'ree', 'usa', 'uk'}

        words = []
        for word in name.split():
            if word.lower() in acronyms:
                words.append(word.upper())
            else:
                words.append(word.capitalize())

        return ' '.join(words)

    def get_operator_name(self, operator_company_id: Optional[str]) -> Optional[str]:
        """
        Get canonical operator name from company ID using EntityIdentity.

        Args:
            operator_company_id: Company ID in format cmp-LEI_XXX or cmp-{slug}

        Returns:
            Canonical company name, or None if not found

        Examples:
            >>> canonicalizer.get_operator_name("cmp-LEI_9695006QKQ4E65FB2U33")
            'Anglo American Platinum'
        """
        if not operator_company_id or not ENTITYIDENTITY_AVAILABLE:
            return None

        try:
            company = get_company_by_id(operator_company_id)
            if company and 'company_name' in company:
                return company['company_name']
        except Exception as e:
            print(f"Warning: Could not resolve operator {operator_company_id}: {e}")

        return None

    def get_primary_type(self, types: List[str]) -> str:
        """
        Get primary facility type from types array.

        Args:
            types: List of facility types

        Returns:
            Primary type (first in list), capitalized

        Examples:
            >>> canonicalizer.get_primary_type(["mine", "concentrator"])
            'Mine'
            >>> canonicalizer.get_primary_type(["smelter"])
            'Smelter'
        """
        if not types:
            return "Facility"  # Default fallback

        primary = types[0]

        # Special handling for compound types
        if primary == "heap_leach":
            return "Heap Leach"

        return primary.capitalize()

    def canonicalize(
        self,
        name: str,
        types: List[str],
        operator_company_id: Optional[str] = None,
        town: Optional[str] = None,
        aliases: Optional[List[str]] = None
    ) -> Dict[str, str]:
        """
        Generate canonical facility name and related names.

        Canonical Name Pattern: {Town} {Operator} {Core} {Type}
        - Components are omitted if not available
        - Minimum viable canonical: "{Core} {Type}"

        Args:
            name: Original facility name
            types: List of facility types (uses first as primary)
            operator_company_id: Optional operator company ID
            town: Optional town/city name
            aliases: Optional list of aliases

        Returns:
            Dictionary with:
                - canonical_name: Full canonical name
                - display_name: Short form (core name only)
                - core_name: Extracted core name
                - components: Dict with individual components

        Examples:
            >>> canonicalizer.canonicalize(
            ...     name="Olympic Dam",
            ...     types=["mine", "smelter"],
            ...     operator_company_id="cmp-bhp",
            ...     town="Roxby Downs"
            ... )
            {
                'canonical_name': 'Roxby Downs BHP Olympic Dam Mine',
                'display_name': 'Olympic Dam',
                'core_name': 'Olympic Dam',
                'components': {
                    'town': 'Roxby Downs',
                    'operator': 'BHP',
                    'core': 'Olympic Dam',
                    'type': 'Mine'
                }
            }
        """
        # Extract core name
        core_name = self.extract_core_name(name, aliases=aliases)

        # Get operator name
        operator_name = None
        if operator_company_id:
            operator_name = self.get_operator_name(operator_company_id)

        # Get primary type
        primary_type = self.get_primary_type(types)

        # Build canonical name components
        components = []

        if town and town != "TODO":
            components.append(town)

        if operator_name:
            components.append(operator_name)

        components.append(core_name)
        components.append(primary_type)

        # Join to create canonical name
        canonical_name = ' '.join(components)

        # Display name is just the core
        display_name = core_name

        return {
            'canonical_name': canonical_name,
            'display_name': display_name,
            'core_name': core_name,
            'components': {
                'town': town if town and town != "TODO" else None,
                'operator': operator_name,
                'core': core_name,
                'type': primary_type
            }
        }

    def canonicalize_facility(self, facility: Dict) -> Dict[str, str]:
        """
        Convenience method to canonicalize a facility dict directly.

        Args:
            facility: Facility dict with fields: name, types, operator_link, location

        Returns:
            Canonical names dict (same as canonicalize())

        Example:
            >>> facility = {
            ...     'name': 'Safford Mine',
            ...     'types': ['mine'],
            ...     'operator_link': {'company_id': 'cmp-freeport'},
            ...     'location': {'town': 'Safford'}
            ... }
            >>> canonicalizer.canonicalize_facility(facility)
        """
        operator_company_id = None
        if facility.get('operator_link'):
            operator_company_id = facility['operator_link'].get('company_id')

        town = None
        if facility.get('location'):
            town = facility['location'].get('town')

        return self.canonicalize(
            name=facility['name'],
            types=facility['types'],
            operator_company_id=operator_company_id,
            town=town,
            aliases=facility.get('aliases', [])
        )


# Convenience function for quick usage
def canonicalize_facility_name(
    name: str,
    types: List[str],
    operator_company_id: Optional[str] = None,
    town: Optional[str] = None,
    aliases: Optional[List[str]] = None
) -> str:
    """
    Quick function to generate canonical name.

    Args:
        name: Facility name
        types: List of facility types
        operator_company_id: Optional operator company ID
        town: Optional town name
        aliases: Optional aliases list

    Returns:
        Canonical facility name string

    Example:
        >>> canonicalize_facility_name("Morenci Mine", ["mine"], town="Morenci")
        'Morenci Morenci Mine'
    """
    canonicalizer = FacilityNameCanonicalizer()
    result = canonicalizer.canonicalize(name, types, operator_company_id, town, aliases)
    return result['canonical_name']


if __name__ == "__main__":
    # Test cases
    canonicalizer = FacilityNameCanonicalizer()

    print("Testing Facility Name Canonicalizer\n")
    print("=" * 60)

    # Test 1: Full canonical with all components
    print("\nTest 1: Full canonical name (all components)")
    result = canonicalizer.canonicalize(
        name="BHP Billiton Olympic Dam Smelter",
        types=["mine", "smelter", "refinery"],
        operator_company_id="cmp-bhp",
        town="Roxby Downs"
    )
    print(f"  Input: 'BHP Billiton Olympic Dam Smelter'")
    print(f"  Canonical: '{result['canonical_name']}'")
    print(f"  Display: '{result['display_name']}'")
    print(f"  Core: '{result['core_name']}'")

    # Test 2: No town
    print("\nTest 2: No town (operator + core + type)")
    result = canonicalizer.canonicalize(
        name="Phelps Dodge Safford Project",
        types=["mine"],
        operator_company_id="cmp-freeport"
    )
    print(f"  Input: 'Phelps Dodge Safford Project'")
    print(f"  Canonical: '{result['canonical_name']}'")

    # Test 3: No operator
    print("\nTest 3: No operator (town + core + type)")
    result = canonicalizer.canonicalize(
        name="Two Rivers Mine",
        types=["mine"],
        town="Rustenburg"
    )
    print(f"  Input: 'Two Rivers Mine'")
    print(f"  Canonical: '{result['canonical_name']}'")

    # Test 4: Minimal (core + type only)
    print("\nTest 4: Minimal (core + type only)")
    result = canonicalizer.canonicalize(
        name="Morenci SX EW Operations",
        types=["mine"]
    )
    print(f"  Input: 'Morenci SX EW Operations'")
    print(f"  Canonical: '{result['canonical_name']}'")

    # Test 5: Complex name with parenthetical
    print("\nTest 5: Complex name with parenthetical")
    result = canonicalizer.canonicalize(
        name="Olympic Dam (Roxby Downs) Refinery Complex",
        types=["refinery"],
        town="Roxby Downs"
    )
    print(f"  Input: 'Olympic Dam (Roxby Downs) Refinery Complex'")
    print(f"  Canonical: '{result['canonical_name']}'")

    print("\n" + "=" * 60)
    print("Tests complete!")
