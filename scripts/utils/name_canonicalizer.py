"""
Facility Name Canonicalization Utility - Production Grade

Generates canonical facility names and slugs following the pattern:
    canonical_name: {Town} {Operator} {Core} {Type}
    canonical_slug: {town}-{core}-{type} (EXCLUDES operator for stability)

Examples:
    - canonical_name: "Roxby Downs BHP Olympic Dam Mine"
    - canonical_slug: "roxby-downs-olympic-dam-mine"
    - display_name: "Olympic Dam"

This module provides production-grade canonicalization with:
- Unicode-aware name processing (NFC normalization)
- ASCII transliteration for slugs (via unidecode if available)
- Type cleaner with confidence scoring
- Deterministic town selection (town > city > municipality > village > hamlet)
- Collision resolver for duplicate slugs
- Confidence scoring for audit trail

Author: GSMC Facilities Team
Version: 2.0.0 (Production)
Date: 2025-10-30
"""

from __future__ import annotations
import re
import unicodedata
import hashlib
from dataclasses import dataclass
from typing import Dict, Any, Optional, Tuple, List

# Try to import unidecode for better ASCII transliteration
try:
    from unidecode import unidecode
except ImportError:
    unidecode = None

# Noise words to remove from facility names
NOISE_WORDS = {
    "project", "operation", "operations", "complex", "property", "deposit", "district",
    "mine", "pit", "shaft", "open pit", "open-pit", "underground", "plant", "mill",
    "smelter", "refinery", "concentrator", "sx-ew", "sxew", "hydromet", "works"
}

# Type mapping: messy strings → validated enum values
TYPE_MAP = {
    # Hydromet variations
    "sx-ew": "hydromet_plant",
    "sxew": "hydromet_plant",
    "sx ew": "hydromet_plant",
    "solvent extraction": "hydromet_plant",

    # Rolling mill variations
    "rod mill": "rolling_mill",
    "wire mill": "rolling_mill",

    # Steel plant variations
    "steelworks": "steel_plant",
    "steel works": "steel_plant",
    "steel mill": "steel_plant",

    # Battery recycling variations
    "battery recycle": "battery_recycling",
    "battery recycling": "battery_recycling",
    "recycling plant": "battery_recycling",

    # Processing plant variations
    "processing plant": "processing_plant",
    "processing": "processing_plant",

    # Pass-through for known valid enum values
    "mine": "mine",
    "smelter": "smelter",
    "refinery": "refinery",
    "concentrator": "concentrator",
    "plant": "plant",
    "mill": "mill",
    "heap_leach": "heap_leach",
    "tailings": "tailings",
    "exploration": "exploration",
    "development": "development",
    "hydromet_plant": "hydromet_plant",
    "rolling_mill": "rolling_mill",
    "steel_plant": "steel_plant",
    "battery_recycling": "battery_recycling",
    "processing_plant": "processing_plant",
}

# Town selection preference order (deterministic)
TOWN_PREF_ORDER = ("town", "city", "municipality", "village", "hamlet")


@dataclass
class CanonicalComponents:
    """Components of a canonical facility name."""
    town: Optional[str]
    operator_display: Optional[str]
    core: str
    primary_type: Optional[str]


def _normalize_unicode(s: str) -> str:
    """Normalize string to NFC form (canonical composition)."""
    return unicodedata.normalize("NFC", s)


def _strip_company_tokens(text: str, operators: List[str]) -> str:
    """Remove operator/company names from text."""
    tokens = [re.escape(o) for o in operators if o]
    if not tokens:
        return text
    pat = re.compile(r"\b(?:%s)\b" % "|".join(tokens), flags=re.IGNORECASE)
    return pat.sub(" ", text)


def _strip_noise(text: str) -> str:
    """Remove noise words and clean up whitespace."""
    t = text

    # Remove parentheticals (often contain towns, handled separately)
    t = re.sub(r"\([^)]*\)", " ", t)

    # Remove common noise words
    for w in sorted(NOISE_WORDS, key=len, reverse=True):
        t = re.sub(rf"\b{re.escape(w)}\b", " ", t, flags=re.IGNORECASE)

    # Collapse whitespace and clean punctuation
    t = re.sub(r"[_\s\-]+", " ", t).strip()
    return t


def _letters_digits_spaces(s: str) -> str:
    """Keep only letters, digits, and safe punctuation."""
    return "".join(ch if (ch.isalnum() or ch in " -'/,&()") else " " for ch in s)


def _title_case_preserve_acronyms(s: str) -> str:
    """Title case but preserve acronyms (e.g., 'SX EW' stays 'SX EW')."""
    parts = s.split()
    out = []
    for p in parts:
        if len(p) <= 3 and p.isupper():
            out.append(p)  # Keep acronym as-is
        else:
            out.append(p.capitalize())
    return " ".join(out)


def _slugify_ascii(*parts: str) -> str:
    """Convert parts to ASCII slug (lowercase, hyphenated)."""
    raw = " ".join([p for p in parts if p])
    raw = _normalize_unicode(raw)

    # ASCII transliteration
    if unidecode:
        ascii_text = unidecode(raw)
    else:
        ascii_text = raw.encode("ascii", "ignore").decode("ascii")

    # Convert to lowercase slug
    ascii_text = ascii_text.lower()
    ascii_text = re.sub(r"[^a-z0-9]+", "-", ascii_text).strip("-")
    ascii_text = re.sub(r"-{2,}", "-", ascii_text)
    return ascii_text


def choose_town_from_address(addr: Dict[str, Any]) -> Optional[str]:
    """
    Choose town from address dict using deterministic preference order.

    Preference: town > city > municipality > village > hamlet

    Args:
        addr: Address dict from reverse geocoding (Nominatim format)

    Returns:
        Town name or None
    """
    for key in TOWN_PREF_ORDER:
        v = addr.get(key)
        if v:
            return _title_case_preserve_acronyms(_letters_digits_spaces(_normalize_unicode(v)))
    return None


def clean_primary_type(raw_type: Optional[str]) -> Tuple[Optional[str], float]:
    """
    Clean and validate facility type.

    Args:
        raw_type: Raw type string (may be messy)

    Returns:
        (cleaned_type, confidence) tuple
    """
    if not raw_type:
        return None, 0.0

    t = raw_type.strip().lower()

    # Try exact match
    if t in TYPE_MAP:
        return TYPE_MAP[t], 0.95

    # Try with space/hyphen normalization
    t_normalized = t.replace(" ", "").replace("-", "")
    for key in TYPE_MAP:
        if key.replace(" ", "").replace("-", "") == t_normalized:
            return TYPE_MAP[key], 0.95

    # Last-chance heuristic: partial match
    for k, v in TYPE_MAP.items():
        if k in raw_type.lower():
            return v, 0.7

    return None, 0.0


def extract_core_name(name: str, operator_display: Optional[str]) -> Tuple[str, float]:
    """
    Extract core facility name by removing company names and noise.

    Args:
        name: Original facility name
        operator_display: Operator name to remove

    Returns:
        (core_name, confidence) tuple
    """
    s = _normalize_unicode(name or "")
    s = _letters_digits_spaces(s)
    s = _strip_company_tokens(s, [operator_display] if operator_display else [])
    s = _strip_noise(s)

    # Remove type descriptors one more time
    s = re.sub(r"\b(?:mine|smelter|refinery|concentrator|plant|mill)\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\s{2,}", " ", s).strip()

    # Confidence scoring
    if s and s.lower() != (name or "").lower():
        conf = 0.9  # Successfully cleaned
    elif s:
        conf = 0.6  # Minimal cleaning
    else:
        conf = 0.2  # Failed to extract
        s = (name or "").strip()  # Fallback to original

    return _title_case_preserve_acronyms(s) or (name or "").strip(), conf


def build_canonical_components(f: Dict[str, Any]) -> Tuple[CanonicalComponents, float, Dict[str, float]]:
    """
    Build canonical name components from facility dict.

    Args:
        f: Facility dict

    Returns:
        (components, overall_confidence, detail_scores) tuple
    """
    # Town
    town = (f.get("location") or {}).get("town")

    # Operator display (derived or provided)
    operator_display = f.get("operator_display")
    if not operator_display and f.get("operators"):
        # Derive from operators array
        current = [o for o in f["operators"] if not o.get("end_date")]
        current = current or f["operators"]  # Fallback to all if no current

        # Sort by share (descending) then company_id
        names = []
        for o in sorted(current, key=lambda x: (-(x.get("share") or 0), str(x.get("company_id")))):
            # TODO: Look up company_id → canonical name from EntityIdentity
            names.append(str(o.get("company_id")))

        operator_display = "-".join(names) + (" JV" if len(names) > 1 else "") if names else None

    # Primary type
    ptype = f.get("primary_type")
    t_conf = 1.0 if ptype else 0.0

    if not ptype:
        # Try to clean types[0]
        raw0 = None
        if isinstance(f.get("types"), list) and f["types"]:
            raw0 = str(f["types"][0])
        ptype, t_conf = clean_primary_type(raw0)

    # Core name
    core, core_conf = extract_core_name(f.get("name", ""), operator_display)

    # Assemble components
    comps = CanonicalComponents(
        town=town,
        operator_display=operator_display,
        core=core,
        primary_type=ptype
    )

    # Overall confidence scoring
    # Weights: town=15%, core=35%, type=30%, operator=20%
    conf = (
        0.15 * (1 if town else 0) +
        0.35 * core_conf +
        0.30 * t_conf +
        0.20 * (1 if operator_display else 0)
    )
    conf = max(0.0, min(1.0, conf))

    # Detail scores for audit
    parts_present = sum(1 for x in [town, core, ptype] if x)
    detail = {
        "town": 1.0 if town else 0.0,
        "core": core_conf,
        "type": t_conf,
        "operator": 1.0 if operator_display else 0.0,
        "parts": parts_present / 3.0  # Exclude operator from completeness
    }

    return comps, conf, detail


def generate_canonical_name(comps: CanonicalComponents) -> str:
    """
    Generate canonical name from components.

    Pattern: {Town} {Operator} {Core} {Type}

    Args:
        comps: Canonical components

    Returns:
        Canonical name string
    """
    parts = []
    if comps.town:
        parts.append(comps.town)
    if comps.operator_display:
        parts.append(comps.operator_display)
    if comps.core:
        parts.append(comps.core)
    if comps.primary_type:
        # Human-friendly type suffix (Title Case, replace underscores)
        t = comps.primary_type.replace("_", " ")
        parts.append(_title_case_preserve_acronyms(t))

    name = " ".join(parts)
    name = re.sub(r"\s{2,}", " ", name).strip()
    return name


def generate_display_name(
    comps: CanonicalComponents,
    display_name_override: Optional[bool],
    existing_display: Optional[str]
) -> str:
    """
    Generate display name (short form for UI).

    Args:
        comps: Canonical components
        display_name_override: If True, use existing_display
        existing_display: Existing display name (if overridden)

    Returns:
        Display name string
    """
    if display_name_override and existing_display:
        return existing_display
    return comps.core  # Agreed rule: display_name = core


def generate_canonical_slug(
    comps: CanonicalComponents,
    country_iso3: Optional[str] = None,
    region: Optional[str] = None,
    geohash: Optional[str] = None
) -> str:
    """
    Generate canonical slug (machine-safe identifier).

    Pattern: {town}-{core}-{type} (EXCLUDES operator for stability)

    Args:
        comps: Canonical components
        country_iso3: Country code (optional, for namespacing)
        region: Region name (optional, for collision resolution)
        geohash: Geohash (optional, for collision resolution)

    Returns:
        Canonical slug string
    """
    # EXCLUDES OPERATOR - stable through operator changes
    base = _slugify_ascii(*(x for x in [comps.town, comps.core, comps.primary_type] if x))

    # Keep it deterministic and non-empty
    if not base:
        base = _slugify_ascii(comps.core) or "facility"

    # Note: Collision resolution happens in resolve_collision()
    return base


def resolve_collision(
    existing_slugs: Dict[str, str],
    slug: str,
    f: Dict[str, Any]
) -> str:
    """
    Resolve slug collision by adding disambiguator.

    If slug exists and points to a different facility_id, append:
    1. Region (if available)
    2. Geohash prefix (if available)
    3. Hash of coordinates (last resort)

    Args:
        existing_slugs: Dict mapping slug → facility_id
        slug: Proposed slug
        f: Facility dict

    Returns:
        Unique slug (potentially with disambiguator)
    """
    if slug not in existing_slugs or existing_slugs.get(slug) == f.get("facility_id"):
        return slug  # No collision

    # Collision detected - add disambiguator
    loc = f.get("location") or {}
    tokens = [slug]

    # Try region
    if loc.get("region"):
        tokens.append(_slugify_ascii(loc["region"]))
    # Try geohash prefix
    elif loc.get("geohash"):
        tokens.append(loc["geohash"][:6])
    else:
        # Last resort: short hash of coordinates
        lat, lon = loc.get("lat"), loc.get("lon")
        if lat and lon:
            h = hashlib.sha1(f"{lat},{lon}".encode("utf-8")).hexdigest()[:6]
            tokens.append(h)
        else:
            # Ultimate fallback: country code
            tokens.append(f.get("country_iso3", "xxx").lower())

    return "-".join(tokens)


class FacilityNameCanonicalizer:
    """
    Generates canonical facility names for standardization and EntityIdentity resolution.

    Usage:
        canonicalizer = FacilityNameCanonicalizer()
        result = canonicalizer.canonicalize_facility(facility_dict, existing_slugs)
    """

    def canonicalize_facility(
        self,
        f: Dict[str, Any],
        existing_slugs: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Canonicalize a facility dict.

        Args:
            f: Facility dict with fields: name, types, operator_display, location, etc.
            existing_slugs: Dict mapping slug → facility_id for collision detection

        Returns:
            Dict with:
                - canonical_name: Full canonical name
                - display_name: Short form for UI
                - canonical_slug: Machine-safe identifier (excludes operator)
                - canonicalization_confidence: Overall confidence score (0-1)
                - canonicalization_detail: Per-component confidence scores
                - canonical_components: Component breakdown
        """
        # Build components
        comps, conf, detail = build_canonical_components(f)

        # Generate names
        canonical_name = generate_canonical_name(comps)
        display_name = generate_display_name(
            comps,
            f.get("display_name_override"),
            f.get("display_name")
        )

        # Generate slug (excludes operator)
        slug = generate_canonical_slug(
            comps,
            f.get("country_iso3"),
            (f.get("location") or {}).get("region"),
            (f.get("location") or {}).get("geohash")
        )

        # Resolve collisions if existing_slugs provided
        if existing_slugs is not None:
            slug = resolve_collision(existing_slugs, slug, f)

        return {
            "canonical_name": canonical_name or None,
            "display_name": display_name or None,
            "canonical_slug": slug,
            "canonicalization_confidence": conf,
            "canonicalization_detail": detail,
            "canonical_components": {
                "town": comps.town,
                "operator_display": comps.operator_display,
                "core": comps.core,
                "primary_type": comps.primary_type
            }
        }


# Convenience function for quick usage
def canonicalize_facility_name(
    name: str,
    types: List[str],
    operator_display: Optional[str] = None,
    town: Optional[str] = None,
    existing_slugs: Optional[Dict[str, str]] = None
) -> Dict[str, str]:
    """
    Quick function to generate canonical name and slug.

    Args:
        name: Facility name
        types: List of facility types
        operator_display: Optional operator display name
        town: Optional town name
        existing_slugs: Optional dict for collision detection

    Returns:
        Dict with canonical_name, display_name, canonical_slug
    """
    canonicalizer = FacilityNameCanonicalizer()
    facility = {
        "name": name,
        "types": types,
        "operator_display": operator_display,
        "location": {"town": town} if town else {}
    }
    result = canonicalizer.canonicalize_facility(facility, existing_slugs)
    return {
        "canonical_name": result["canonical_name"],
        "display_name": result["display_name"],
        "canonical_slug": result["canonical_slug"]
    }


if __name__ == "__main__":
    # Test cases
    canonicalizer = FacilityNameCanonicalizer()

    print("Testing Facility Name Canonicalizer (Production)\n")
    print("=" * 70)

    # Test 1: Full canonical with all components
    print("\nTest 1: Full canonical name (all components)")
    facility = {
        "name": "BHP Billiton Olympic Dam Smelter",
        "types": ["mine", "smelter"],
        "operator_display": "BHP",
        "location": {"town": "Roxby Downs"}
    }
    result = canonicalizer.canonicalize_facility(facility)
    print(f"  Input: '{facility['name']}'")
    print(f"  Canonical: '{result['canonical_name']}'")
    print(f"  Slug: '{result['canonical_slug']}'")
    print(f"  Display: '{result['display_name']}'")
    print(f"  Confidence: {result['canonicalization_confidence']:.2f}")

    # Test 2: Non-Latin characters
    print("\nTest 2: Non-Latin characters (Unicode)")
    facility = {
        "name": "São João Vale Plant",
        "types": ["plant"],
        "location": {"town": "São João"}
    }
    result = canonicalizer.canonicalize_facility(facility)
    print(f"  Input: '{facility['name']}'")
    print(f"  Canonical: '{result['canonical_name']}'")
    print(f"  Slug: '{result['canonical_slug']}'")
    print(f"  (Tests ASCII transliteration)")

    # Test 3: Operator stability
    print("\nTest 3: Operator stability (slug excludes operator)")
    facility1 = {
        "name": "Morenci Mine",
        "types": ["mine"],
        "operator_display": "Freeport-McMoRan",
        "location": {"town": "Morenci"}
    }
    facility2 = {
        "name": "Morenci Mine",
        "types": ["mine"],
        "operator_display": "BHP",  # Changed operator
        "location": {"town": "Morenci"}
    }
    result1 = canonicalizer.canonicalize_facility(facility1)
    result2 = canonicalizer.canonicalize_facility(facility2)
    print(f"  With Freeport: slug='{result1['canonical_slug']}'")
    print(f"  With BHP:      slug='{result2['canonical_slug']}'")
    print(f"  Same slug: {result1['canonical_slug'] == result2['canonical_slug']}")

    # Test 4: Type cleaning
    print("\nTest 4: Type cleaning (SX-EW → hydromet_plant)")
    facility = {
        "name": "Bagdad SX-EW Plant",
        "types": ["SX-EW"],
        "location": {"town": "Bagdad"}
    }
    result = canonicalizer.canonicalize_facility(facility)
    print(f"  Input type: 'SX-EW'")
    print(f"  Cleaned: '{result['canonical_components']['primary_type']}'")
    print(f"  Canonical: '{result['canonical_name']}'")

    # Test 5: Collision resolution
    print("\nTest 5: Collision resolution")
    existing_slugs = {"central-mine": "zaf-central-mine-fac-1"}
    facility = {
        "facility_id": "zaf-central-mine-fac-2",
        "name": "Central Mine",
        "types": ["mine"],
        "location": {"region": "Gauteng"},
        "country_iso3": "ZAF"
    }
    result = canonicalizer.canonicalize_facility(facility, existing_slugs)
    print(f"  Base slug: 'central-mine'")
    print(f"  Existing: 'central-mine' → zaf-central-mine-fac-1")
    print(f"  Resolved: '{result['canonical_slug']}'")
    print(f"  (Appended region disambiguator)")

    print("\n" + "=" * 70)
    print("Tests complete!")
