"""
Facility Name Canonicalization Utility - Production Grade v2

Includes:
- Unicode-aware name normalization and slug generation
- Global slug registry for uniqueness
- Canonical name generation
"""

from __future__ import annotations
import re
import unicodedata
from collections import defaultdict
from typing import Dict, Any, Optional, List

from scripts.utils.type_map import normalize_type

# Try to import unidecode for transliteration
try:
    from unidecode import unidecode
except ImportError:
    unidecode = None

# Try to import pygeohash for geohash computation
try:
    import pygeohash
except ImportError:
    pygeohash = None


# =============================================================================
# Unicode Utilities (formerly name_parts.py)
# =============================================================================

def nfc(s: str) -> str:
    """Normalize string to NFC (canonical composition) form."""
    return unicodedata.normalize("NFC", s or "")


def to_ascii(s: str) -> str:
    """Convert Unicode string to ASCII equivalent."""
    s = nfc(s)
    if unidecode:
        s = unidecode(s)
    else:
        # Fallback: decompose and strip accents
        s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return s


def slugify(*parts: str) -> str:
    """Create URL-safe slug from parts, handling Unicode properly."""
    txt = " ".join([p for p in map(nfc, parts) if p])
    base = to_ascii(txt).lower()
    base = re.sub(r"[^a-z0-9]+", "-", base).strip("-")
    base = re.sub(r"-{2,}", "-", base)
    return base or "facility"


def equal_ignoring_accents(a: str, b: str) -> bool:
    """Check if two strings are equal ignoring accents and case."""
    if not a or not b:
        return False
    return to_ascii(a).lower().strip() == to_ascii(b).lower().strip()


# =============================================================================
# Slug Registry (formerly slug_registry.py)
# =============================================================================

def _slugify_suffix(s: str) -> str:
    """Helper to slugify disambiguation suffixes."""
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s


class SlugRegistry:
    """Registry to ensure globally unique facility slugs."""

    def __init__(self, preseed=()):
        """
        Initialize registry with optional pre-seeding.

        Args:
            preseed: Iterable of existing slugs to register upfront
        """
        self.seen = defaultdict(int)  # slug -> count
        for s in preseed:
            if s:
                self.seen[s] = 1

    def unique(self, slug: str, *, country: Optional[str] = None,
               region: Optional[str] = None, geohash6: Optional[str] = None) -> str:
        """
        Get unique slug, adding deterministic disambiguator if needed.

        Args:
            slug: Base slug
            country: Country code for disambiguation
            region: Region name for disambiguation
            geohash6: Geohash prefix for disambiguation

        Returns:
            Unique slug, potentially with suffix
        """
        if slug not in self.seen:
            self.seen[slug] = 1
            return slug

        # Deterministic disambiguation: region -> geohash6 -> numeric suffix
        for suffix in filter(None, [region, geohash6]):
            s = f"{slug}-{_slugify_suffix(suffix)}"
            if s not in self.seen:
                self.seen[s] = 1
                return s

        # Last resort: numeric suffix
        i = self.seen[slug] + 1
        self.seen[slug] = i
        unique_slug = f"{slug}-{i}"
        self.seen[unique_slug] = 1
        return unique_slug

    def load_existing(self, slugs: list[str]):
        """Pre-load registry with existing slugs to avoid collisions."""
        for slug in slugs:
            if slug:
                self.seen[slug] = 1


# =============================================================================
# Name Canonicalization
# =============================================================================

# Single registry kept per run
SLUGS = SlugRegistry()

# Noise words to remove from facility names
NOISE_WORDS = {
    "project", "operation", "operations", "complex", "property", "deposit", "district",
    "mine", "pit", "shaft", "open pit", "open-pit", "underground", "plant", "mill",
    "smelter", "refinery", "concentrator", "sx-ew", "sxew", "hydromet", "works"
}

# Town selection preference order (deterministic)
TOWN_PREF_ORDER = ("town", "city", "municipality", "village", "hamlet")


def compute_geohash6(lat: Optional[float], lon: Optional[float]) -> Optional[str]:
    """Compute geohash with 6-char precision (~1.2km)."""
    if lat is None or lon is None:
        return None
    if pygeohash:
        try:
            return pygeohash.encode(lat, lon)[:6]
        except:
            return None
    return None


def humanize_type(type_str: str) -> str:
    """Convert snake_case type to Title Case."""
    if not type_str or type_str == "facility":
        return ""
    return type_str.replace("_", " ").title()


def extract_core_name(name: str, operator: Optional[str] = None) -> str:
    """Extract core facility name by removing company names and noise."""
    if not name:
        return ""

    s = nfc(name)

    # Remove parentheticals (often contain towns)
    s = re.sub(r"\([^)]*\)", " ", s)

    # Remove operator name if present
    if operator:
        operator_norm = re.escape(operator)
        s = re.sub(rf"\b{operator_norm}\b", " ", s, flags=re.IGNORECASE)

    # Remove noise words
    for word in sorted(NOISE_WORDS, key=len, reverse=True):
        s = re.sub(rf"\b{re.escape(word)}\b", " ", s, flags=re.IGNORECASE)

    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()

    return s or name.strip()


def choose_town_from_address(addr: Dict[str, Any]) -> Optional[str]:
    """Choose town from address dict using deterministic preference order."""
    for key in TOWN_PREF_ORDER:
        v = addr.get(key)
        if v:
            return nfc(str(v).strip())
    return None


class FacilityNameCanonicalizer:
    """Generates canonical facility names for standardization."""

    def canonicalize_facility(
        self,
        fac: Dict[str, Any],
        existing_slugs: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Canonicalize a facility dict.

        Args:
            fac: Facility dict
            existing_slugs: Ignored (we use global SLUGS registry)

        Returns:
            Dict with canonical fields
        """
        # Gather inputs
        name = nfc(fac.get("name", ""))
        loc = fac.get("location", {}) or {}
        town = nfc(loc.get("town") or "")
        region = nfc(loc.get("region") or "")
        lat, lon = loc.get("lat"), loc.get("lon")

        # Get primary type - prefer already-set primary_type, then types[0]
        if fac.get("primary_type"):
            primary_type = fac["primary_type"]
            type_conf = fac.get("type_confidence", 0.9)
        else:
            raw_type = (fac.get("types") or [None])[0]
            primary_type, type_conf = normalize_type(raw_type)

        # Get operator
        operator = nfc(fac.get("operator_display") or "")

        # Extract core name
        core = extract_core_name(name, operator)
        core = nfc(core)

        # Dedupe: drop town/operator if they equal core
        if town and equal_ignoring_accents(town, core):
            town = ""
        if operator and equal_ignoring_accents(operator, core):
            operator = ""

        # Build canonical name (with operator if present)
        parts = [p for p in [town, operator, core, humanize_type(primary_type)] if p]
        canonical_name = " ".join(parts)

        # Build slug (NO operator for stability)
        base_slug = slugify(town, core, primary_type)
        geohash6 = compute_geohash6(lat, lon)
        canonical_slug = SLUGS.unique(
            base_slug,
            country=fac.get("country_iso3"),
            region=region or None,
            geohash6=geohash6
        )

        # Display name = Core (unless overridden)
        if fac.get("display_name_override") and fac.get("display_name"):
            display_name = fac["display_name"]
        else:
            display_name = core

        # Calculate confidence scores
        town_score = 1.0 if town else 0.0
        core_score = 0.9 if core and core != name else 0.6
        operator_score = 1.0 if operator else 0.0

        # Overall confidence
        conf = (
            0.15 * town_score +
            0.35 * core_score +
            0.30 * type_conf +
            0.20 * operator_score
        )
        conf = max(0.0, min(1.0, conf))

        # Detail scores
        detail = {
            "town": town_score,
            "core": core_score,
            "type": type_conf,
            "operator": operator_score,
            "parts": sum(1 for x in [town, core, primary_type] if x) / 3.0
        }

        # Components for debugging
        components = {
            "town": town or None,
            "operator_display": operator or None,
            "core": core,
            "primary_type": primary_type
        }

        return {
            "canonical_name": canonical_name or None,
            "display_name": display_name or None,
            "canonical_slug": canonical_slug,
            "primary_type": primary_type,
            "type_confidence": type_conf,
            "canonicalization_confidence": conf,
            "canonicalization_detail": detail,
            "canonical_components": components
        }


# Convenience function
def canonicalize_facility_name(
    name: str,
    types: List[str],
    operator_display: Optional[str] = None,
    town: Optional[str] = None,
    existing_slugs: Optional[Dict[str, str]] = None
) -> Dict[str, str]:
    """Quick function to generate canonical name and slug."""
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
        "canonical_slug": result["canonical_slug"],
        "primary_type": result["primary_type"],
        "type_confidence": result["type_confidence"]
    }
