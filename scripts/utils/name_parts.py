"""Unicode-aware name normalization and slug generation utilities."""

import re
import unicodedata

try:
    from unidecode import unidecode
except Exception:
    unidecode = None


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