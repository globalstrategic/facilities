"""Global slug registry for ensuring unique canonical slugs."""

from collections import defaultdict
from typing import Optional


def _slugify_suffix(s: str) -> str:
    """Helper to slugify disambiguation suffixes."""
    import re
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s


class SlugRegistry:
    """Registry to ensure globally unique facility slugs."""

    def __init__(self):
        self.seen = defaultdict(int)  # slug -> count

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