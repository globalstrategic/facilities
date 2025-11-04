#!/usr/bin/env python3
"""Fix slug collisions globally in one deterministic pass."""

import json
import pathlib
import sys

# Add parent to path for utils
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from scripts.utils.name_parts import slugify, to_ascii

def disambig(f):
    """Generate deterministic disambiguation suffix."""
    loc = f.get('location') or {}
    reg = loc.get('region') or ''
    town = loc.get('town') or ''
    gh = (loc.get('geohash') or '')[:6]

    if reg:
        return slugify(reg)
    if town:
        return slugify(town)
    if gh:
        return gh

    # deterministic fallback: last 4 of facility_id
    return to_ascii(f['facility_id'])[-4:].lower()

def main():
    root = pathlib.Path('facilities')
    items = []

    # Load all facilities
    for p in root.rglob('*.json'):
        try:
            d = json.loads(p.read_text(encoding='utf-8'))
            items.append((p, d))
        except Exception as e:
            print(f"Error loading {p}: {e}")
            continue

    # Group by slug
    by_slug = {}
    for p, d in items:
        s = d.get('canonical_slug')
        if not s:
            continue
        by_slug.setdefault(s, []).append((p, d))

    fixes = 0

    # Process collisions
    for s, rows in by_slug.items():
        if len(rows) <= 1:
            continue

        # stable order to make the result reproducible:
        rows.sort(key=lambda r: (r[1].get('country_iso3', ''), r[1]['facility_id']))

        # Keep the first, disambiguate the rest
        for idx, (p, d) in enumerate(rows[1:], start=2):
            suffix = disambig(d)
            new_slug = f"{s}-{suffix}"
            d['canonical_slug'] = new_slug

            # Update verification notes
            v = d.setdefault('verification', {})
            prev = v.get('notes') or ''
            tag = f"Slug collision resolved: {s} -> {new_slug}"
            v['notes'] = f"{prev} | {tag}" if prev else tag

            # Write back
            p.write_text(json.dumps(d, ensure_ascii=False, indent=2) + "\n", encoding='utf-8')

            fixes += 1
            print(f"Fixed: {d['facility_id']}: {s} -> {new_slug}")

    print(f"\nResolved {fixes} slug collisions.")

if __name__ == "__main__":
    main()
