#!/usr/bin/env python3
"""Export facilities to EntityIdentity parquet format."""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime
import sys

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

def load_all_facilities():
    """Load all facility JSON files."""
    facilities = []
    facilities_dir = Path("facilities")

    for country_dir in sorted(facilities_dir.iterdir()):
        if not country_dir.is_dir():
            continue
        for fac_file in sorted(country_dir.glob("*.json")):
            try:
                with open(fac_file, 'r', encoding='utf-8') as f:
                    facility = json.load(f)
                    facilities.append(facility)
            except Exception as e:
                print(f"Error loading {fac_file}: {e}")

    return facilities


def facility_to_row(facility):
    """Convert facility to EntityIdentity row."""
    loc = facility.get("location", {})
    data_quality = facility.get("data_quality", {})
    flags = data_quality.get("flags", {})

    # Get first commodity if available
    commodities = facility.get("commodities", [])
    primary_commodity = None
    if commodities:
        for c in commodities:
            if c.get("primary"):
                primary_commodity = c.get("metal")
                break
        if not primary_commodity and commodities:
            primary_commodity = commodities[0].get("metal")

    return {
        "facility_id": facility.get("facility_id"),
        "country_iso3": facility.get("country_iso3"),
        "canonical_name": facility.get("canonical_name"),
        "canonical_slug": facility.get("canonical_slug"),
        "display_name": facility.get("display_name"),
        "primary_type": facility.get("primary_type"),
        "type_confidence": facility.get("type_confidence"),
        "lat": loc.get("lat"),
        "lon": loc.get("lon"),
        "precision": loc.get("precision"),
        "town": loc.get("town"),
        "town_ascii": loc.get("town_ascii"),
        "region": loc.get("region"),
        "geohash": loc.get("geohash"),
        "operator_display": facility.get("operator_display"),
        "town_missing": flags.get("town_missing", False),
        "operator_unresolved": flags.get("operator_unresolved", False),
        "canonical_name_incomplete": flags.get("canonical_name_incomplete", False),
        "canonicalization_confidence": data_quality.get("canonicalization_confidence"),
        "status": facility.get("status"),
        "primary_commodity": primary_commodity,
        "aliases": "|".join(facility.get("aliases", [])),
        "company_mentions": "|".join(str(c) if isinstance(c, str) else c.get("name", "") for c in facility.get("company_mentions", [])),
    }


def main():
    """Export facilities to EntityIdentity format."""

    print("Loading facilities...")
    facilities = load_all_facilities()
    print(f"Loaded {len(facilities)} facilities")

    # Convert to rows
    rows = []
    for fac in facilities:
        rows.append(facility_to_row(fac))

    # Create DataFrame
    df = pd.DataFrame(rows)

    # Sort by country and facility_id
    df = df.sort_values(["country_iso3", "facility_id"])

    # Create output directory
    output_dir = Path("../entityidentity/entityidentity/facilities/data")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Write parquet
    output_file = output_dir / "facilities_canonical.parquet"
    df.to_parquet(output_file, index=False, compression='snappy')
    print(f"Wrote {output_file} with {len(df)} facilities")

    # Also write a backup with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = Path("data") / f"facilities_canonical_{timestamp}.parquet"
    backup_file.parent.mkdir(exist_ok=True)
    df.to_parquet(backup_file, index=False, compression='snappy')
    print(f"Wrote backup to {backup_file}")

    # Print summary statistics
    print("\n=== Export Summary ===")
    print(f"Total facilities: {len(df)}")
    print(f"Countries: {df['country_iso3'].nunique()}")
    print(f"With canonical names: {df['canonical_name'].notna().sum()}")
    print(f"With canonical slugs: {df['canonical_slug'].notna().sum()}")
    print(f"With coordinates: {df['lat'].notna().sum()}")
    print(f"With towns: {df['town'].notna().sum()}")
    print(f"Primary types distribution:")
    print(df['primary_type'].value_counts().head(10))


if __name__ == "__main__":
    main()