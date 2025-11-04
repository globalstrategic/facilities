#!/usr/bin/env python3
"""Apply geocoding enrichments directly to facility JSONs."""

import json
from pathlib import Path

# Enrichments from external sources
enrichments = [
    {
        "facility_id": "ago-longonjo-fac",
        "lat": -12.91667,
        "lon": 15.21667,
        "precision": "site",
        "town": "Longonjo",
        "region": "Huambo",
        "source": "Longonjo carbonatite intrusion coords (Mindat); Pensana project location confirmed"
    },
    {
        "facility_id": "mco-aurum-monaco-refinery-fac",
        "lat": 43.72811,
        "lon": 7.41404,
        "precision": "exact",
        "town": "Monaco",
        "region": "Monaco",
        "source": "Address 1 Rue du Gabian (Le Thalès); coords from nearby POIs on Rue du Gabian"
    },
    {
        "facility_id": "mdg-green-giant-project-fac",
        "lat": -24.08000,
        "lon": 45.01417,
        "precision": "region",
        "town": None,  # Region-level only
        "region": "Atsimo-Andrefana",
        "source": "Green Giant vanadium project near Fotadrevo/Betioky Sud; Mindat estimated coords"
    },
    {
        "facility_id": "ner-tarouadji-project-fac",
        "lat": 17.34583,
        "lon": 8.41886,
        "precision": "region",
        "town": None,  # Region-level only
        "region": "Agadez",
        "source": "Permit corner coordinates per Table 1; centroid approx; verify on next pass"
    }
]

applied = 0
for enrich in enrichments:
    fac_id = enrich["facility_id"]
    country = fac_id.split("-")[0].upper()
    fac_path = Path(f"facilities/{country}/{fac_id}.json")

    if not fac_path.exists():
        print(f"Warning: {fac_path} not found")
        continue

    try:
        # Load facility
        with open(fac_path, 'r', encoding='utf-8') as f:
            facility = json.load(f)

        # Update location
        if "location" not in facility:
            facility["location"] = {}

        facility["location"]["lat"] = enrich["lat"]
        facility["location"]["lon"] = enrich["lon"]
        facility["location"]["precision"] = enrich["precision"]

        if enrich.get("town"):
            facility["location"]["town"] = enrich["town"]
        elif facility["location"].get("town"):
            # Clear if we only have region-level
            facility["location"]["town"] = None

        if enrich.get("region"):
            facility["location"]["region"] = enrich["region"]

        # Set data quality flags for region-level
        if enrich["precision"] == "region":
            if "data_quality" not in facility:
                facility["data_quality"] = {}
            if "flags" not in facility["data_quality"]:
                facility["data_quality"]["flags"] = {}
            facility["data_quality"]["flags"]["town_missing"] = True

        # Add source as verification note
        if "verification" not in facility:
            facility["verification"] = {}
        facility["verification"]["geocoding_source"] = enrich["source"]

        # Write back
        with open(fac_path, 'w', encoding='utf-8') as f:
            json.dump(facility, f, ensure_ascii=False, indent=2)
            f.write("\n")

        applied += 1
        print(f"Updated {fac_id}")

    except Exception as e:
        print(f"Error updating {fac_id}: {e}")

print(f"\nApplied {applied} geocoding enrichments")