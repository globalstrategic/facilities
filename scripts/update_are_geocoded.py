#!/usr/bin/env python3
"""
Update UAE facility files with geocoded coordinates and additional metadata
Based on manual research and verification
"""

import json
import os
from datetime import datetime
from pathlib import Path

# Define the updates for each facility
facility_updates = {
    "are-al-ahlia-gypsum-mine-fac": {
        "location": {
            "lat": None,
            "lon": None,
            "precision": "unknown"
        },
        "operator": "Al Ahlia Gypsum / Luluat Alahli",
        "notes": "Real company operating gypsum mine in UAE, but exact location not publicly disclosed. Office in Al Qusais, Dubai."
    },

    "are-emsteel-steel-division-fac": {
        "location": {
            "lat": 24.322033,
            "lon": 54.467987,
            "precision": "plant"
        },
        "operator": "EMSTEEL PJSC",
        "owner": "EMSTEEL PJSC (majority owned by ADQ via Senaat)",
        "town": "Mussafah",
        "province": "Abu Dhabi",
        "notes": "Integrated steel complex in ICAD I, Mussafah, Abu Dhabi"
    },

    "are-esnaa-copper-smelter-fac": {
        "location": {
            "lat": None,
            "lon": None,
            "precision": "region"
        },
        "status": "planned",
        "operator": "ESNAA Copper Non-Ferrous Metal Casting L.L.C.",
        "province": "Dubai",
        "notes": "Next-generation copper and precious-metals refining complex in designated industrial zone, Dubai. Under development."
    },

    "are-fujairah-cement-industries-fac": {
        "location": {
            "lat": 25.5529488,
            "lon": 56.2242521,
            "precision": "plant"
        },
        "operator": "Fujairah Cement Industries PJSC",
        "owner": "Fujairah Cement Industries PJSC",
        "town": "Dibba",
        "province": "Fujairah",
        "types": ["cement_plant"]
    },

    "are-fujairah-chrome-mine-fac": {
        "location": {
            "lat": 25.2930556,
            "lon": 56.0502778,
            "precision": "mine"
        },
        "operator": "Derwent Mining Ltd.",
        "owner": "Derwent Mining Ltd.",
        "province": "Fujairah",
        "types": ["mine"],
        "notes": "Large open-pit chromite mine"
    },

    "are-fujairah-clay-quarry-fac": {
        "location": {
            "lat": None,
            "lon": None,
            "precision": "emirate"
        },
        "operator": "Fujairah Natural Resources Corp.",
        "owner": "Fujairah Natural Resources Corp.",
        "province": "Fujairah",
        "types": ["quarry"],
        "notes": "Large surface clay mine; exact coordinates not publicly available"
    },

    "are-fujairah-mine-fac": {
        "location": {
            "lat": None,
            "lon": None,
            "precision": "unknown"
        },
        "owner": "Fujairah Natural Resources Corp.",
        "notes": "Generic FNRC surface mine record; consider merging into specific quarries or deleting once better-resolved mines are in database"
    },

    "are-khor-khwair-quarry-fac": {
        "location": {
            "lat": 25.967222,
            "lon": 56.054444,
            "precision": "industrial_zone"
        },
        "operator": "Stevin Rock LLC",
        "owner": "Government of Ras Al Khaimah (via Stevin Rock / RAK Rock)",
        "town": "Khor Khuwair",
        "province": "Ras Al Khaimah",
        "types": ["quarry"],
        "notes": "World's largest limestone quarry complex at Khor Khuwair / Saqr Port"
    },

    "are-national-cement-co-p-s-c-fac": {
        "location": {
            "lat": 25.157778,
            "lon": 55.243333,
            "precision": "plant"
        },
        "operator": "National Cement Co. P.S.C.",
        "owner": "National Cement Co. P.S.C.",
        "town": "Al Quoz",
        "province": "Dubai",
        "types": ["cement_plant"]
    },

    "are-sun-metal-casting-factory-llc-fac": {
        "location": {
            "lat": 25.506944,
            "lon": 55.558056,
            "precision": "industrial_zone_approx"
        },
        "operator": "Sun Metal Casting Factory LLC",
        "province": "Umm Al Quwain",
        "types": ["smelter", "secondary_smelter"],
        "notes": "Zinc/lead oxide plant in Umm Al Quwain Industrial Area 4; coordinates approximate to industrial-area centre"
    }
}

def update_facility(facility_path, updates):
    """Update a facility JSON file with new data"""
    # Read existing file
    with open(facility_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Apply updates
    if "location" in updates:
        data["location"] = updates["location"]

    if "operator" in updates:
        # Add to company_mentions if not already there
        if updates["operator"] not in data.get("company_mentions", []):
            if "company_mentions" not in data:
                data["company_mentions"] = []
            data["company_mentions"].append(updates["operator"])

    if "owner" in updates:
        # Add to company_mentions if not already there
        if updates["owner"] not in data.get("company_mentions", []):
            if "company_mentions" not in data:
                data["company_mentions"] = []
            if updates["owner"] not in data["company_mentions"]:
                data["company_mentions"].append(updates["owner"])

    if "town" in updates:
        data["town"] = updates["town"]

    if "province" in updates:
        data["province"] = updates["province"]

    if "types" in updates:
        data["types"] = updates["types"]

    if "status" in updates:
        data["status"] = updates["status"]

    # Update verification
    data["verification"]["last_checked"] = datetime.now().isoformat() + "Z"
    data["verification"]["checked_by"] = "manual_geocoding"
    if data["location"]["lat"] is not None:
        data["verification"]["confidence"] = 0.85  # High confidence for manually verified coords

    # Add geocoding source
    if "sources" not in data:
        data["sources"] = []
    data["sources"].append({
        "type": "geocoding",
        "id": "manual_verification_2025",
        "date": datetime.now().isoformat(),
        "notes": updates.get("notes", "")
    })

    # Update data quality flags
    if "data_quality" in data and "flags" in data["data_quality"]:
        if "town" in updates:
            data["data_quality"]["flags"]["town_missing"] = False
        if "operator" in updates or "owner" in updates:
            data["data_quality"]["flags"]["operator_unresolved"] = False

    # Write updated file
    with open(facility_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    return data

def main():
    """Update all UAE facilities with geocoded data"""
    base_path = Path("/Users/willb/Github/GSMC/facilities/facilities/ARE")

    updated_count = 0
    with_coords = 0

    for facility_id, updates in facility_updates.items():
        facility_file = base_path / f"{facility_id}.json"

        if not facility_file.exists():
            print(f"WARNING: {facility_file} not found, skipping...")
            continue

        print(f"Updating {facility_id}...")
        updated_data = update_facility(facility_file, updates)
        updated_count += 1

        if updated_data["location"]["lat"] is not None:
            with_coords += 1
            print(f"  ✓ Added coordinates: {updated_data['location']['lat']}, {updated_data['location']['lon']}")
            print(f"    Precision: {updated_data['location']['precision']}")
        else:
            print(f"  ⚠ No coordinates available (precision: {updated_data['location']['precision']})")

        if "operator" in updates:
            print(f"    Operator: {updates['operator']}")
        if "owner" in updates:
            print(f"    Owner: {updates['owner']}")

    print(f"\n✅ Updated {updated_count} facilities")
    print(f"   {with_coords} now have coordinates")
    print(f"   {updated_count - with_coords} still need manual geocoding")

if __name__ == "__main__":
    main()