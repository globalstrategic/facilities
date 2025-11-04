#!/usr/bin/env python3
"""Fix known broken JSON files."""

import json
from pathlib import Path

fixes = [
    {
        "file": "facilities/MDG/mdg-green-giant-project-fac.json",
        "fix": """
{
  "facility_id": "mdg-green-giant-project-fac",
  "name": "Green Giant Project",
  "aliases": [],
  "country_iso3": "MDG",
  "location": {
    "lat": null,
    "lon": null,
    "precision": "unknown"
  },
  "types": [
    "exploration project"
  ],
  "commodities": [
    {
      "metal": "Vanadium",
      "primary": true,
      "chemical_formula": "V",
      "category": "specialty_metal"
    },
    {
      "metal": "Neodymium-Praseodymium",
      "primary": false,
      "category": "rare_earth_element"
    }
  ],
  "status": "unknown",
  "company_mentions": [],
  "products": [],
  "sources": [
    {
      "type": "research",
      "id": "Deep Research 2025",
      "date": "2025-10-30T14:29:33.125193"
    }
  ],
  "verification": {
    "status": "unverified",
    "confidence": 0.65,
    "last_checked": "2025-10-30T14:29:33.125198",
    "checked_by": "import_pipeline",
    "notes": null
  }
}
"""
    },
    {
        "file": "facilities/MDG/mdg-millie-s-reward-project-fac.json",
        "fix": """
{
  "facility_id": "mdg-millie-s-reward-project-fac",
  "name": "Millie's Reward Project",
  "aliases": [],
  "country_iso3": "MDG",
  "location": {
    "lat": null,
    "lon": null,
    "precision": "unknown"
  },
  "types": [
    "development"
  ],
  "commodities": [
    {
      "metal": "Copper",
      "primary": true,
      "chemical_formula": "Cu",
      "category": "base_metal"
    },
    {
      "metal": "Gold",
      "primary": false,
      "chemical_formula": "Au",
      "category": "precious_metal"
    }
  ],
  "status": "unknown",
  "company_mentions": [],
  "products": [],
  "sources": [
    {
      "type": "research",
      "id": "Deep Research 2025",
      "date": "2025-10-30T14:29:33.125193"
    }
  ],
  "verification": {
    "status": "unverified",
    "confidence": 0.65,
    "last_checked": "2025-10-30T14:29:33.125198",
    "checked_by": "import_pipeline",
    "notes": null
  }
}
"""
    },
    {
        "file": "facilities/NER/ner-tarouadji-project-fac.json",
        "fix": """
{
  "facility_id": "ner-tarouadji-project-fac",
  "name": "Tarouadji Project",
  "aliases": [],
  "country_iso3": "NER",
  "location": {
    "lat": null,
    "lon": null,
    "precision": "unknown"
  },
  "types": [
    "exploration"
  ],
  "commodities": [
    {
      "metal": "Lithium",
      "primary": true,
      "chemical_formula": "Li",
      "category": "battery_metal"
    },
    {
      "metal": "Tin",
      "primary": false,
      "chemical_formula": "Sn",
      "category": "base_metal"
    }
  ],
  "status": "unknown",
  "company_mentions": [],
  "products": [],
  "sources": [
    {
      "type": "research",
      "id": "Deep Research 2025",
      "date": "2025-10-30T14:29:25.607036"
    }
  ],
  "verification": {
    "status": "unverified",
    "confidence": 0.65,
    "last_checked": "2025-10-30T14:29:25.607041",
    "checked_by": "import_pipeline",
    "notes": null
  }
}
"""
    },
    {
        "file": "facilities/MMR/mmr-tha-byu-mine-fac.json",
        "fix": """
{
  "facility_id": "mmr-tha-byu-mine-fac",
  "name": "Tha-byu Mine",
  "aliases": [],
  "country_iso3": "MMR",
  "location": {
    "lat": null,
    "lon": null,
    "precision": "unknown"
  },
  "types": [
    "mine"
  ],
  "commodities": [
    {
      "metal": "Lead",
      "primary": true,
      "chemical_formula": "Pb",
      "category": "base_metal"
    },
    {
      "metal": "Silver",
      "primary": false,
      "chemical_formula": "Ag",
      "category": "precious_metal"
    },
    {
      "metal": "Zinc",
      "primary": false,
      "chemical_formula": "Zn",
      "category": "base_metal"
    }
  ],
  "status": "unknown",
  "company_mentions": [],
  "products": [],
  "sources": [
    {
      "type": "research",
      "id": "Deep Research 2025",
      "date": "2025-10-30T14:17:25.459749"
    }
  ],
  "verification": {
    "status": "unverified",
    "confidence": 0.65,
    "last_checked": "2025-10-30T14:17:25.459754",
    "checked_by": "import_pipeline",
    "notes": null
  }
}
"""
    },
    {
        "file": "facilities/SWE/swe-f-bodtj-rn-fac.json",
        "fix": """
{
  "facility_id": "swe-f-bodtj-rn-fac",
  "name": "Fäbodtjärn",
  "aliases": [],
  "country_iso3": "SWE",
  "location": {
    "lat": null,
    "lon": null,
    "precision": "unknown"
  },
  "types": [
    "exploration"
  ],
  "commodities": [
    {
      "metal": "Graphite",
      "primary": true,
      "chemical_formula": "C",
      "category": "industrial_mineral"
    }
  ],
  "status": "unknown",
  "company_mentions": [],
  "products": [],
  "sources": [
    {
      "type": "research",
      "id": "Deep Research 2025",
      "date": "2025-10-30T14:33:15.134652"
    }
  ],
  "verification": {
    "status": "unverified",
    "confidence": 0.65,
    "last_checked": "2025-10-30T14:33:15.134657",
    "checked_by": "import_pipeline",
    "notes": null
  }
}
"""
    }
]

for fix_spec in fixes:
    path = Path(fix_spec["file"])
    try:
        # Parse to validate JSON
        json.loads(fix_spec["fix"].strip())
        # Write fixed content
        path.write_text(fix_spec["fix"].strip() + "\n")
        print(f"Fixed {fix_spec['file']}")
    except Exception as e:
        print(f"Error fixing {fix_spec['file']}: {e}")

print("Done fixing broken JSONs")