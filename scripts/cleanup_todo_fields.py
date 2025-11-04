#!/usr/bin/env python3
"""One-off script to clean up TODO sentinels in facility JSONs."""

import json
import pathlib

count = 0
for p in pathlib.Path("facilities").rglob("*.json"):
    try:
        d = json.loads(p.read_text())
        loc = d.get("location") or {}

        # Check if town is "TODO" (case insensitive)
        if isinstance(loc.get("town"), str) and loc["town"].strip().upper() == "TODO":
            loc["town"] = None
            d.setdefault("data_quality", {}).setdefault("flags", {})["town_missing"] = True
            d["location"] = loc

            # Write back with UTF-8 support
            p.write_text(json.dumps(d, ensure_ascii=False, indent=2) + "\n")
            count += 1
    except Exception as e:
        print(f"Error processing {p}: {e}")

print(f"Done. Cleaned {count} files with TODO sentinels.")