#!/usr/bin/env python3
"""Fix data hygiene issues: UTF-8 mojibake, invalid ISO codes, bogus coordinates."""

import json
from pathlib import Path
import unicodedata
import re

# Try ftfy for mojibake fixing
try:
    import ftfy
    HAS_FTFY = True
except ImportError:
    HAS_FTFY = False
    print("Warning: ftfy not installed. Install with: pip install ftfy")

def clean_text(text):
    """Clean text with NFC normalization and mojibake fixing."""
    if not text:
        return text

    # NFC normalize
    text = unicodedata.normalize("NFC", text)

    # Fix mojibake if ftfy available
    if HAS_FTFY:
        text = ftfy.fix_text(text)

    return text

def is_valid_coordinate(lat, lon):
    """Check if coordinates are valid."""
    if lat is None or lon is None:
        return False
    try:
        lat = float(lat)
        lon = float(lon)
        # Basic range check
        if not (-90 <= lat <= 90):
            return False
        if not (-180 <= lon <= 180):
            return False
        # Check for bogus coordinates
        if abs(lat) < 0.001 and abs(lon) < 0.001:  # 0,0
            return False
        return True
    except:
        return False

def fix_iso_code(iso3, filename):
    """Fix known ISO code issues."""
    # KOR incorrectly coded as ATF
    if iso3 == "ATF" and "kor-" in filename:
        return "KOR"
    return iso3

def fix_facility(facility, fac_path):
    """Fix hygiene issues in a facility."""
    modified = False

    # Fix ISO code
    old_iso = facility.get("country_iso3")
    new_iso = fix_iso_code(old_iso, fac_path.name)
    if new_iso != old_iso:
        facility["country_iso3"] = new_iso
        modified = True
        print(f"  Fixed ISO: {old_iso} → {new_iso}")

    # Clean all text fields
    text_fields = [
        "canonical_name", "display_name", "operator_display",
        "raw_name", "status", "canonical_slug"
    ]

    for field in text_fields:
        if field in facility and facility[field]:
            old_val = facility[field]
            new_val = clean_text(old_val)
            if new_val != old_val:
                facility[field] = new_val
                modified = True
                if old_val != new_val:  # Only show if actually different
                    print(f"  Fixed {field}: {repr(old_val)} → {repr(new_val)}")

    # Clean location fields
    if "location" in facility:
        loc = facility["location"]
        for field in ["town", "town_ascii", "region"]:
            if field in loc and loc[field]:
                old_val = loc[field]
                new_val = clean_text(old_val)
                if new_val != old_val:
                    loc[field] = new_val
                    modified = True
                    print(f"  Fixed location.{field}: {repr(old_val)} → {repr(new_val)}")

        # Check coordinates
        lat = loc.get("lat")
        lon = loc.get("lon")
        if lat is not None and lon is not None:
            if not is_valid_coordinate(lat, lon):
                print(f"  Warning: Invalid coordinates: {lat}, {lon}")
                # Don't auto-delete, just warn

    # Clean aliases
    if "aliases" in facility and facility["aliases"]:
        new_aliases = []
        for alias in facility["aliases"]:
            new_alias = clean_text(alias)
            if new_alias != alias:
                modified = True
                print(f"  Fixed alias: {repr(alias)} → {repr(new_alias)}")
            new_aliases.append(new_alias)
        facility["aliases"] = new_aliases

    # Clean company mentions
    if "company_mentions" in facility and facility["company_mentions"]:
        new_mentions = []
        for mention in facility["company_mentions"]:
            if isinstance(mention, str):
                new_mention = clean_text(mention)
                if new_mention != mention:
                    modified = True
                    print(f"  Fixed company_mention: {repr(mention)} → {repr(new_mention)}")
                new_mentions.append(new_mention)
            elif isinstance(mention, dict) and "name" in mention:
                old_name = mention["name"]
                new_name = clean_text(old_name)
                if new_name != old_name:
                    mention["name"] = new_name
                    modified = True
                    print(f"  Fixed company_mention.name: {repr(old_name)} → {repr(new_name)}")
                new_mentions.append(mention)
            else:
                new_mentions.append(mention)
        facility["company_mentions"] = new_mentions

    return facility, modified

def main():
    """Fix hygiene issues across all facilities."""

    facilities_dir = Path("facilities")

    fixed_count = 0
    warning_count = 0
    total_count = 0

    # Check KOR facilities specifically
    kor_wrong_dir = facilities_dir / "ATF"
    kor_right_dir = facilities_dir / "KOR"

    if kor_wrong_dir.exists():
        kor_right_dir.mkdir(exist_ok=True)
        for fac_file in kor_wrong_dir.glob("kor-*.json"):
            print(f"\nMoving {fac_file.name} from ATF/ to KOR/")
            new_path = kor_right_dir / fac_file.name

            # Load, fix ISO, and save
            with open(fac_file, 'r', encoding='utf-8') as f:
                facility = json.load(f)

            facility["country_iso3"] = "KOR"

            with open(new_path, 'w', encoding='utf-8') as f:
                json.dump(facility, f, ensure_ascii=False, indent=2)
                f.write("\n")

            # Remove old file
            fac_file.unlink()
            fixed_count += 1

        # Remove ATF directory if empty
        if not list(kor_wrong_dir.glob("*.json")):
            kor_wrong_dir.rmdir()
            print("Removed empty ATF directory")

    # Process all facilities
    for country_dir in sorted(facilities_dir.iterdir()):
        if not country_dir.is_dir():
            continue

        for fac_file in sorted(country_dir.glob("*.json")):
            total_count += 1

            try:
                with open(fac_file, 'r', encoding='utf-8') as f:
                    facility = json.load(f)

                facility, modified = fix_facility(facility, fac_file)

                if modified:
                    print(f"\nFixing {fac_file.name}")

                    # Write back with UTF-8
                    with open(fac_file, 'w', encoding='utf-8') as f:
                        json.dump(facility, f, ensure_ascii=False, indent=2)
                        f.write("\n")

                    fixed_count += 1

                # Check for warnings even if not modified
                loc = facility.get("location", {})
                if not is_valid_coordinate(loc.get("lat"), loc.get("lon")):
                    if loc.get("lat") is not None or loc.get("lon") is not None:
                        warning_count += 1

            except Exception as e:
                print(f"Error processing {fac_file}: {e}")
                warning_count += 1

    print(f"\n✓ Processed {total_count} facilities")
    print(f"✓ Fixed {fixed_count} facilities")
    if warning_count:
        print(f"⚠ {warning_count} facilities have warnings")

if __name__ == "__main__":
    main()