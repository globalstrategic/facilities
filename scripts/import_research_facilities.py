#!/usr/bin/env python3
"""
Import facilities from deep research reports into the facilities database.

This script:
1. Accepts structured input (CSV/JSON) from research reports
2. Maps fields to the facility schema
3. Checks for duplicate facilities before creating
4. Generates proper facility IDs and metadata
5. Supports any country and handles various facility types

Usage:
    python import_research_facilities.py input.csv --country DZA --source "Algeria Mining Report 2025"
    python import_research_facilities.py input.json --country AFG --format json
"""

import csv
import json
import re
import sys
import pathlib
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import logging
from collections import defaultdict

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('research_import.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Paths
ROOT = pathlib.Path(__file__).parent.parent
FACILITIES_DIR = ROOT / "config" / "facilities"
OUTPUT_DIR = ROOT / "output"
IMPORT_LOGS_DIR = OUTPUT_DIR / "import_logs"

# Create directories
for dir_path in [FACILITIES_DIR, IMPORT_LOGS_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Metal normalization map
METAL_NORMALIZE_MAP = {
    "aluminium": "aluminum",
    "ferronickel": "nickel",
    "ferromanganese": "manganese",
    "ferrosilicon manganese": "manganese",
    "chromite": "chromium",
    "pgm": "platinum",
    "pge": "platinum",
    "ree": "rare earths",
    "rare earth elements": "rare earths",
    "ferrous": "iron",
    "fe": "iron",
    "cu": "copper",
    "au": "gold",
    "ag": "silver",
    "zn": "zinc",
    "pb": "lead",
    "ni": "nickel",
    "co": "cobalt",
    "li": "lithium",
    "u": "uranium",
    "gemstones": "precious stones",
    "lapis lazuli": "precious stones",
    "ruby": "precious stones",
    "emerald": "precious stones",
    "spinel": "precious stones",
    "barium": "baryte",
    "barite": "baryte"
}

# Facility type mapping
TYPE_NORMALIZE_MAP = {
    "open pit mine": "mine",
    "underground mine": "mine",
    "surface mine": "mine",
    "coal mine": "mine",
    "deposit": "mine",
    "mine": "mine",
    "smelter": "smelter",
    "refinery": "refinery",
    "processing facility": "plant",
    "processing complex": "plant",
    "steel complex": "plant",
    "steelworks": "plant",
    "cement plant": "plant",
    "cement factory": "plant",
    "fertilizer complex": "plant",
    "concentrator": "concentrator",
    "mill": "mill",
    "heap leach": "heap_leach",
    "tailings": "tailings",
    "quarry": "mine",
    "pegmatite field": "exploration"
}

# Status mapping
STATUS_MAP = {
    "operational": "operating",
    "operating": "operating",
    "active": "operating",
    "in development": "construction",
    "under construction": "construction",
    "construction": "construction",
    "proposed": "planned",
    "planned": "planned",
    "contracted": "planned",
    "undeveloped": "planned",
    "closed": "closed",
    "inactive": "closed",
    "suspended": "suspended",
    "care and maintenance": "care_and_maintenance",
    "stalled": "suspended",
    "unknown": "unknown",
    "relaunching": "planned"
}


class FacilityImporter:
    """Handles import of facilities from research reports."""

    def __init__(self, country_iso3: str, source_name: str):
        self.country_iso3 = country_iso3.upper()
        self.source_name = source_name
        self.facilities = []
        self.stats = defaultdict(int)
        self.errors = []
        self.duplicates_found = []
        self.existing_facilities = self._load_existing_facilities()

    def slugify(self, text: str) -> str:
        """Convert text to URL-safe slug."""
        if not text:
            return ""
        text = text.lower().strip()
        # Remove parenthetical content
        text = re.sub(r'\([^)]*\)', '', text)
        # Convert to slug
        text = re.sub(r'[^a-z0-9]+', '-', text)
        return text.strip('-')

    def _load_existing_facilities(self) -> Dict[str, Dict]:
        """Load all existing facility files for this country."""
        existing = {}
        country_dir = FACILITIES_DIR / self.country_iso3
        if not country_dir.exists():
            return existing

        for facility_file in country_dir.glob("*.json"):
            try:
                with open(facility_file, 'r', encoding='utf-8') as f:
                    facility = json.load(f)
                    existing[facility['facility_id']] = facility
            except Exception as e:
                logger.warning(f"Could not load {facility_file}: {e}")

        logger.info(f"Loaded {len(existing)} existing facilities for {self.country_iso3}")
        return existing

    def check_duplicate(self, facility_id: str, name: str, lat: Optional[float], lon: Optional[float]) -> Optional[str]:
        """
        Check if a facility already exists.
        Returns the existing facility_id if found, None otherwise.
        """
        # Check by exact ID
        if facility_id in self.existing_facilities:
            return facility_id

        # Check by name similarity and location proximity
        for existing_id, existing in self.existing_facilities.items():
            # Name match (case insensitive)
            if name.lower() == existing['name'].lower():
                # If both have coordinates, check distance
                if lat and lon and existing['location'].get('lat') and existing['location'].get('lon'):
                    # Simple distance check (within ~1km = 0.01 degrees)
                    lat_diff = abs(lat - existing['location']['lat'])
                    lon_diff = abs(lon - existing['location']['lon'])
                    if lat_diff < 0.01 and lon_diff < 0.01:
                        return existing_id
                else:
                    # No coordinates to compare, assume duplicate by name
                    return existing_id

            # Check aliases
            if name.lower() in [a.lower() for a in existing.get('aliases', [])]:
                return existing_id

        return None

    def normalize_metal(self, metal_name: str) -> str:
        """Normalize metal name to canonical form."""
        if not metal_name:
            return ""

        metal_lower = metal_name.lower().strip()

        # Try direct mapping
        if metal_lower in METAL_NORMALIZE_MAP:
            return METAL_NORMALIZE_MAP[metal_lower]

        return metal_lower

    def parse_commodities(self, primary: str, other: str) -> List[Dict]:
        """Parse commodity list from primary and other commodities."""
        commodities = []
        seen = set()

        # Parse primary
        if primary:
            for metal in re.split(r'[,;]', primary):
                metal = self.normalize_metal(metal.strip())
                if metal and metal not in seen:
                    commodities.append({"metal": metal, "primary": True})
                    seen.add(metal)

        # Parse other
        if other:
            for metal in re.split(r'[,;]', other):
                metal = self.normalize_metal(metal.strip())
                if metal and metal not in seen:
                    commodities.append({"metal": metal, "primary": False})
                    seen.add(metal)

        return commodities

    def parse_facility_types(self, type_str: str) -> List[str]:
        """Parse and normalize facility types."""
        if not type_str:
            return ["mine"]  # Default

        types = []
        for t in re.split(r'[,;]', type_str.lower()):
            t = t.strip()
            normalized = TYPE_NORMALIZE_MAP.get(t, t)
            if normalized and normalized not in types:
                types.append(normalized)

        return types if types else ["mine"]

    def parse_status(self, status_str: str) -> str:
        """Parse and normalize operational status."""
        if not status_str:
            return "unknown"

        status_lower = status_str.lower().strip()
        return STATUS_MAP.get(status_lower, "unknown")

    def parse_coordinates(self, lat_str: str, lon_str: str) -> Tuple[Optional[float], Optional[float]]:
        """Parse latitude and longitude strings."""
        lat, lon = None, None
        try:
            if lat_str and lat_str.strip():
                lat = float(lat_str.strip())
            if lon_str and lon_str.strip():
                lon = float(lon_str.strip())
        except ValueError:
            pass
        return lat, lon

    def parse_csv_row(self, row: Dict, row_num: int) -> Optional[Dict]:
        """Parse a single CSV row into facility JSON structure."""
        try:
            # Extract name - try various column names
            name = (row.get("Site/Mine Name") or row.get("Asset Name") or
                   row.get("Mine Name") or row.get("Facility Name") or
                   row.get("name") or "").strip()

            if not name or name == "-":
                logger.warning(f"Row {row_num}: No facility name found, skipping")
                return None

            # Generate facility ID
            facility_id = f"{self.country_iso3.lower()}-{self.slugify(name)}-fac"

            # Extract coordinates
            lat, lon = self.parse_coordinates(
                row.get("Latitude") or row.get("lat") or "",
                row.get("Longitude") or row.get("lon") or ""
            )

            # Check for duplicates
            existing_id = self.check_duplicate(facility_id, name, lat, lon)
            if existing_id:
                logger.info(f"Row {row_num}: Duplicate found - '{name}' already exists as {existing_id}")
                self.duplicates_found.append({
                    "row": row_num,
                    "name": name,
                    "existing_id": existing_id
                })
                self.stats['duplicates_skipped'] += 1
                return None

            # Parse asset types
            type_str = (row.get("Asset Type") or row.get("Facility Type") or
                       row.get("types") or "mine").strip()
            types = self.parse_facility_types(type_str)

            # Parse commodities
            primary = (row.get("Primary Commodity") or row.get("primary") or "").strip()
            other = (row.get("Other Commodities") or row.get("Secondary Commodity") or
                    row.get("secondary") or "").strip()
            commodities = self.parse_commodities(primary, other)

            # Parse status
            status_str = (row.get("Operational Status") or row.get("Status/Notes") or
                         row.get("status") or "unknown").strip()
            # Extract just the status keyword from longer descriptions
            status_match = re.search(r'\b(operational|operating|active|construction|proposed|planned|closed|inactive|suspended|stalled|undeveloped)\b',
                                    status_str.lower())
            status = self.parse_status(status_match.group(1) if status_match else status_str)

            # Parse aliases/synonyms
            aliases_str = (row.get("Synonyms") or row.get("Aliases") or
                          row.get("Alternative Names") or "").strip()
            aliases = [a.strip() for a in aliases_str.split(',') if a.strip() and a.strip() != "-"]

            # Parse operator
            operator_str = (row.get("Operator(s) / Key Stakeholders") or
                          row.get("Operator") or row.get("operator") or "").strip()
            operator_link = None
            if operator_str and operator_str != "-":
                # For now, store as a simple string in notes
                # In a real system, this would resolve to a company_id
                pass

            # Notes from analyst notes or status/notes field
            notes = (row.get("Analyst Notes & Key Snippet IDs") or
                    row.get("Status/Notes") or "").strip()
            if notes and notes != "-":
                notes = notes[:500]  # Limit length
            else:
                notes = None

            # Build facility object
            facility = {
                "facility_id": facility_id,
                "name": name,
                "aliases": aliases,
                "country_iso3": self.country_iso3,
                "location": {
                    "lat": lat,
                    "lon": lon,
                    "precision": "site" if (lat and lon) else "unknown"
                },
                "types": types,
                "commodities": commodities,
                "status": status,
                "owner_links": [],
                "operator_link": operator_link,
                "products": [],
                "sources": [
                    {
                        "type": "gemini_research",
                        "id": self.source_name,
                        "date": datetime.now().isoformat()
                    }
                ],
                "verification": {
                    "status": "llm_suggested",
                    "confidence": 0.75,
                    "last_checked": datetime.now().isoformat(),
                    "checked_by": "research_import_script",
                    "notes": notes
                }
            }

            # Update stats
            self.stats['total_facilities'] += 1
            self.stats[f'type_{types[0]}'] += 1
            if commodities:
                for comm in commodities:
                    self.stats[f'metal_{comm["metal"]}'] += 1

            return facility

        except Exception as e:
            logger.error(f"Row {row_num}: Error parsing row: {e}")
            self.errors.append(f"Row {row_num}: {str(e)}")
            return None

    def import_csv(self, csv_path: pathlib.Path) -> bool:
        """Import facilities from CSV file."""
        logger.info(f"Starting import from {csv_path}")

        if not csv_path.exists():
            logger.error(f"CSV file not found at {csv_path}")
            return False

        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row_num, row in enumerate(reader, start=2):
                facility = self.parse_csv_row(row, row_num)
                if facility:
                    self.facilities.append(facility)

                # Progress reporting
                if row_num % 100 == 0:
                    logger.info(f"Processed {row_num} rows...")

        logger.info(f"Parsed {len(self.facilities)} new facilities from CSV")
        return True

    def import_json(self, json_path: pathlib.Path) -> bool:
        """Import facilities from JSON file."""
        logger.info(f"Starting import from {json_path}")

        if not json_path.exists():
            logger.error(f"JSON file not found at {json_path}")
            return False

        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Expect a list of facility objects or a dict with a 'facilities' key
        if isinstance(data, list):
            facilities_data = data
        elif isinstance(data, dict) and 'facilities' in data:
            facilities_data = data['facilities']
        else:
            logger.error("JSON format not recognized. Expected a list or dict with 'facilities' key")
            return False

        for idx, row in enumerate(facilities_data, start=1):
            facility = self.parse_csv_row(row, idx)
            if facility:
                self.facilities.append(facility)

        logger.info(f"Parsed {len(self.facilities)} new facilities from JSON")
        return True

    def write_facilities(self) -> None:
        """Write facility JSON files to the facilities directory."""
        logger.info("Writing facility JSON files...")

        country_dir = FACILITIES_DIR / self.country_iso3
        country_dir.mkdir(parents=True, exist_ok=True)

        for facility in self.facilities:
            facility_file = country_dir / f"{facility['facility_id']}.json"
            with open(facility_file, 'w', encoding='utf-8') as f:
                json.dump(facility, f, ensure_ascii=False, indent=2)

            self.stats['files_written'] += 1

        logger.info(f"Wrote {self.stats['files_written']} facility files")

    def write_import_report(self) -> None:
        """Write detailed import report."""
        report = {
            "timestamp": datetime.now().isoformat(),
            "source": self.source_name,
            "country_iso3": self.country_iso3,
            "statistics": dict(self.stats),
            "errors": self.errors,
            "duplicates_found": self.duplicates_found,
            "summary": {
                "new_facilities": self.stats['total_facilities'],
                "duplicates_skipped": self.stats['duplicates_skipped'],
                "files_written": self.stats['files_written'],
                "errors": len(self.errors)
            }
        }

        report_file = IMPORT_LOGS_DIR / f"import_report_{self.country_iso3}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        logger.info(f"Import report written to {report_file}")

        # Print summary
        print("\n" + "="*60)
        print("IMPORT SUMMARY")
        print("="*60)
        print(f"Country: {self.country_iso3}")
        print(f"Source: {self.source_name}")
        print(f"New facilities imported: {self.stats['total_facilities']}")
        print(f"Duplicates skipped: {self.stats['duplicates_skipped']}")
        print(f"Files written: {self.stats['files_written']}")
        if self.errors:
            print(f"Errors encountered: {len(self.errors)}")
        print("="*60)

    def run(self, input_path: pathlib.Path, format: str = "csv") -> bool:
        """Execute the full import process."""
        logger.info(f"Starting facility import for {self.country_iso3}...")

        # Import data
        if format == "csv":
            if not self.import_csv(input_path):
                logger.error("Failed to import CSV, aborting")
                return False
        elif format == "json":
            if not self.import_json(input_path):
                logger.error("Failed to import JSON, aborting")
                return False
        else:
            logger.error(f"Unknown format: {format}")
            return False

        # Write facility files
        if self.facilities:
            self.write_facilities()
        else:
            logger.warning("No new facilities to write (all may be duplicates)")

        # Write report
        self.write_import_report()

        logger.info("Import completed successfully!")
        return True


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Import facilities from deep research reports",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Import from CSV
  python import_research_facilities.py algeria_facilities.csv --country DZA --source "Algeria Mining Report 2025"

  # Import from JSON
  python import_research_facilities.py afghanistan.json --country AFG --format json --source "Afghanistan Report 2025"
        """
    )
    parser.add_argument("input_file", help="Input CSV or JSON file")
    parser.add_argument("--country", required=True, help="Country ISO3 code (e.g., DZA, AFG)")
    parser.add_argument("--source", required=True, help="Source name/description for citation")
    parser.add_argument("--format", choices=["csv", "json"], default="csv", help="Input file format")

    args = parser.parse_args()

    input_path = pathlib.Path(args.input_file)
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}")
        sys.exit(1)

    importer = FacilityImporter(args.country, args.source)
    success = importer.run(input_path, args.format)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
