#!/usr/bin/env python3
"""
Migrate facilities from Mines.csv to structured JSON format with entityidentity integration.

This script:
1. Reads the Mines.csv file
2. Normalizes countries, metals, and companies using entityidentity
3. Creates canonical facility JSON files organized by country
4. Generates per-metal facility indexes
5. Creates audit logs and mapping files
"""

import csv
import json
import re
import sys
import pathlib
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import logging

# Add entityidentity to path
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent.parent / 'entityidentity'))

try:
    from entityidentity import (
        company_identifier,
        country_identifier,
        metal_identifier,
        match_company
    )
    ENTITYIDENTITY_AVAILABLE = True
except ImportError:
    print("Warning: entityidentity not available. Using fallback resolution.")
    ENTITYIDENTITY_AVAILABLE = False

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Paths
ROOT = pathlib.Path(__file__).parent.parent
MINES_CSV = ROOT / "Mines.csv"
CONFIG_DIR = ROOT / "config"
FACILITIES_DIR = CONFIG_DIR / "facilities"
SUPPLY_DIR = CONFIG_DIR / "supply"
MAPPINGS_DIR = CONFIG_DIR / "mappings"
OUTPUT_DIR = ROOT / "output"
MIGRATION_LOGS_DIR = OUTPUT_DIR / "migration_logs"

# Create directories
for dir_path in [FACILITIES_DIR, MAPPINGS_DIR, MIGRATION_LOGS_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Confidence mapping
CONFIDENCE_MAP = {
    "Very High": 0.95,
    "High": 0.85,
    "Moderate": 0.65,
    "Low": 0.40,
    "Very Low": 0.20
}

# Metal normalization map (for common variations)
METAL_NORMALIZE_MAP = {
    "aluminium": "aluminum",
    "ferronickel": "nickel",
    "ferromanganese": "manganese",
    "ferrosilicon manganese": "manganese",
    "chromite": "chromium",
    "pgm": "platinum",
    "pge": "platinum",
    "ree": "rare earths",
    "ferrous": "iron"
}

# ISO2 to ISO3 mapping (partial - extend as needed)
ISO2_TO_ISO3 = {
    "US": "USA", "GB": "GBR", "AU": "AUS", "CA": "CAN", "CN": "CHN",
    "RU": "RUS", "ZA": "ZAF", "BR": "BRA", "IN": "IND", "DE": "DEU",
    "FR": "FRA", "JP": "JPN", "KR": "KOR", "MX": "MEX", "CL": "CHL",
    "PE": "PER", "AR": "ARG", "NO": "NOR", "SE": "SWE", "FI": "FIN"
}

# Country name to ISO3 mapping (for common names)
COUNTRY_TO_ISO3 = {
    "USA": "USA", "United States": "USA", "US": "USA", "America": "USA",
    "UK": "GBR", "United Kingdom": "GBR", "Britain": "GBR", "England": "GBR",
    "Australia": "AUS", "Canada": "CAN", "China": "CHN", "Russia": "RUS",
    "South Africa": "ZAF", "Brazil": "BRA", "India": "IND", "Germany": "DEU",
    "France": "FRA", "Japan": "JPN", "South Korea": "KOR", "Mexico": "MEX",
    "Chile": "CHL", "Peru": "PER", "Argentina": "ARG", "Norway": "NOR",
    "Sweden": "SWE", "Finland": "FIN", "Indonesia": "IDN", "Philippines": "PHL",
    "Papua New Guinea": "PNG", "New Zealand": "NZL", "DRC": "COD",
    "Democratic Republic of Congo": "COD", "Congo": "COG", "Zambia": "ZMB",
    "Zimbabwe": "ZWE", "Tanzania": "TZA", "Ghana": "GHA", "Mali": "MLI"
}


class FacilityMigrator:
    """Handles migration of facilities from CSV to JSON format."""

    def __init__(self):
        self.company_cache = {}
        self.country_cache = {}
        self.metal_cache = {}
        self.facilities = []
        self.per_metal_facilities = defaultdict(set)
        self.stats = defaultdict(int)
        self.company_mapping = {}
        self.errors = []

    def slugify(self, text: str) -> str:
        """Convert text to URL-safe slug."""
        if not text:
            return ""
        text = text.lower().strip()
        text = re.sub(r'[^a-z0-9]+', '-', text)
        return text.strip('-')

    def split_semicolon(self, text: str) -> List[str]:
        """Split semicolon-separated values and clean them."""
        if not text:
            return []
        return [x.strip() for x in text.split(';') if x.strip()]

    def normalize_country(self, country_name: str) -> Optional[str]:
        """Normalize country name to ISO3 code."""
        if not country_name:
            return None

        # Check cache first
        if country_name in self.country_cache:
            return self.country_cache[country_name]

        # Try direct mapping
        if country_name in COUNTRY_TO_ISO3:
            iso3 = COUNTRY_TO_ISO3[country_name]
            self.country_cache[country_name] = iso3
            return iso3

        # Try entityidentity if available
        if ENTITYIDENTITY_AVAILABLE:
            try:
                iso2 = country_identifier(country_name)
                if iso2:
                    iso3 = ISO2_TO_ISO3.get(iso2, iso2)
                    self.country_cache[country_name] = iso3
                    logger.debug(f"Resolved country '{country_name}' to '{iso3}' via entityidentity")
                    return iso3
            except Exception as e:
                logger.warning(f"Error resolving country '{country_name}': {e}")

        # Fallback: use first 3 letters uppercase
        iso3 = country_name[:3].upper() if len(country_name) >= 3 else country_name.upper()
        self.country_cache[country_name] = iso3
        logger.warning(f"Using fallback ISO3 '{iso3}' for country '{country_name}'")
        return iso3

    def normalize_metal(self, metal_name: str) -> str:
        """Normalize metal name to canonical form."""
        if not metal_name:
            return ""

        metal_lower = metal_name.lower().strip()

        # Check cache
        if metal_lower in self.metal_cache:
            return self.metal_cache[metal_lower]

        # Try direct mapping
        if metal_lower in METAL_NORMALIZE_MAP:
            normalized = METAL_NORMALIZE_MAP[metal_lower]
            self.metal_cache[metal_lower] = normalized
            return normalized

        # Try entityidentity if available
        if ENTITYIDENTITY_AVAILABLE:
            try:
                result = metal_identifier(metal_name)
                if result and 'name' in result:
                    normalized = result['name'].lower()
                    self.metal_cache[metal_lower] = normalized
                    return normalized
            except Exception as e:
                logger.debug(f"Could not resolve metal '{metal_name}': {e}")

        # Default: return lowercase version
        self.metal_cache[metal_lower] = metal_lower
        return metal_lower

    def resolve_company(self, company_name: str, country: Optional[str] = None) -> Optional[Dict]:
        """Resolve company name to canonical ID using entityidentity."""
        if not company_name:
            return None

        cache_key = (company_name, country)
        if cache_key in self.company_cache:
            return self.company_cache[cache_key]

        result = None
        if ENTITYIDENTITY_AVAILABLE:
            try:
                # Try to get canonical identifier
                canonical = company_identifier(company_name, country)
                if canonical:
                    # Get full company details
                    company_data = match_company(company_name, country)
                    if company_data:
                        company_id = f"cmp-{self.slugify(canonical)}"
                        result = {
                            'company_id': company_id,
                            'canonical_name': company_data.get('name', canonical),
                            'country': company_data.get('country'),
                            'lei': company_data.get('lei'),
                            'wikidata_qid': company_data.get('wikidata_qid'),
                            'confidence': 0.85
                        }
                        self.company_mapping[company_name] = company_id
                        logger.debug(f"Resolved company '{company_name}' to '{company_id}'")
            except Exception as e:
                logger.debug(f"Could not resolve company '{company_name}': {e}")

        self.company_cache[cache_key] = result
        return result

    def parse_csv_row(self, row: Dict, row_num: int) -> Optional[Dict]:
        """Parse a single CSV row into facility JSON structure."""
        try:
            # Extract basic fields
            name = row.get("Mine Name", "").strip() or row.get("Mine Name ", "").strip()
            if not name:
                logger.warning(f"Row {row_num}: No mine name found, skipping")
                return None

            # Get country and normalize
            country_raw = row.get("Country or Region", "").strip()
            country_iso3 = self.normalize_country(country_raw)
            if not country_iso3:
                country_iso3 = "UNK"
                logger.warning(f"Row {row_num}: Could not resolve country '{country_raw}'")

            # Extract coordinates
            lat = None
            lon = None
            try:
                if row.get("Latitude"):
                    lat = float(row["Latitude"])
                if row.get("Longitude"):
                    lon = float(row["Longitude"])
            except ValueError:
                logger.warning(f"Row {row_num}: Invalid coordinates for {name}")

            # Parse asset types
            asset_types = self.split_semicolon(row.get("Asset Type", ""))
            if not asset_types:
                asset_types = ["mine"]  # Default to mine if not specified
            types = [t.lower() for t in asset_types]

            # Parse commodities
            primary = self.normalize_metal(row.get("Primary Commodity", ""))
            secondary = [self.normalize_metal(m) for m in self.split_semicolon(row.get("Secondary Commodity", ""))]
            other = [self.normalize_metal(m) for m in self.split_semicolon(row.get("Other Commodities", ""))]

            # Build commodities list
            commodities = []
            all_metals = []
            if primary:
                commodities.append({"metal": primary, "primary": True})
                all_metals.append(primary)
            for metal in secondary + other:
                if metal and metal not in all_metals:
                    commodities.append({"metal": metal, "primary": False})
                    all_metals.append(metal)

            # Generate facility ID
            facility_id = f"{country_iso3.lower()}-{self.slugify(name)}-fac"

            # Parse aliases
            aliases = self.split_semicolon(row.get("Group Names", ""))

            # Get confidence
            confidence_str = row.get("Confidence Factor", "")
            confidence = CONFIDENCE_MAP.get(confidence_str, 0.5)

            # Build facility object
            facility = {
                "facility_id": facility_id,
                "name": name,
                "aliases": aliases,
                "country_iso3": country_iso3,
                "location": {
                    "lat": lat,
                    "lon": lon,
                    "precision": "site" if (lat and lon) else "unknown"
                },
                "types": types,
                "commodities": commodities,
                "status": "unknown",  # Will be enriched later
                "owner_links": [],    # Will be enriched later
                "operator_link": None, # Will be enriched later
                "products": [],        # Will be enriched later
                "sources": [
                    {"type": "mines_csv", "row": row_num}
                ],
                "verification": {
                    "status": "csv_imported",
                    "confidence": confidence,
                    "last_checked": datetime.now().isoformat(),
                    "checked_by": "migration_script"
                }
            }

            # Track metals for indexing
            for metal in all_metals:
                self.per_metal_facilities[metal].add(facility_id)

            # Update stats
            self.stats['total_facilities'] += 1
            self.stats[f'country_{country_iso3}'] += 1
            for metal in all_metals:
                self.stats[f'metal_{metal}'] += 1

            return facility

        except Exception as e:
            logger.error(f"Row {row_num}: Error parsing row: {e}")
            self.errors.append(f"Row {row_num}: {str(e)}")
            return None

    def migrate_csv(self):
        """Main migration from CSV to JSON."""
        logger.info(f"Starting migration from {MINES_CSV}")

        if not MINES_CSV.exists():
            logger.error(f"Mines.csv not found at {MINES_CSV}")
            return False

        with open(MINES_CSV, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
                facility = self.parse_csv_row(row, row_num)
                if facility:
                    self.facilities.append(facility)

                # Progress reporting
                if row_num % 500 == 0:
                    logger.info(f"Processed {row_num} rows...")

        logger.info(f"Parsed {len(self.facilities)} facilities from CSV")
        return True

    def write_facilities(self):
        """Write facility JSON files organized by country."""
        logger.info("Writing facility JSON files...")

        for facility in self.facilities:
            country_iso3 = facility['country_iso3']
            country_dir = FACILITIES_DIR / country_iso3
            country_dir.mkdir(parents=True, exist_ok=True)

            facility_file = country_dir / f"{facility['facility_id']}.json"
            with open(facility_file, 'w', encoding='utf-8') as f:
                json.dump(facility, f, ensure_ascii=False, indent=2)

            self.stats['files_written'] += 1

        logger.info(f"Wrote {self.stats['files_written']} facility files")

    def write_metal_indexes(self):
        """Create per-metal facility index files."""
        logger.info("Writing metal index files...")

        for metal, facility_ids in self.per_metal_facilities.items():
            metal_dir = SUPPLY_DIR / self.slugify(metal)
            metal_dir.mkdir(parents=True, exist_ok=True)

            index_data = {
                "generated": datetime.now().isoformat(),
                "metal": metal,
                "total_facilities": len(facility_ids),
                "facilities": sorted(list(facility_ids))
            }

            index_file = metal_dir / "facilities.index.json"
            with open(index_file, 'w', encoding='utf-8') as f:
                json.dump(index_data, f, ensure_ascii=False, indent=2)

            logger.info(f"Created index for {metal}: {len(facility_ids)} facilities")

    def write_mappings(self):
        """Write mapping files for companies, countries, and metals."""
        logger.info("Writing mapping files...")

        # Company mappings
        if self.company_mapping:
            company_map_file = MAPPINGS_DIR / "company_canonical.json"
            with open(company_map_file, 'w', encoding='utf-8') as f:
                json.dump(self.company_mapping, f, ensure_ascii=False, indent=2)
            logger.info(f"Wrote {len(self.company_mapping)} company mappings")

        # Country mappings
        country_map_file = MAPPINGS_DIR / "country_canonical.json"
        with open(country_map_file, 'w', encoding='utf-8') as f:
            json.dump(self.country_cache, f, ensure_ascii=False, indent=2)
        logger.info(f"Wrote {len(self.country_cache)} country mappings")

        # Metal mappings
        metal_map_file = MAPPINGS_DIR / "metal_canonical.json"
        with open(metal_map_file, 'w', encoding='utf-8') as f:
            json.dump(self.metal_cache, f, ensure_ascii=False, indent=2)
        logger.info(f"Wrote {len(self.metal_cache)} metal mappings")

    def write_migration_report(self):
        """Write detailed migration report."""
        report = {
            "timestamp": datetime.now().isoformat(),
            "entityidentity_available": ENTITYIDENTITY_AVAILABLE,
            "statistics": dict(self.stats),
            "errors": self.errors,
            "summary": {
                "total_facilities": self.stats['total_facilities'],
                "total_countries": len([k for k in self.stats.keys() if k.startswith('country_')]),
                "total_metals": len([k for k in self.stats.keys() if k.startswith('metal_')]),
                "files_written": self.stats['files_written']
            }
        }

        report_file = MIGRATION_LOGS_DIR / f"migration_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        logger.info(f"Migration report written to {report_file}")

        # Print summary
        print("\n" + "="*60)
        print("MIGRATION SUMMARY")
        print("="*60)
        print(f"Total facilities migrated: {self.stats['total_facilities']}")
        print(f"Countries represented: {report['summary']['total_countries']}")
        print(f"Metals/commodities: {report['summary']['total_metals']}")
        print(f"Files written: {self.stats['files_written']}")
        if self.errors:
            print(f"Errors encountered: {len(self.errors)}")
        print("="*60)

    def run(self):
        """Execute the full migration process."""
        logger.info("Starting facility migration...")

        # Step 1: Parse CSV
        if not self.migrate_csv():
            logger.error("Failed to parse CSV, aborting migration")
            return False

        # Step 2: Write facility files
        self.write_facilities()

        # Step 3: Write metal indexes
        self.write_metal_indexes()

        # Step 4: Write mappings
        self.write_mappings()

        # Step 5: Write report
        self.write_migration_report()

        logger.info("Migration completed successfully!")
        return True


def main():
    """Main entry point."""
    migrator = FacilityMigrator()
    success = migrator.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()