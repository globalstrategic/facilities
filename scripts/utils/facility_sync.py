"""
Facility synchronization between JSON database and entityidentity parquet format.

This module provides bidirectional sync capabilities to export facilities to
the entityidentity parquet schema and import facilities from entityidentity
parquet files.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pycountry

logger = logging.getLogger(__name__)


# Define ISO conversion functions inline to avoid entityidentity dependency
def iso2_to_iso3(iso2: str) -> str:
    """
    Convert ISO 3166-1 alpha-2 code to alpha-3 code.

    Args:
        iso2: Two-letter country code (e.g., "DZ", "US")

    Returns:
        Three-letter country code (e.g., "DZA", "USA")

    Raises:
        ValueError: If country code is not found
    """
    try:
        country = pycountry.countries.get(alpha_2=iso2.upper())
        if country is None:
            raise ValueError(f"Invalid ISO2 country code: {iso2}")
        return country.alpha_3
    except (AttributeError, LookupError) as e:
        raise ValueError(f"Invalid ISO2 country code: {iso2}") from e


def iso3_to_iso2(iso3: str) -> str:
    """
    Convert ISO 3166-1 alpha-3 code to alpha-2 code.

    Args:
        iso3: Three-letter country code (e.g., "DZA", "USA")

    Returns:
        Two-letter country code (e.g., "DZ", "US")

    Raises:
        ValueError: If country code is not found
    """
    try:
        country = pycountry.countries.get(alpha_3=iso3.upper())
        if country is None:
            raise ValueError(f"Invalid ISO3 country code: {iso3}")
        return country.alpha_2
    except (AttributeError, LookupError) as e:
        raise ValueError(f"Invalid ISO3 country code: {iso3}") from e


class FacilitySyncManager:
    """
    Manage synchronization between facilities JSON database and entityidentity parquet format.

    This class handles:
    - Exporting facility JSONs to entityidentity parquet schema
    - Importing facilities from entityidentity parquet files
    - Schema conversion between formats
    - Duplicate detection and overwrite handling

    Attributes:
        facilities_dir: Path to facilities directory (default: facilities/)
    """

    def __init__(self, facilities_dir: Optional[Path] = None):
        """
        Initialize FacilitySyncManager.

        Args:
            facilities_dir: Path to facilities directory. Defaults to 'facilities/'
        """
        if facilities_dir is None:
            facilities_dir = Path(__file__).parent.parent.parent / "facilities"
        self.facilities_dir = Path(facilities_dir)

        if not self.facilities_dir.exists():
            raise ValueError(f"Facilities directory not found: {self.facilities_dir}")

    def export_to_entityidentity_format(self, output_path: Path) -> Path:
        """
        Export all facilities to entityidentity parquet format.

        Converts facility JSONs to a DataFrame matching the entityidentity schema:
        - facility_id, company_id, facility_name, alt_names, facility_type
        - country, country_iso3, admin1, city, address
        - lat, lon, geo_precision
        - commodities (list), process_stages, capacity_value, capacity_unit
        - operating_status, confidence, is_verified, verification_notes
        - evidence_urls, evidence_titles
        - first_seen_utc, last_seen_utc, source

        Args:
            output_path: Directory path for output parquet file

        Returns:
            Path to the generated parquet file

        Example:
            >>> manager = FacilitySyncManager()
            >>> output_file = manager.export_to_entityidentity_format(Path("output/"))
            >>> print(f"Exported to {output_file}")
        """
        logger.info("Starting facility export to entityidentity format...")

        # Generate timestamp filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = Path(output_path) / f"facilities_{timestamp}.parquet"
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # Load all facilities
        facilities = []
        for country_dir in self.facilities_dir.iterdir():
            if not country_dir.is_dir():
                continue

            for fac_file in country_dir.glob("*.json"):
                try:
                    with open(fac_file, 'r', encoding='utf-8') as f:
                        facility = json.load(f)
                        facilities.append(facility)
                except Exception as e:
                    logger.warning(f"Failed to load {fac_file}: {e}")
                    continue

        if not facilities:
            raise ValueError("No facilities found to export")

        logger.info(f"Loaded {len(facilities)} facilities from {self.facilities_dir}")

        # Convert to entityidentity format
        rows = []
        for facility in facilities:
            try:
                row = self._facility_to_parquet_row(facility)
                rows.append(row)
            except Exception as e:
                logger.warning(f"Failed to convert facility {facility.get('facility_id')}: {e}")
                continue

        # Create DataFrame
        df = pd.DataFrame(rows)

        # Ensure correct data types
        df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
        df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
        df['confidence'] = pd.to_numeric(df['confidence'], errors='coerce')
        df['is_verified'] = df['is_verified'].astype(bool)

        # Export to parquet
        df.to_parquet(output_file, index=False)

        logger.info(f"Successfully exported {len(df)} facilities to {output_file}")
        logger.info(f"Parquet file size: {output_file.stat().st_size / 1024 / 1024:.2f} MB")

        return output_file

    def import_from_entityidentity(
        self,
        parquet_path: Path,
        overwrite: bool = False
    ) -> Dict[str, int]:
        """
        Import facilities from entityidentity parquet file.

        Converts parquet rows to facility JSON format and writes to appropriate
        country directories. Skips existing facilities unless overwrite=True.

        Args:
            parquet_path: Path to entityidentity facilities parquet file
            overwrite: If True, overwrite existing facilities. Default False.

        Returns:
            Dictionary with import statistics:
            - imported: Number of facilities imported
            - skipped: Number of facilities skipped (already exist)
            - failed: Number of facilities that failed to import

        Example:
            >>> manager = FacilitySyncManager()
            >>> stats = manager.import_from_entityidentity(
            ...     Path("entityidentity/tables/facilities/facilities_20251003_134822.parquet")
            ... )
            >>> print(f"Imported: {stats['imported']}, Skipped: {stats['skipped']}")
        """
        logger.info(f"Starting import from {parquet_path}")

        if not parquet_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

        # Load parquet file
        df = pd.read_parquet(parquet_path)
        logger.info(f"Loaded {len(df)} facilities from parquet")

        imported = 0
        skipped = 0
        failed = 0

        for idx, row in df.iterrows():
            try:
                # Convert row to facility JSON
                facility = self._parquet_row_to_facility(row)
                facility_id = facility['facility_id']

                # Check if facility already exists
                if self._facility_exists(facility_id) and not overwrite:
                    logger.debug(f"Skipping existing facility: {facility_id}")
                    skipped += 1
                    continue

                # Determine country directory
                country_iso3 = facility['country_iso3']
                country_dir = self.facilities_dir / country_iso3
                country_dir.mkdir(parents=True, exist_ok=True)

                # Write facility JSON
                output_file = country_dir / f"{facility_id}.json"
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(facility, f, indent=2)

                logger.debug(f"Imported facility: {facility_id}")
                imported += 1

            except Exception as e:
                logger.error(f"Failed to import row {idx}: {e}")
                failed += 1
                continue

        stats = {
            'imported': imported,
            'skipped': skipped,
            'failed': failed
        }

        logger.info(f"Import complete: {imported} imported, {skipped} skipped, {failed} failed")
        return stats

    def _facility_to_parquet_row(self, facility: dict) -> dict:
        """
        Convert facility JSON to parquet row format.

        Args:
            facility: Facility dictionary in JSON schema format

        Returns:
            Dictionary matching entityidentity parquet schema
        """
        # Extract operator company_id
        company_id = None
        if facility.get('operator_link') and isinstance(facility['operator_link'], dict):
            company_id = facility['operator_link'].get('company_id')

        # Extract commodities as list
        commodities = []
        if facility.get('commodities'):
            commodities = [c['metal'] for c in facility['commodities']]

        # Extract evidence URLs and titles from sources
        evidence_urls = []
        evidence_titles = []
        if facility.get('sources'):
            for source in facility['sources']:
                if source.get('url'):
                    evidence_urls.append(source['url'])
                if source.get('id'):
                    evidence_titles.append(source['id'])

        # Get primary facility type
        facility_type = facility['types'][0] if facility.get('types') else None

        # Get process stages (use types as proxy)
        process_stages = facility.get('types', [])

        # Extract capacity information from products
        capacity_value = None
        capacity_unit = None
        capacity_asof = None
        if facility.get('products') and len(facility['products']) > 0:
            first_product = facility['products'][0]
            capacity_value = first_product.get('capacity')
            capacity_unit = first_product.get('unit')
            capacity_asof = first_product.get('year')

        # Use ISO3 directly (consistent with internal schema)
        country_iso3 = facility['country_iso3']

        # Get country name
        try:
            country_obj = pycountry.countries.get(alpha_3=country_iso3)
            country_name = country_obj.name if country_obj else country_iso3
        except:
            country_name = country_iso3

        # Determine if verified
        verification_status = facility.get('verification', {}).get('status', '')
        is_verified = verification_status in ['human_verified', 'llm_verified']

        # Get source type
        source = None
        if facility.get('sources') and len(facility['sources']) > 0:
            source = facility['sources'][0].get('type', 'unknown')

        # Build parquet row
        row = {
            'facility_id': facility['facility_id'],
            'company_id': company_id,
            'facility_name': facility['name'],
            'alt_names': facility.get('aliases', []),
            'facility_type': facility_type,
            'country': country_name,
            'country_iso3': country_iso3,
            'admin1': None,  # Not in current schema
            'city': None,    # Not in current schema
            'address': None, # Not in current schema
            'lat': facility.get('location', {}).get('lat'),
            'lon': facility.get('location', {}).get('lon'),
            'geo_precision': facility.get('location', {}).get('precision', 'unknown'),
            'commodities': ';'.join(commodities) if commodities else None,  # Semi-colon separated
            'process_stages': ';'.join(process_stages) if process_stages else None,
            'capacity_value': capacity_value,
            'capacity_unit': capacity_unit,
            'capacity_asof': capacity_asof,
            'operating_status': facility.get('status', 'unknown'),
            'evidence_urls': ';'.join(evidence_urls) if evidence_urls else '',
            'evidence_titles': ';'.join(evidence_titles) if evidence_titles else None,
            'confidence': facility.get('verification', {}).get('confidence', 0.5),
            'is_verified': is_verified,
            'verification_notes': facility.get('verification', {}).get('notes'),
            'first_seen_utc': facility.get('verification', {}).get('last_checked'),
            'last_seen_utc': facility.get('verification', {}).get('last_checked'),
            'source': source
        }

        return row

    def _parquet_row_to_facility(self, row: pd.Series) -> dict:
        """
        Convert parquet row to facility JSON format.

        Args:
            row: Pandas Series representing a parquet row

        Returns:
            Dictionary in facility JSON schema format
        """
        # Parse commodities from semi-colon separated string
        commodities = []
        if pd.notna(row.get('commodities')) and row['commodities']:
            metals = str(row['commodities']).split(';')
            commodities = [
                {
                    'metal': metal.strip(),
                    'primary': idx == 0  # First is primary
                }
                for idx, metal in enumerate(metals) if metal.strip()
            ]

        # Parse facility types from process_stages
        types = []
        if pd.notna(row.get('process_stages')) and row['process_stages']:
            types = [t.strip() for t in str(row['process_stages']).split(';') if t.strip()]
        elif pd.notna(row.get('facility_type')):
            types = [str(row['facility_type'])]

        # Ensure at least one type
        if not types:
            types = ['mine']  # Default fallback

        # Get ISO3 country code (prefer country_iso3, fallback to country_iso2 conversion, then country name)
        country_iso3 = None
        if pd.notna(row.get('country_iso3')):
            country_iso3 = str(row['country_iso3'])
        elif pd.notna(row.get('country_iso2')):
            # Legacy support: convert ISO2 to ISO3 if present
            try:
                country_iso3 = iso2_to_iso3(str(row['country_iso2']))
            except ValueError:
                pass

        # If still no ISO3, try to resolve from country name
        if not country_iso3 and pd.notna(row.get('country')):
            try:
                country_obj = pycountry.countries.search_fuzzy(str(row['country']))[0]
                country_iso3 = country_obj.alpha_3
            except:
                country_iso3 = 'UNK'

        if not country_iso3:
            country_iso3 = 'UNK'

        # Parse evidence URLs and titles
        sources = []
        if pd.notna(row.get('evidence_urls')) and row['evidence_urls']:
            urls = str(row['evidence_urls']).split(';')
            titles = []
            if pd.notna(row.get('evidence_titles')) and row['evidence_titles']:
                titles = str(row['evidence_titles']).split(';')

            for idx, url in enumerate(urls):
                if url.strip():
                    source = {
                        'type': 'web',
                        'url': url.strip()
                    }
                    if idx < len(titles) and titles[idx].strip():
                        source['id'] = titles[idx].strip()
                    source['date'] = datetime.now().isoformat()
                    sources.append(source)

        # Add source type if available
        if pd.notna(row.get('source')):
            sources.append({
                'type': str(row['source']),
                'id': f"Imported from entityidentity parquet",
                'date': datetime.now().isoformat()
            })

        if not sources:
            sources = [{
                'type': 'manual',
                'id': 'Imported from entityidentity',
                'date': datetime.now().isoformat()
            }]

        # Build operator link
        operator_link = None
        if pd.notna(row.get('company_id')) and row['company_id']:
            operator_link = {
                'company_id': str(row['company_id']),
                'confidence': float(row.get('confidence', 0.75))
            }

        # Build products if capacity data available
        products = []
        if pd.notna(row.get('capacity_value')) and row['capacity_value']:
            products.append({
                'stream': commodities[0]['metal'] if commodities else 'unknown',
                'capacity': float(row['capacity_value']) if pd.notna(row['capacity_value']) else None,
                'unit': str(row['capacity_unit']) if pd.notna(row.get('capacity_unit')) else None,
                'year': int(row['capacity_asof']) if pd.notna(row.get('capacity_asof')) else None
            })

        # Determine verification status
        is_verified = bool(row.get('is_verified', False))
        verification_status = 'human_verified' if is_verified else 'llm_suggested'

        # Handle aliases (may be list, array, or None)
        aliases = []
        alt_names_val = row.get('alt_names')
        if alt_names_val is not None:
            # Check if it's a simple type first
            if isinstance(alt_names_val, str):
                aliases = [alt_names_val] if alt_names_val else []
            elif isinstance(alt_names_val, (list, tuple)):
                aliases = list(alt_names_val)
            elif hasattr(alt_names_val, 'tolist'):  # numpy array
                try:
                    aliases = alt_names_val.tolist()
                except:
                    aliases = []

        # Build facility
        facility = {
            'facility_id': str(row['facility_id']),
            'name': str(row['facility_name']),
            'aliases': aliases,
            'country_iso3': country_iso3,
            'location': {
                'lat': float(row['lat']) if pd.notna(row.get('lat')) else None,
                'lon': float(row['lon']) if pd.notna(row.get('lon')) else None,
                'precision': str(row.get('geo_precision', 'unknown'))
            },
            'types': types,
            'commodities': commodities,
            'status': str(row.get('operating_status', 'unknown')),
            'owner_links': [],  # Not preserved in parquet format
            'operator_link': operator_link,
            'products': products,
            'sources': sources,
            'verification': {
                'status': verification_status,
                'confidence': float(row.get('confidence', 0.75)),
                'last_checked': str(row.get('last_seen_utc', datetime.now().isoformat())),
                'checked_by': 'entityidentity_import',
                'notes': str(row['verification_notes']) if pd.notna(row.get('verification_notes')) else None
            }
        }

        return facility

    def _facility_exists(self, facility_id: str) -> bool:
        """
        Check if a facility JSON file already exists.

        Args:
            facility_id: Facility identifier (e.g., "usa-bingham-canyon-fac" or "test-sample-mine-fac")

        Returns:
            True if facility exists, False otherwise
        """
        # Search for the facility file in all country directories
        # This handles cases where facility_id prefix doesn't match directory name
        for country_dir in self.facilities_dir.iterdir():
            if not country_dir.is_dir():
                continue

            facility_file = country_dir / f"{facility_id}.json"
            if facility_file.exists():
                return True

        return False
