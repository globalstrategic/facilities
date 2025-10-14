#!/usr/bin/env python3
"""
One-command facility import from research reports.

This unified script extracts tables from a research report and imports facilities
into the database with automatic duplicate detection - all in one pipeline.

Usage:
    # Interactive mode (paste your report)
    python import_from_report.py --country DZA --source "Algeria Report 2025"
    [Paste report text, then Ctrl+D]

    # From file
    python import_from_report.py report.txt --country AFG --source "Afghanistan Report 2025"

    # From stdin
    cat report.txt | python import_from_report.py --country DZA --source "Algeria Report"
"""

import re
import json
import sys
import argparse
import pathlib
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

# Paths
ROOT = pathlib.Path(__file__).parent.parent
FACILITIES_DIR = ROOT / "facilities"
IMPORT_LOGS_DIR = ROOT / "output" / "import_logs"

# Create directories
for dir_path in [FACILITIES_DIR, IMPORT_LOGS_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Normalization maps
METAL_NORMALIZE_MAP = {
    "aluminium": "aluminum", "ferronickel": "nickel", "ferromanganese": "manganese",
    "chromite": "chromium", "pgm": "platinum", "pge": "platinum",
    "ree": "rare earths", "rare earth elements": "rare earths",
    "fe": "iron", "cu": "copper", "au": "gold", "ag": "silver",
    "zn": "zinc", "pb": "lead", "ni": "nickel", "co": "cobalt",
    "li": "lithium", "u": "uranium", "hg": "mercury",
    "gemstones": "precious stones", "lapis lazuli": "precious stones",
    "ruby": "precious stones", "emerald": "precious stones",
    "barium": "baryte", "barite": "baryte"
}

TYPE_MAP = {
    "open pit mine": "mine", "underground mine": "mine", "surface mine": "mine",
    "coal mine": "mine", "deposit": "mine", "quarry": "mine",
    "smelter": "smelter", "refinery": "refinery",
    "processing facility": "plant", "processing complex": "plant",
    "steel complex": "plant", "steelworks": "plant",
    "cement plant": "plant", "cement factory": "plant",
    "fertilizer complex": "plant", "concentrator": "concentrator"
}

STATUS_MAP = {
    "operational": "operating", "operating": "operating", "active": "operating",
    "in development": "construction", "under construction": "construction",
    "proposed": "planned", "planned": "planned", "contracted": "planned",
    "closed": "closed", "inactive": "closed", "suspended": "suspended",
    "stalled": "suspended", "relaunching": "planned", "undeveloped": "planned"
}


def slugify(text: str) -> str:
    """Convert text to URL-safe slug."""
    if not text:
        return ""
    text = text.lower().strip()
    text = re.sub(r'\([^)]*\)', '', text)  # Remove parentheticals
    text = re.sub(r'[^a-z0-9]+', '-', text)
    return text.strip('-')


def extract_markdown_tables(text: str) -> List[Dict]:
    """Extract all markdown tables from text (supports both | and tab-separated)."""
    tables = []
    lines = text.split('\n')

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Check for pipe-separated tables
        if '|' in line and line.count('|') >= 2:
            table_lines = []
            while i < len(lines):
                line = lines[i].strip()
                if '|' in line and line.count('|') >= 2:
                    table_lines.append(line)
                    i += 1
                elif line == '':
                    i += 1
                    if i < len(lines) and '|' in lines[i]:
                        continue
                    else:
                        break
                else:
                    break

            if len(table_lines) >= 2:
                table = parse_markdown_table(table_lines, separator='|')
                if table and is_facility_table(table):
                    tables.append(table)

        # Check for tab-separated tables
        elif '\t' in line and line.count('\t') >= 2:
            table_lines = []
            # Collect all tab lines - stop when we see a clear section break
            consecutive_non_tab = 0
            while i < len(lines):
                line = lines[i]  # Don't strip - tabs matter
                if '\t' in line and line.count('\t') >= 2:
                    table_lines.append(line)
                    consecutive_non_tab = 0
                    i += 1
                else:
                    # Line without tabs - could be multi-line notes or end of table
                    # Check if this looks like a section header (capitalized, not just notes)
                    line_stripped = line.strip()
                    is_header = (line_stripped and
                                len(line_stripped) > 10 and
                                line_stripped[0].isupper() and
                                not line_stripped.startswith('Historic') and
                                not line_stripped.startswith('One of') and
                                not line_stripped.startswith('Part of') and
                                not line_stripped.startswith('A ') and
                                ':' not in line_stripped)

                    if is_header and consecutive_non_tab >= 1:
                        # Looks like a new section starting, stop table
                        break

                    consecutive_non_tab += 1
                    i += 1
                    # If we see many lines without tabs, assume table ended
                    if consecutive_non_tab >= 5:
                        break

            if len(table_lines) >= 2:
                table = parse_markdown_table(table_lines, separator='\t')
                if table and is_facility_table(table):
                    tables.append(table)
        else:
            i += 1

    return tables


def parse_markdown_table(lines: List[str], separator: str = '|') -> Optional[Dict]:
    """Parse markdown table from lines (supports | or tab separator)."""
    def split_row(line, sep):
        line = line.strip()
        if sep == '|':
            # Remove leading/trailing pipes
            if line.startswith('|'):
                line = line[1:]
            if line.endswith('|'):
                line = line[:-1]
        return [cell.strip() for cell in line.split(sep)]

    if len(lines) < 2:
        return None

    # First line is headers
    headers = split_row(lines[0], separator)

    # For pipe tables, skip separator line (---|----|---)
    # For tab tables, no separator line needed
    separator_idx = 1
    if separator == '|':
        while separator_idx < len(lines) and re.match(r'^[\s|:-]+$', lines[separator_idx]):
            separator_idx += 1
    # For tab-separated, start immediately at line 1 (no separator row)

    rows = []
    for line in lines[separator_idx:]:
        # Skip empty lines and separator lines
        if not line.strip():
            continue
        if separator == '|' and re.match(r'^[\s|:-]+$', line):
            continue

        # Must have the separator to be a valid row
        if separator not in line:
            continue

        row_data = split_row(line, separator)
        # Pad row if needed
        while len(row_data) < len(headers):
            row_data.append('')
        # Truncate if too long
        row_data = row_data[:len(headers)]
        rows.append(dict(zip(headers, row_data)))

    return {'headers': headers, 'rows': rows}


def is_facility_table(table: Dict) -> bool:
    """Check if table contains facility data."""
    if not table or 'headers' not in table:
        return False

    headers_lower = [h.lower() for h in table['headers']]
    indicators = ['site', 'mine', 'facility', 'name', 'deposit', 'project',
                 'latitude', 'longitude', 'commodity', 'metal', 'operator']

    matches = sum(1 for ind in indicators if any(ind in h for h in headers_lower))
    return matches >= 3


def normalize_headers(headers: List[str]) -> List[str]:
    """Normalize header names to standard schema."""
    normalized = []
    for h in headers:
        h_lower = h.lower().strip()

        if any(x in h_lower for x in ['site', 'mine name', 'facility name', 'asset name']):
            normalized.append('name')
        elif 'synonym' in h_lower or 'alias' in h_lower:
            normalized.append('synonyms')
        elif 'latitude' in h_lower or h_lower == 'lat':
            normalized.append('lat')
        elif 'longitude' in h_lower or h_lower == 'lon':
            normalized.append('lon')
        elif 'primary commodity' in h_lower:
            normalized.append('primary')
        elif 'other commodities' in h_lower or 'secondary commodity' in h_lower:
            normalized.append('other')
        elif 'asset type' in h_lower or 'facility type' in h_lower:
            normalized.append('types')
        elif 'status' in h_lower or 'operational' in h_lower:
            normalized.append('status')
        elif 'operator' in h_lower or 'stakeholder' in h_lower:
            normalized.append('operator')
        elif 'note' in h_lower:
            normalized.append('notes')
        else:
            normalized.append(h)

    return normalized


def find_country_code(country_input: str) -> str:
    """Find the actual country code used in the repository.

    Checks if a directory exists for this country (tries both as-is, upper, and variations).
    Returns the actual directory name if found, otherwise returns input.
    """
    # Try exact match first
    if (FACILITIES_DIR / country_input).exists():
        return country_input

    # Try uppercase
    upper = country_input.upper()
    if (FACILITIES_DIR / upper).exists():
        return upper

    # For common conversions (DZA->DZ, USA->US, etc), try truncated version
    if len(country_input) == 3:
        short = country_input[:2].upper()
        if (FACILITIES_DIR / short).exists():
            logger.info(f"Found existing directory '{short}' for input '{country_input}'")
            return short

    # No existing directory found, use input as-is
    return country_input.upper()


def load_existing_facilities(country_iso3: str) -> Dict[str, Dict]:
    """Load existing facilities for duplicate detection."""
    existing = {}
    country_dir = FACILITIES_DIR / country_iso3
    if not country_dir.exists():
        return existing

    for facility_file in country_dir.glob("*.json"):
        try:
            with open(facility_file, 'r') as f:
                facility = json.load(f)
                existing[facility['facility_id']] = facility
        except Exception as e:
            logger.debug(f"Could not load {facility_file}: {e}")

    return existing


def check_duplicate(facility_id: str, name: str, lat: Optional[float], lon: Optional[float],
                   existing: Dict[str, Dict]) -> Optional[str]:
    """Check if facility already exists. Returns existing ID if duplicate."""
    # Exact ID match
    if facility_id in existing:
        return facility_id

    # Name + location match
    for existing_id, existing_fac in existing.items():
        # Name match
        if name.lower() == existing_fac['name'].lower():
            # Check location if both have coords
            if lat and lon and existing_fac['location'].get('lat') and existing_fac['location'].get('lon'):
                lat_diff = abs(lat - existing_fac['location']['lat'])
                lon_diff = abs(lon - existing_fac['location']['lon'])
                if lat_diff < 0.01 and lon_diff < 0.01:  # Within ~1km
                    return existing_id
            else:
                # No coords to compare, assume duplicate by name
                return existing_id

        # Check aliases
        if name.lower() in [a.lower() for a in existing_fac.get('aliases', [])]:
            return existing_id

    return None


def normalize_metal(metal: str) -> str:
    """Normalize metal name."""
    metal_lower = metal.lower().strip()
    return METAL_NORMALIZE_MAP.get(metal_lower, metal_lower)


def parse_commodities(primary: str, other: str) -> List[Dict]:
    """Parse commodities from primary and other strings."""
    commodities = []
    seen = set()

    if primary:
        for metal in re.split(r'[,;]', primary):
            metal = normalize_metal(metal.strip())
            if metal and metal not in seen and metal != '-':
                commodities.append({"metal": metal, "primary": True})
                seen.add(metal)

    if other:
        for metal in re.split(r'[,;]', other):
            metal = normalize_metal(metal.strip())
            if metal and metal not in seen and metal != '-':
                commodities.append({"metal": metal, "primary": False})
                seen.add(metal)

    return commodities


def parse_types(type_str: str) -> List[str]:
    """Parse and normalize facility types."""
    if not type_str or type_str.strip() == '-':
        return ["mine"]

    types = []
    for t in re.split(r'[,;]', type_str.lower()):
        t = t.strip()
        normalized = TYPE_MAP.get(t, t)
        if normalized and normalized not in types:
            types.append(normalized)

    return types if types else ["mine"]


def parse_status(status_str: str) -> str:
    """Parse and normalize status."""
    if not status_str or status_str.strip() == '-':
        return "unknown"

    # Extract status keyword from longer descriptions
    status_match = re.search(
        r'\b(operational|operating|active|construction|proposed|planned|closed|inactive|suspended|stalled|undeveloped|relaunching)\b',
        status_str.lower()
    )
    if status_match:
        return STATUS_MAP.get(status_match.group(1), "unknown")

    return STATUS_MAP.get(status_str.lower().strip(), "unknown")


def process_report(report_text: str, country_iso3: str, source_name: str) -> Dict:
    """Main pipeline: extract tables and import facilities."""
    logger.info(f"Processing report for {country_iso3}...")

    # Extract tables
    logger.info("Extracting facility tables from report...")
    tables = extract_markdown_tables(report_text)
    logger.info(f"Found {len(tables)} facility tables")

    if not tables:
        logger.error("No facility tables found in report")
        logger.error("Check that your report contains tables with | or tab separators")
        logger.error("Tables must have headers like: Name, Location, Commodity, etc.")
        return {"error": "No tables found", "facilities": []}

    # Load existing facilities
    existing = load_existing_facilities(country_iso3)
    logger.info(f"Loaded {len(existing)} existing facilities for duplicate detection")

    # Process all rows from all tables
    facilities = []
    duplicates = []
    errors = []
    stats = defaultdict(int)

    row_num = 0
    for table in tables:
        headers = normalize_headers(table['headers'])

        for row_data in table['rows']:
            row_num += 1

            # Map to normalized headers
            row = {}
            for i, (orig_header, norm_header) in enumerate(zip(table['headers'], headers)):
                if i < len(row_data):
                    row[norm_header] = list(row_data.values())[i]

            try:
                # Extract name
                name = row.get('name', '').strip()
                if not name or name == '-':
                    continue

                # Generate facility ID
                facility_id = f"{country_iso3.lower()}-{slugify(name)}-fac"

                # Parse coordinates - handle both separate lat/lon and combined format
                lat, lon = None, None
                try:
                    # Try separate lat/lon fields first
                    if row.get('lat') and row.get('lat').strip() and row.get('lat').strip() != '-':
                        lat = float(row['lat'].strip())
                    if row.get('lon') and row.get('lon').strip() and row.get('lon').strip() != '-':
                        lon = float(row['lon'].strip())

                    # If no lat/lon, try combined "Coordinates" field
                    if not lat and not lon:
                        coords = row.get('Coordinates (Lat, Lon)', '').strip()
                        if coords and coords != '-' and coords.lower() != 'not specified':
                            # Format: "35.849, 7.118" or similar
                            parts = [p.strip() for p in coords.replace(',', ' ').split()]
                            if len(parts) >= 2:
                                try:
                                    lat = float(parts[0])
                                    lon = float(parts[1])
                                except (ValueError, IndexError):
                                    pass
                except (ValueError, AttributeError):
                    pass

                # Check for duplicate
                existing_id = check_duplicate(facility_id, name, lat, lon, existing)
                if existing_id:
                    logger.info(f"  Duplicate: '{name}' already exists as {existing_id}")
                    duplicates.append({
                        "row": row_num,
                        "name": name,
                        "existing_id": existing_id
                    })
                    stats['duplicates_skipped'] += 1
                    continue

                # Parse data
                types = parse_types(row.get('types', ''))
                commodities = parse_commodities(row.get('primary', ''), row.get('other', ''))
                status = parse_status(row.get('status', ''))

                aliases_str = row.get('synonyms', '').strip()
                aliases = [a.strip() for a in aliases_str.split(',') if a.strip() and a.strip() != '-']

                notes = row.get('notes', '').strip()
                if notes and notes != '-':
                    notes = notes[:500]
                else:
                    notes = None

                # Build facility
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
                    "status": status,
                    "owner_links": [],
                    "operator_link": None,
                    "products": [],
                    "sources": [{
                        "type": "gemini_research",
                        "id": source_name,
                        "date": datetime.now().isoformat()
                    }],
                    "verification": {
                        "status": "llm_suggested",
                        "confidence": 0.75,
                        "last_checked": datetime.now().isoformat(),
                        "checked_by": "import_pipeline",
                        "notes": notes
                    }
                }

                facilities.append(facility)
                stats['total_facilities'] += 1

            except Exception as e:
                logger.error(f"Row {row_num}: Error parsing: {e}")
                errors.append(f"Row {row_num}: {str(e)}")

    logger.info(f"Processed {row_num} rows")
    logger.info(f"Found {stats['total_facilities']} new facilities")
    logger.info(f"Skipped {stats['duplicates_skipped']} duplicates")

    return {
        "facilities": facilities,
        "duplicates": duplicates,
        "errors": errors,
        "stats": stats
    }


def write_facilities(facilities: List[Dict], country_iso3: str) -> int:
    """Write facility JSON files."""
    country_dir = FACILITIES_DIR / country_iso3
    country_dir.mkdir(parents=True, exist_ok=True)

    written = 0
    for facility in facilities:
        facility_file = country_dir / f"{facility['facility_id']}.json"
        with open(facility_file, 'w', encoding='utf-8') as f:
            json.dump(facility, f, ensure_ascii=False, indent=2)
        written += 1

    return written


def write_report(result: Dict, country_iso3: str, source_name: str):
    """Write import report."""
    report = {
        "timestamp": datetime.now().isoformat(),
        "source": source_name,
        "country_iso3": country_iso3,
        "statistics": dict(result['stats']),
        "errors": result['errors'],
        "duplicates_found": result['duplicates'],
        "summary": {
            "new_facilities": result['stats']['total_facilities'],
            "duplicates_skipped": result['stats']['duplicates_skipped'],
            "files_written": len(result['facilities']),
            "errors": len(result['errors'])
        }
    }

    report_file = IMPORT_LOGS_DIR / f"import_report_{country_iso3}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    logger.info(f"Report written to {report_file}")
    return report


def main():
    parser = argparse.ArgumentParser(
        description="One-command facility import from research reports",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage - save report to file first
  python import_from_report.py report.txt --country DZA

  # With optional source name
  python import_from_report.py report.txt --country DZA --source "Algeria Report 2025"

  # Quick save and import
  cat > report.txt
  [Paste text, Ctrl+D]
  python import_from_report.py report.txt --country AFG

  # From stdin (pipe)
  pbpaste | python import_from_report.py --country DZA
        """
    )
    parser.add_argument("input_file", help="Input report file (required)")
    parser.add_argument("--country", required=True, help="Country ISO3 code (e.g., DZA, AFG)")
    parser.add_argument("--source", help="Source name for citation (optional, auto-generated if not provided)")

    args = parser.parse_args()

    # Find actual country code used in repo (handles DZA->DZ, etc)
    country_iso3 = find_country_code(args.country)

    # Auto-generate source name if not provided
    if not args.source:
        source_name = f"Research Import {country_iso3} {datetime.now().strftime('%Y-%m-%d')}"
    else:
        source_name = args.source

    # Read input file
    if args.input_file == '-':
        # Read from stdin if file is '-'
        logger.info("Reading from stdin...")
        report_text = sys.stdin.read()
        if not report_text.strip():
            print("Error: No input provided on stdin")
            return 1
    else:
        input_path = pathlib.Path(args.input_file)
        if not input_path.exists():
            print(f"Error: File not found: {input_path}")
            print(f"\nTip: Save your report to a file first:")
            print(f"  cat > {args.input_file}")
            print(f"  [Paste text, then press Ctrl+D]")
            return 1
        with open(input_path, 'r', encoding='utf-8') as f:
            report_text = f.read()
        logger.info(f"Read report from {input_path} ({len(report_text)} chars)")

        # Warn if report seems very small (likely incomplete)
        if len(report_text) < 1000:
            logger.warning(f"Report is only {len(report_text)} characters - this seems small. Did the paste work correctly?")

    # Process
    result = process_report(report_text, country_iso3, source_name)

    if 'error' in result:
        print(f"\nError: {result['error']}")
        return 1

    # Write files
    if result['facilities']:
        written = write_facilities(result['facilities'], country_iso3)
        logger.info(f"Wrote {written} facility files")
    else:
        logger.warning("No new facilities to write (all may be duplicates)")

    # Write report
    report = write_report(result, country_iso3, source_name)

    # Print summary
    print("\n" + "="*60)
    print("IMPORT COMPLETE")
    print("="*60)
    print(f"Country: {country_iso3}")
    print(f"Source: {args.source}")
    print(f"New facilities: {report['summary']['new_facilities']}")
    print(f"Duplicates skipped: {report['summary']['duplicates_skipped']}")
    print(f"Files written: {report['summary']['files_written']}")
    if report['summary']['errors']:
        print(f"Errors: {report['summary']['errors']}")
    print("="*60)

    if result['duplicates']:
        print(f"\nDuplicates found (skipped {len(result['duplicates'])} existing facilities):")
        for dup in result['duplicates'][:5]:  # Show first 5
            print(f"  - '{dup['name']}' (exists as {dup['existing_id']})")
        if len(result['duplicates']) > 5:
            print(f"  ... and {len(result['duplicates']) - 5} more")

    return 0


if __name__ == "__main__":
    sys.exit(main())
