#!/usr/bin/env python3
"""
Facility import from CSV/reports with Phase 2 company resolution pattern.

Phase 1 (This Script):
- Extract facility data from CSV files or markdown tables
- Normalize metals with chemical formulas via metal_identifier()
- Extract company mentions (NO resolution yet)
- Parse "Group Names" column to detect companies vs aliases
- Support per-row country detection (for Mines.csv)
- Write facility JSONs with company_mentions[] array

Phase 2 (enrich_companies.py):
- Resolve company mentions to canonical IDs using CompanyResolver
- Apply quality gates (auto_accept / review / pending)
- Write relationships to facility_company_relationships.parquet

Usage:
    # Import from Mines.csv with per-row countries
    python import_from_report.py gt/Mines.csv --source "Mines.csv Initial Load"

    # Import with explicit country
    python import_from_report.py report.txt --country DZA

    # Auto-detect country from filename
    python import_from_report.py algeria_report.txt

    # From stdin (requires --country)
    cat report.txt | python import_from_report.py --country DZA

Requirements:
    - entityidentity library for metal_identifier()
    - scripts.utils.country_utils for country normalization
"""

import re
import json
import sys
import argparse
import pathlib
import csv
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

# Normalization maps (fallback)
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
    "operational (old)": "operating",  # South Africa report format
    "in development": "construction", "under construction": "construction",
    "executable": "construction",  # South Africa report format
    "proposed": "planned", "planned": "planned", "contracted": "planned",
    "feasibility": "planned", "pre-feasibility": "planned", "bankable": "planned",  # Study phases
    "closed": "closed", "inactive": "closed", "completed": "closed",
    "suspended": "suspended", "stalled": "suspended",
    "care and maintenance": "suspended",  # South Africa report format
    "dormant": "suspended", "dormant (l/r)": "suspended",  # License relinquished
    "cancelled": "closed", "grassroots": "planned"  # Exploration phase
}


def slugify(text: str) -> str:
    """Convert text to URL-safe slug."""
    if not text:
        return ""
    text = text.lower().strip()
    text = re.sub(r'\([^)]*\)', '', text)  # Remove parentheticals
    text = re.sub(r'[^a-z0-9]+', '-', text)
    return text.strip('-')


def parse_csv_file(text: str) -> Optional[Dict]:
    """Parse CSV text into table format.

    Returns:
        Dictionary with 'headers' and 'rows' keys, or None if not valid CSV
    """
    try:
        # Try to parse as CSV
        lines = text.strip().split('\n')
        if not lines:
            return None

        # Use csv.DictReader to handle proper CSV parsing
        reader = csv.DictReader(lines)
        rows = list(reader)

        if not rows:
            return None

        # Get headers from the first row keys
        headers = list(rows[0].keys())

        # Convert to the format expected by the rest of the pipeline
        return {
            'headers': headers,
            'rows': rows
        }
    except Exception as e:
        logger.debug(f"Failed to parse as CSV: {e}")
        return None


def extract_concatenated_table(text: str) -> Optional[Dict]:
    """Extract table where entries are concatenated without separators.

    Example format from South Africa report:
    Mine/Project NameStatusPrimary Commodity
    Acid Mine Drainage - Long Term Solution (Amdlts)OperationalIndustrialAcorn Coal ProjectDormant (L/R)Coal...

    Returns a dict with 'headers' and 'rows' keys if successful.
    """
    # Look for the specific table header pattern
    header_match = re.search(r'Mine/Project Name\s*Status\s*Primary Commodity', text)
    if not header_match:
        return None

    # Find the end of the table (before next major section)
    table_start = header_match.end()
    end_patterns = [
        r'Sector Analysis',
        r'Corporate Portfolio',
        r'Concluding Analysis',
        r'\n\n[A-Z][a-z]+\s+[A-Z][a-z]+.*:',  # Section headers like "The Pillars of..."
    ]

    table_end = len(text)
    for pattern in end_patterns:
        match = re.search(pattern, text[table_start:])
        if match:
            table_end = table_start + match.start()
            break

    table_content = text[table_start:table_end].strip()
    if not table_content:
        return None

    # Status keywords that help identify boundaries
    status_keywords = [
        'Operational', 'Dormant (L/R)', 'Completed', 'Pre-Feasibility', 'Grassroots',
        'Operational (Old)', 'Cancelled', 'Executable', 'Care and Maintenance',
        'Feasibility', 'Closed', 'Bankable', 'Cancelled - Pre-Feasibility',
        'Cancelled - Grassroots', 'Cancelled - Feasibility', 'Cancelled - Executable'
    ]

    # Commodity keywords
    commodity_keywords = [
        'Industrial', 'Coal', 'Gold', 'PGM', 'Diamonds', 'Copper', 'Iron Ore',
        'Manganese', 'Nickel', 'Chrome', 'Uranium', 'Vanadium', 'Zinc', 'Lead',
        'Phosphate', 'Fluorspar', 'Rare Earths', 'Emeralds', 'Ilmenite', 'Rutile',
        'Tin', 'Silica'
    ]

    # Create regex pattern to split entries
    # Pattern: (NamePart)(Status)(Commodity)
    # Note: The table can be either concatenated OR newline-separated
    # Build combined pattern for status|commodity
    status_pattern = '|'.join(re.escape(s) for s in sorted(status_keywords, key=len, reverse=True))
    commodity_pattern = '|'.join(re.escape(c) for c in sorted(commodity_keywords, key=len, reverse=True))

    # Pattern to match: (Name)(Status)(Commodity) with optional whitespace/newlines
    # Use \s* to match across newlines
    entry_pattern = f'(.+?)\\s*({status_pattern})\\s*({commodity_pattern})'

    rows = []
    matches = list(re.finditer(entry_pattern, table_content, re.DOTALL))

    logger.info(f"Found {len(matches)} facility entries in concatenated table")

    for match in matches:
        name = match.group(1).strip()
        status = match.group(2).strip()
        commodity = match.group(3).strip()

        # Skip if name is too short (likely parsing error)
        if len(name) < 3:
            continue

        # Clean up name: remove excess newlines and whitespace
        name = re.sub(r'\s+', ' ', name).strip()

        rows.append({
            'Name': name,
            'Status': status,
            'Primary Commodity': commodity
        })

    if not rows:
        return None

    return {
        'headers': ['Name', 'Status', 'Primary Commodity'],
        'rows': rows
    }


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


def extract_facilities_from_text(text: str) -> List[Dict]:
    """Extract facilities from narrative text, lists, and paragraphs.

    This parser handles facilities mentioned in non-table formats like:
    - "Letpadaung Mine: This is the largest..."
    - "Mawchi Mine (Kayah State): Historically..."
    - List items with facility names
    - Inline mentions in paragraphs

    Returns list of facility dictionaries with extracted attributes.
    """
    facilities = []
    lines = text.split('\n')

    # Track current section for commodity context
    current_commodity = None
    current_section = None

    # Patterns for facility mentions
    facility_patterns = [
        # Pattern 1: "Name Mine:" or "Name Project:" at start of line
        r'^([A-Z][^:\n]+?(?:Mine|Project|Complex|Plant|Refinery|Smelter|Deposit|Field|Quarry|Basin))\s*(?:\([^)]+\))?\s*:',
        # Pattern 2: "Name (Location):"
        r'^([A-Z][^:\n]+?)\s*\(([^)]+)\)\s*:',
        # Pattern 3: List items "- Name:" or "• Name:"
        r'^[\-•]\s*([A-Z][^:\n]+?(?:Mine|Project|Complex|Plant|Refinery|Smelter|Deposit|Field|Quarry))\s*(?:\([^)]+\))?\s*:',
    ]

    # Section header pattern to detect commodity context
    section_pattern = r'^(?:Section\s+)?(\d+\.?\d*)\s+(.+?)(?:\s*-\s*(.+?))?$'

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Check if this is a section header
        section_match = re.match(section_pattern, line)
        if section_match and len(line) < 100:
            current_section = section_match.group(2).strip()
            # Try to extract commodity from section title
            # e.g., "2.1 Copper" -> commodity = "copper"
            commodity_keywords = {
                'copper': 'copper', 'lead': 'lead', 'zinc': 'zinc',
                'tin': 'tin', 'tungsten': 'tungsten', 'nickel': 'nickel',
                'gold': 'gold', 'silver': 'silver', 'jade': 'jade',
                'ruby': 'precious stones', 'sapphire': 'precious stones',
                'iron': 'iron', 'coal': 'coal', 'rare earth': 'rare earths',
                'antimony': 'antimony', 'gas': 'natural gas', 'oil': 'petroleum',
                'cement': 'limestone', 'marble': 'marble'
            }
            for keyword, normalized in commodity_keywords.items():
                if keyword in current_section.lower():
                    current_commodity = normalized
                    logger.debug(f"Section: {current_section} -> commodity: {current_commodity}")
                    break
            i += 1
            continue

        # Check for facility mentions
        facility_match = None
        location_from_pattern = None

        for pattern in facility_patterns:
            match = re.match(pattern, line, re.IGNORECASE)
            if match:
                facility_match = match
                break

        if facility_match:
            # Extract facility name
            facility_name = facility_match.group(1).strip()

            # Try to extract location from name (e.g., "Name (Location)")
            location_match = re.search(r'\(([^)]+)\)', facility_name)
            if location_match:
                location_from_pattern = location_match.group(1)
                # Remove location from name
                facility_name = re.sub(r'\s*\([^)]+\)', '', facility_name).strip()
            elif facility_match.lastindex >= 2 and facility_match.group(2):
                location_from_pattern = facility_match.group(2)

            # Gather context from following lines (next 5-10 lines)
            context_lines = []
            j = i + 1
            while j < min(i + 10, len(lines)) and j < len(lines):
                context_line = lines[j].strip()
                # Stop at next facility or section
                if re.match(facility_patterns[0], context_line) or \
                   re.match(facility_patterns[1], context_line) or \
                   re.match(facility_patterns[2], context_line):
                    break
                if context_line:
                    context_lines.append(context_line)
                j += 1

            context_text = ' '.join(context_lines)

            # Extract attributes from context
            extracted_data = extract_facility_attributes(
                facility_name,
                context_text,
                location_from_pattern,
                current_commodity
            )

            if extracted_data:
                facilities.append(extracted_data)
                logger.debug(f"Extracted from text: {facility_name}")

            i = j  # Skip to after the context we just processed
        else:
            i += 1

    logger.info(f"Extracted {len(facilities)} facilities from text")
    return facilities


def extract_facility_attributes(name: str, context: str, location_hint: Optional[str],
                               commodity_hint: Optional[str]) -> Optional[Dict]:
    """Extract facility attributes from name and context text.

    Args:
        name: Facility name
        context: Surrounding text (next few lines)
        location_hint: Location extracted from name pattern
        commodity_hint: Commodity from section context

    Returns:
        Dictionary with extracted attributes or None if insufficient data
    """
    # Clean name
    name = re.sub(r'\s+', ' ', name).strip()
    if not name or len(name) < 3:
        return None

    # Blacklist: Filter out non-facility entities
    blacklist_patterns = [
        # Government agencies and organizations
        r'\b(?:department|ministry|council|agency|commission|authority|bureau|administration)\b',
        r'\b(?:government|federal|national|state|provincial)\s+(?:of|for)\b',
        # Generic/abstract terms (not specific facilities)
        r'^(?:platinum group metals?|precious metals?|base metals?|rare earths?|commodities?)$',
        r'^(?:gold|copper|iron|coal|diamonds?)$',  # Just commodity names
        # Corporate/business entities (not facilities)
        r'\b(?:corporation|company|enterprises?|holdings?|ltd|inc|plc|limited)\b',
        r'\b(?:services?|solutions?|consulting|investments?)\b',
        # Generic categories
        r'^(?:the\s+)?(?:mining|minerals?|resources?)\s+(?:industry|sector)$',
    ]

    name_lower = name.lower()
    for pattern in blacklist_patterns:
        if re.search(pattern, name_lower, re.IGNORECASE):
            logger.debug(f"Filtered out non-facility: '{name}' (matched pattern: {pattern})")
            return None

    # Extract location
    location_text = location_hint or ""

    # Look for location patterns in context if not in name
    if not location_text:
        location_patterns = [
            r'(?:located|situated|in)\s+(?:the\s+)?([A-Z][^,\.]+(?:Region|State|Province|District|Township|Area))',
            r'\(([A-Z][^)]+(?:Region|State|Province|District))\)',
        ]
        for pattern in location_patterns:
            match = re.search(pattern, context, re.IGNORECASE)
            if match:
                location_text = match.group(1).strip()
                break

    # Extract commodities
    commodities = set()
    if commodity_hint:
        commodities.add(commodity_hint.lower())

    # Look for commodity mentions in context
    commodity_patterns = {
        'copper': r'\bcopper\b', 'lead': r'\blead\b', 'zinc': r'\bzinc\b',
        'gold': r'\bgold\b', 'silver': r'\bsilver\b', 'tin': r'\btin\b',
        'tungsten': r'\btungsten\b', 'nickel': r'\bnickel\b', 'iron': r'\biron\b',
        'coal': r'\bcoal\b', 'jade': r'\bjade\b', 'ruby': r'\bruby\b',
        'rare earths': r'\brare earth', 'antimony': r'\bantimony\b',
        'natural gas': r'\bnatural gas\b|\bgas\b'
    }
    for metal, pattern in commodity_patterns.items():
        if re.search(pattern, context, re.IGNORECASE):
            commodities.add(metal)

    # Extract facility type from name
    type_keywords = {
        'mine': r'\bmine\b',
        'smelter': r'\bsmelter\b',
        'refinery': r'\brefinery\b',
        'plant': r'\bplant\b|\bfactory\b',
        'complex': r'\bcomplex\b',
        'deposit': r'\bdeposit\b',
        'project': r'\bproject\b',
        'quarry': r'\bquarry\b',
    }
    facility_types = []
    for ftype, pattern in type_keywords.items():
        if re.search(pattern, name, re.IGNORECASE):
            facility_types.append(ftype)
            break
    if not facility_types:
        facility_types = ['mine']  # Default

    # Extract status
    status = 'unknown'
    status_patterns = {
        'operating': r'\boperating\b|\boperational\b|\bactive\b|\bproducing\b',
        'closed': r'\bclosed\b|\binactive\b|\bshuttered\b',
        'construction': r'\bunder construction\b|\bdeveloping\b|\bin development\b',
        'planned': r'\bplanned\b|\bproposed\b|\bcontracted\b',
    }
    for status_val, pattern in status_patterns.items():
        if re.search(pattern, context, re.IGNORECASE):
            status = status_val
            break

    # Extract operator/owner mentions
    operator = None
    operator_patterns = [
        r'(?:operated by|operator|run by)\s+([A-Z][^,\.;]+?(?:Ltd|Limited|Co\.|Corporation|Inc|Company|Group|Enterprise)\.?)',
        r'(?:owned by|owner)\s+([A-Z][^,\.;]+?(?:Ltd|Limited|Co\.|Corporation|Inc|Company|Group|Enterprise)\.?)',
    ]
    for pattern in operator_patterns:
        match = re.search(pattern, context, re.IGNORECASE)
        if match:
            operator = match.group(1).strip()
            break

    # Build structured data
    return {
        'name': name,
        'location': location_text,
        'commodities': list(commodities),
        'types': facility_types,
        'status': status,
        'operator': operator,
        'notes': context[:200] if context else None  # Keep first 200 chars as notes
    }


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

    # Require at least 2 columns for a valid facility table
    # Single-column tables are likely mis-parsed concatenated formats
    if len(table['headers']) < 2:
        return False

    headers_lower = [h.lower() for h in table['headers']]
    indicators = ['site', 'mine', 'facility', 'name', 'deposit', 'project',
                 'latitude', 'longitude', 'commodity', 'metal', 'operator']

    matches = sum(1 for ind in indicators if any(ind in h for h in headers_lower))
    return matches >= 3


def parse_group_names(group_names_str: str, source_name: str) -> Tuple[List[str], List[Dict]]:
    """Parse Group Names column from Mines.csv into aliases and company mentions.

    The Group Names column contains semicolon-separated values that are a mix of:
    - Simple aliases (e.g., "Marmato", "Zona Baja")
    - Company names (e.g., "Omai Gold Mines Ltd.")

    Args:
        group_names_str: Semicolon-separated string from Group Names column
        source_name: Source name for company_mentions provenance

    Returns:
        Tuple of (aliases_list, company_mentions_list)
    """
    if not group_names_str or group_names_str.strip() in ('', '-'):
        return ([], [])

    aliases = []
    company_mentions = []

    # Split by semicolon
    parts = [p.strip() for p in group_names_str.split(';') if p.strip()]

    # Company indicators
    company_patterns = [
        r'\b(Ltd|Limited|LLC|Inc|Incorporated|Corp|Corporation|Co\.|Company|Group|Enterprise|Holdings|PLC|S\.A\.|GmbH|AG)\b',
        r'\b(Mining|Mines|Resources|Minerals|Metals|Industries|Energy)\b.*\b(Ltd|Limited|Inc|Corporation|Co\.|Company)\b'
    ]

    for part in parts:
        # Check if this looks like a company name
        is_company = any(re.search(pattern, part, re.IGNORECASE) for pattern in company_patterns)

        if is_company:
            # This is a company - add to company_mentions
            company_mentions.append({
                "name": part,
                "role": "owner",  # Default to owner since Group Names typically indicates ownership
                "source": source_name,
                "confidence": 0.60,  # Lower confidence - needs Phase 2 resolution
                "first_seen": datetime.now().isoformat(),
                "evidence": f"Extracted from Group Names column: {group_names_str}"
            })
        else:
            # This is an alias
            if part and part != '-':
                aliases.append(part)

    return (aliases, company_mentions)


def normalize_headers(headers: List[str]) -> List[str]:
    """Normalize header names to standard schema."""
    normalized = []
    for h in headers:
        h_lower = h.lower().strip()

        if any(x in h_lower for x in ['site', 'mine name', 'facility name', 'asset name']) or h_lower == 'name':
            normalized.append('name')
        elif 'group name' in h_lower:  # Map "Group Names" from Mines.csv
            normalized.append('group_names')
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
        elif 'confidence factor' in h_lower:  # Map confidence from Mines.csv
            normalized.append('confidence_factor')
        elif 'country' in h_lower or 'region' in h_lower:  # Map country column
            normalized.append('country')
        elif 'operator' in h_lower or 'stakeholder' in h_lower:
            normalized.append('operator')
        elif 'note' in h_lower:
            normalized.append('notes')
        else:
            normalized.append(h)

    return normalized


def find_country_code(country_input: str) -> Tuple[str, str]:
    """Find the actual country code and directory used in the repository.

    Uses country_utils to normalize to ISO3, then checks which directory exists.

    Returns:
        Tuple of (iso3_code, directory_name)
        e.g., ("DZA", "DZ") or ("USA", "USA")
    """
    # First, normalize to ISO3 using country_utils
    try:
        sys.path.insert(0, str(pathlib.Path(__file__).parent))
        from utils.country_utils import normalize_country_to_iso3
        iso3 = normalize_country_to_iso3(country_input)
    except Exception as e:
        logger.warning(f"Could not use country_utils: {e}, falling back")
        iso3 = country_input.upper()

    # Now find which directory exists (could be ISO3 or ISO2)
    # Try exact ISO3
    if (FACILITIES_DIR / iso3).exists():
        return (iso3, iso3)

    # Try ISO2 (first 2 chars)
    if len(iso3) == 3:
        iso2 = iso3[:2]
        if (FACILITIES_DIR / iso2).exists():
            logger.info(f"Using directory '{iso2}' for country {iso3}")
            return (iso3, iso2)

    # Try uppercase input as-is
    upper = country_input.upper()
    if (FACILITIES_DIR / upper).exists():
        return (iso3, upper)

    # No existing directory found, create with ISO3
    logger.info(f"No existing directory found, will create {iso3}")
    return (iso3, iso3)


def load_existing_facilities(country_dir_name: str) -> Dict[str, Dict]:
    """Load existing facilities for duplicate detection.

    Args:
        country_dir_name: Directory name (could be ISO2 or ISO3)
    """
    existing = {}
    country_dir = FACILITIES_DIR / country_dir_name
    if not country_dir.exists():
        return existing

    for facility_file in country_dir.glob("*.json"):
        # Skip backup files
        if '.backup_' in facility_file.name:
            continue

        try:
            with open(facility_file, 'r') as f:
                facility = json.load(f)
                existing[facility['facility_id']] = facility
        except Exception as e:
            logger.debug(f"Could not load {facility_file}: {e}")

    return existing


def check_duplicate(facility_id: str, name: str, lat: Optional[float], lon: Optional[float],
                   existing: Dict[str, Dict]) -> Optional[str]:
    """Check if facility already exists. Returns existing ID if duplicate.

    Strategy:
    1. Exact ID match
    2. Coordinate-based matching (primary - within 0.5 degrees ~55km)
    3. Exact name match
    4. Fuzzy name match (>85% similarity)
    5. Alias match
    """
    from difflib import SequenceMatcher

    # Exact ID match
    if facility_id in existing:
        return facility_id

    # PRIORITY 1: Coordinate-based matching (most reliable)
    # Check if new facility has coords
    if lat is not None and lon is not None:
        for existing_id, existing_fac in existing.items():
            existing_lat = existing_fac['location'].get('lat')
            existing_lon = existing_fac['location'].get('lon')

            if existing_lat is not None and existing_lon is not None:
                lat_diff = abs(lat - existing_lat)
                lon_diff = abs(lon - existing_lon)

                name_lower = name.lower()
                existing_name_lower = existing_fac['name'].lower()
                name_similarity = SequenceMatcher(None, name_lower, existing_name_lower).ratio()

                # Check if one name contains the other (for "Two Rivers" vs "Two Rivers Platinum Mine")
                shorter = name_lower if len(name_lower) < len(existing_name_lower) else existing_name_lower
                longer = existing_name_lower if len(name_lower) < len(existing_name_lower) else name_lower
                contains_match = shorter in longer

                # Two-tier matching:
                # Tier 1: Very close coords (0.01 deg ~1km) + moderate name match (>0.6 OR contains)
                # Tier 2: Close coords (0.1 deg ~11km) + high name match (>0.85 OR contains)
                tier1_match = (lat_diff < 0.01 and lon_diff < 0.01) and (name_similarity > 0.6 or contains_match)
                tier2_match = (lat_diff < 0.1 and lon_diff < 0.1) and (name_similarity > 0.85 or contains_match)

                if tier1_match or tier2_match:
                    logger.debug(f"Coordinate match: '{name}' ~= '{existing_fac['name']}' "
                               f"(coords: Δlat={lat_diff:.3f},Δlon={lon_diff:.3f}, similarity: {name_similarity:.2f}, contains: {contains_match})")
                    return existing_id

    # PRIORITY 2: Exact name match
    name_lower = name.lower()
    for existing_id, existing_fac in existing.items():
        existing_name_lower = existing_fac['name'].lower()

        if name_lower == existing_name_lower:
            # Exact name match - check coords if available
            if lat and lon and existing_fac['location'].get('lat') and existing_fac['location'].get('lon'):
                lat_diff = abs(lat - existing_fac['location']['lat'])
                lon_diff = abs(lon - existing_fac['location']['lon'])
                if lat_diff < 0.01 and lon_diff < 0.01:  # Within ~1km
                    return existing_id
            else:
                # No coords to compare, assume duplicate by exact name
                return existing_id

    # PRIORITY 3: Fuzzy name match (>85% similarity OR high word overlap)
    for existing_id, existing_fac in existing.items():
        existing_name_lower = existing_fac['name'].lower()
        name_similarity = SequenceMatcher(None, name_lower, existing_name_lower).ratio()

        # Also check word overlap (e.g., "two rivers mine" vs "two rivers platinum mine")
        words1 = set(name_lower.split())
        words2 = set(existing_name_lower.split())
        if words1 and words2:
            word_overlap = len(words1 & words2) / min(len(words1), len(words2))
        else:
            word_overlap = 0

        # Match if high similarity OR very high word overlap (>0.8 means 80%+ words match)
        if name_similarity > 0.85 or word_overlap > 0.8:
            logger.debug(f"Fuzzy name match: '{name}' ~= '{existing_fac['name']}' "
                       f"(similarity: {name_similarity:.2f}, word_overlap: {word_overlap:.2f})")
            return existing_id

    # PRIORITY 4: Alias match
    for existing_id, existing_fac in existing.items():
        if name_lower in [a.lower() for a in existing_fac.get('aliases', [])]:
            logger.debug(f"Alias match: '{name}' in aliases of '{existing_fac['name']}'")
            return existing_id

    return None


def normalize_metal(metal: str) -> str:
    """Normalize metal name (fallback version)."""
    metal_lower = metal.lower().strip()
    return METAL_NORMALIZE_MAP.get(metal_lower, metal_lower)


def parse_commodities(primary: str, other: str, enhanced: bool = False) -> List[Dict]:
    """Parse commodities from primary and other strings.

    Args:
        primary: Primary commodity string
        other: Other commodities string
        enhanced: If True, use entityidentity metal_identifier for enhanced resolution

    Returns:
        List of commodity dictionaries with metal, primary flag, and optionally
        chemical_formula and category if enhanced mode is enabled
    """
    commodities = []
    seen = set()

    # Import enhanced metal identifier if available
    metal_identifier_func = None
    if enhanced:
        try:
            sys.path.insert(0, str(pathlib.Path(__file__).parent.parent.parent / 'entityidentity'))
            from entityidentity import metal_identifier
            metal_identifier_func = metal_identifier
            logger.debug("Using enhanced metal identification from entityidentity")
        except ImportError as e:
            logger.warning(f"Could not import entityidentity metal_identifier, using fallback: {e}")

    def normalize_commodity_entry(metal_str: str) -> Dict:
        """Normalize a single commodity entry."""
        if metal_identifier_func:
            # Enhanced normalization with entityidentity
            try:
                result = metal_identifier_func(metal_str)
                if result:
                    return {
                        "metal": result.get('name', metal_str.lower()),
                        "chemical_formula": result.get('formula'),
                        "category": result.get('category', 'unknown')
                    }
            except Exception as e:
                logger.debug(f"Metal identifier failed for '{metal_str}': {e}")

        # Fallback to basic normalization
        return {"metal": normalize_metal(metal_str), "chemical_formula": None, "category": "unknown"}

    if primary:
        for metal in re.split(r'[,;]', primary):
            metal = metal.strip()
            if metal and metal != '-':
                normalized = normalize_commodity_entry(metal)
                metal_key = normalized['metal']
                if metal_key not in seen:
                    commodity = {"metal": metal_key, "primary": True}
                    if normalized.get('chemical_formula'):
                        commodity['chemical_formula'] = normalized['chemical_formula']
                    if normalized.get('category') and normalized['category'] != 'unknown':
                        commodity['category'] = normalized['category']
                    commodities.append(commodity)
                    seen.add(metal_key)

    if other:
        for metal in re.split(r'[,;]', other):
            metal = metal.strip()
            if metal and metal != '-':
                normalized = normalize_commodity_entry(metal)
                metal_key = normalized['metal']
                if metal_key not in seen:
                    commodity = {"metal": metal_key, "primary": False}
                    if normalized.get('chemical_formula'):
                        commodity['chemical_formula'] = normalized['chemical_formula']
                    if normalized.get('category') and normalized['category'] != 'unknown':
                        commodity['category'] = normalized['category']
                    commodities.append(commodity)
                    seen.add(metal_key)

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


def process_report(report_text: str, country_iso3: str, country_dir: str, source_name: str) -> Dict:
    """Main pipeline: extract tables and import facilities.

    Uses entity resolution by default for:
    - Metal normalization with chemical formulas
    - Company resolution with canonical IDs
    - Auto-detection of country from facility data

    Args:
        report_text: The report text containing facility tables
        country_iso3: ISO3 country code (or None for auto-detection)
        source_name: Source name for citations

    Returns:
        Dictionary containing facilities, duplicates, errors, and statistics
    """
    logger.info(f"Processing report for {country_iso3}...")

    # Phase 2 design: No company resolution during import
    # Company mentions are extracted, resolution happens in enrich_companies.py

    # Try CSV parsing first
    tables = []
    csv_table = parse_csv_file(report_text)
    if csv_table and is_facility_table(csv_table):
        tables = [csv_table]
        logger.info(f"Parsed as CSV file with {len(csv_table['rows'])} rows")
    else:
        # Extract markdown/tab tables
        logger.info("Extracting facility tables from report...")
        tables = extract_markdown_tables(report_text)
        logger.info(f"Found {len(tables)} facility tables")

    # Try concatenated table format (South Africa style) if no standard tables found
    if not tables:
        logger.info("No standard tables found, trying concatenated table format...")
        concat_table = extract_concatenated_table(report_text)
        if concat_table:
            tables = [concat_table]
            logger.info(f"Extracted concatenated table with {len(concat_table['rows'])} rows")

    # Extract facilities from narrative text (list/paragraph format)
    text_facilities = []
    if not tables:
        logger.info("No tables found, attempting text extraction...")
        text_facilities = extract_facilities_from_text(report_text)
        logger.info(f"Extracted {len(text_facilities)} facilities from text")

    if not tables and not text_facilities:
        logger.error("No facility data found in report")
        logger.error("Check that your report contains:")
        logger.error("  - Tables with | or tab separators, OR")
        logger.error("  - Facility mentions like 'Name Mine: description' or '- Name Project:'")
        return {"error": "No facility data found", "facilities": []}

    # Load existing facilities from directory
    existing = load_existing_facilities(country_dir)
    logger.info(f"Loaded {len(existing)} existing facilities for duplicate detection")

    # Process all rows from all tables
    facilities = []
    duplicates = []
    errors = []
    stats = defaultdict(int)

    # Entity resolution statistics
    stats['metal_resolutions'] = 0
    stats['company_mentions_extracted'] = 0

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

                # Handle per-row country (for Mines.csv with "Country or Region" column)
                row_country_iso3 = country_iso3  # default from function parameter
                row_country_dir = country_dir    # default from function parameter

                if row.get('country'):  # If row has country column
                    try:
                        from utils.country_utils import normalize_country_to_iso3
                        row_country_name = row.get('country', '').strip()
                        if row_country_name and row_country_name != '-':
                            row_country_iso3 = normalize_country_to_iso3(row_country_name)
                            # Determine directory for this country
                            row_country_dir = row_country_iso3
                            if not (FACILITIES_DIR / row_country_iso3).exists():
                                # Try ISO2
                                if len(row_country_iso3) == 3:
                                    iso2 = row_country_iso3[:2]
                                    if (FACILITIES_DIR / iso2).exists():
                                        row_country_dir = iso2
                    except Exception as e:
                        logger.warning(f"Could not normalize country '{row.get('country')}': {e}")

                # Generate facility ID using detected country
                facility_id = f"{row_country_iso3.lower()}-{slugify(name)}-fac"

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

                # Load existing facilities for this country if needed
                if row.get('country') and row_country_dir != country_dir:
                    # Load existing for this country
                    row_existing = load_existing_facilities(row_country_dir)
                else:
                    row_existing = existing

                # Check for duplicate (standard check - facility matcher moved to entityidentity)
                existing_id = check_duplicate(facility_id, name, lat, lon, row_existing)
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
                commodities = parse_commodities(row.get('primary', ''), row.get('other', ''), enhanced=True)

                # Count metal resolutions with formulas
                stats['metal_resolutions'] += len([c for c in commodities if c.get('chemical_formula')])

                status = parse_status(row.get('status', ''))

                # Parse aliases and company mentions from Group Names (Mines.csv specific)
                group_names_str = row.get('group_names', '').strip()
                aliases_from_synonyms = row.get('synonyms', '').strip()

                # Combine aliases from both sources
                aliases = []
                company_mentions = []

                if group_names_str and group_names_str != '-':
                    group_aliases, group_companies = parse_group_names(group_names_str, source_name)
                    aliases.extend(group_aliases)
                    company_mentions.extend(group_companies)

                if aliases_from_synonyms and aliases_from_synonyms != '-':
                    aliases.extend([a.strip() for a in aliases_from_synonyms.split(',') if a.strip()])

                notes = row.get('notes', '').strip()
                if notes and notes != '-':
                    notes = notes[:500]
                else:
                    notes = None

                # Map confidence_factor from Mines.csv to verification
                confidence_factor = row.get('confidence_factor', '').strip()
                confidence_map = {
                    'very high': (0.90, 'csv_imported'),
                    'high': (0.80, 'csv_imported'),
                    'moderate': (0.65, 'csv_imported'),
                    'low': (0.45, 'csv_imported'),
                    'very low': (0.30, 'csv_imported')
                }
                base_confidence, verification_status = confidence_map.get(
                    confidence_factor.lower(),
                    (0.50, 'csv_imported')  # default
                )

                # Build facility (Phase 2 pattern - NO operator_link/owner_links)
                facility = {
                    "facility_id": facility_id,
                    "name": name,
                    "aliases": aliases,
                    "country_iso3": row_country_iso3,  # Use per-row country
                    "country_dir": row_country_dir,    # Store for writing
                    "location": {
                        "lat": lat,
                        "lon": lon,
                        "precision": "site" if (lat and lon) else "unknown"
                    },
                    "types": types,
                    "commodities": commodities,
                    "status": status,
                    "company_mentions": company_mentions,  # Phase 2: mentions only
                    "products": [],
                    "sources": [{
                        "type": "csv_import",
                        "id": source_name,
                        "date": datetime.now().isoformat()
                    }],
                    "verification": {
                        "status": verification_status,
                        "confidence": base_confidence,
                        "last_checked": datetime.now().isoformat(),
                        "checked_by": "import_pipeline",
                        "notes": notes
                    }
                }

                # Track company mentions stats
                stats['company_mentions_extracted'] += len(company_mentions)

                facilities.append(facility)
                stats['total_facilities'] += 1

            except Exception as e:
                logger.error(f"Row {row_num}: Error parsing: {e}")
                errors.append(f"Row {row_num}: {str(e)}")

    # Process text-extracted facilities
    text_num = 0
    for text_fac in text_facilities:
        text_num += 1
        try:
            name = text_fac.get('name', '').strip()
            if not name:
                continue

            # Generate facility ID
            facility_id = f"{country_iso3.lower()}-{slugify(name)}-fac"

            # Check for duplicate
            existing_id = check_duplicate(facility_id, name, None, None, existing)
            if existing_id:
                logger.info(f"  Duplicate: '{name}' already exists as {existing_id}")
                duplicates.append({
                    "row": f"text-{text_num}",
                    "name": name,
                    "existing_id": existing_id
                })
                stats['duplicates_skipped'] += 1
                continue

            # Parse commodities from list of strings
            commodities = []
            for metal in text_fac.get('commodities', []):
                commodity_data = parse_commodities(metal, '', enhanced=True)
                if commodity_data:
                    commodities.extend(commodity_data)

            # Count metal resolutions with formulas
            stats['metal_resolutions'] += len([c for c in commodities if c.get('chemical_formula')])

            # Extract company mentions (Phase 2 pattern - no resolution yet)
            company_mentions = []
            base_confidence = 0.65  # Lower confidence for text extraction

            if text_fac.get('operator'):
                operator_str = text_fac.get('operator', '').strip()
                if operator_str:
                    company_mentions.append({
                        "name": operator_str,
                        "role": "operator",
                        "source": source_name,
                        "confidence": 0.60,
                        "first_seen": datetime.now().isoformat(),
                        "evidence": "Extracted from text"
                    })
                    stats['company_mentions_extracted'] += 1

            # Build facility (Phase 2 pattern - NO operator_link/owner_links)
            facility = {
                "facility_id": facility_id,
                "name": name,
                "aliases": [],
                "country_iso3": country_iso3,
                "location": {
                    "lat": None,
                    "lon": None,
                    "precision": "unknown",
                    "description": text_fac.get('location', '')
                },
                "types": text_fac.get('types', ['mine']),
                "commodities": commodities,
                "status": text_fac.get('status', 'unknown'),
                "company_mentions": company_mentions,  # Phase 2: mentions only
                "products": [],
                "sources": [{
                    "type": "text_extraction",
                    "id": source_name,
                    "date": datetime.now().isoformat()
                }],
                "verification": {
                    "status": "llm_suggested",
                    "confidence": base_confidence,
                    "last_checked": datetime.now().isoformat(),
                    "checked_by": "import_pipeline_text_extraction",
                    "notes": text_fac.get('notes')
                }
            }

            facilities.append(facility)
            stats['total_facilities'] += 1

        except Exception as e:
            logger.error(f"Text facility {text_num}: Error parsing: {e}")
            errors.append(f"Text facility {text_num}: {str(e)}")

    logger.info(f"Processed {row_num} table rows + {text_num} text facilities")
    logger.info(f"Found {stats['total_facilities']} new facilities")
    logger.info(f"Skipped {stats['duplicates_skipped']} duplicates")
    logger.info(f"Entity resolution stats:")
    logger.info(f"  - Metals with formulas: {stats['metal_resolutions']}")
    logger.info(f"  - Company mentions extracted: {stats['company_mentions_extracted']}")
    logger.info(f"  (Run enrich_companies.py for Phase 2 resolution)")

    return {
        "facilities": facilities,
        "duplicates": duplicates,
        "errors": errors,
        "stats": stats
    }


def write_facilities(facilities: List[Dict], country_dir_name: str) -> int:
    """Write facility JSON files.

    Args:
        facilities: List of facility dicts (may have per-row country_dir)
        country_dir_name: Default directory name (could be ISO2 or ISO3)
    """
    written = 0
    written_by_country = defaultdict(int)

    for facility in facilities:
        # Check if facility has its own country_dir (per-row country case)
        if 'country_dir' in facility:
            dir_name = facility.pop('country_dir')  # Remove temp field before writing
        else:
            dir_name = country_dir_name

        country_dir = FACILITIES_DIR / dir_name
        country_dir.mkdir(parents=True, exist_ok=True)

        facility_file = country_dir / f"{facility['facility_id']}.json"
        with open(facility_file, 'w', encoding='utf-8') as f:
            json.dump(facility, f, ensure_ascii=False, indent=2)
        written += 1
        written_by_country[dir_name] += 1

    # Log per-country stats if multiple countries
    if len(written_by_country) > 1:
        logger.info(f"Wrote facilities to {len(written_by_country)} countries:")
        for country, count in sorted(written_by_country.items(), key=lambda x: -x[1])[:10]:
            logger.info(f"  {country}: {count} facilities")

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


def detect_country_from_filename(filename: str) -> Optional[str]:
    """Detect country from filename.

    Examples:
        "albania.txt" -> "albania"
        "algeria_mines.txt" -> "algeria"
        "/path/to/dza_facilities.txt" -> "dza"
    """
    if not filename or filename == '-':
        return None

    # Get just the filename without path
    basename = pathlib.Path(filename).stem

    # Split on common separators and take first part
    parts = re.split(r'[_\-\s.]', basename.lower())
    if parts:
        # Return the first meaningful part (likely the country)
        country_part = parts[0].strip()
        if len(country_part) >= 2:  # At least 2 chars
            return country_part

    return None


def main():
    parser = argparse.ArgumentParser(
        description="Facility import from research reports with automatic entity resolution",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Auto-detect country from filename
  python import_from_report.py albania.txt
  python import_from_report.py algeria_mines.txt

  # Explicitly specify country
  python import_from_report.py report.txt --country DZ
  python import_from_report.py report.txt --country Algeria

  # With custom source name
  python import_from_report.py albania.txt --source "Albania Mining Report 2025"

  # From stdin (requires --country)
  cat report.txt | python import_from_report.py --country DZ
  pbpaste | python import_from_report.py --country AF

Features (automatic):
  - Country auto-detection from filename (e.g., "albania.txt" -> ALB)
  - Metal normalization with chemical formulas (e.g., copper -> Cu)
  - Company resolution to canonical IDs via EntityIdentity
  - Duplicate detection (name + location + aliases)
  - Confidence boosting for facilities with resolved operators
        """
    )
    parser.add_argument("input_file", help="Input report file (use '-' for stdin)")
    parser.add_argument("--country", help="Country name, ISO2, or ISO3 code (optional, auto-detected from filename if not provided)")
    parser.add_argument("--source", help="Source name for citation (optional, auto-generated if not provided)")

    args = parser.parse_args()

    # Determine country - try auto-detect first, then use explicit arg
    country_input = args.country
    if not country_input:
        # Try to detect from filename
        detected = detect_country_from_filename(args.input_file)
        if detected:
            country_input = detected
            logger.info(f"Auto-detected country from filename: {detected}")
        else:
            # Check if file might have per-row countries (like Mines.csv)
            if args.input_file.lower().endswith('.csv'):
                logger.info("CSV file detected, will use per-row country detection if available")
                country_input = "MULTI"  # Marker for multi-country files
            else:
                print("Error: Could not auto-detect country from filename")
                print("Please specify country explicitly with --country")
                print("\nUsage: python import_from_report.py report.txt --country DZ")
                print("Or use a filename with country name: python import_from_report.py albania.txt")
                return 1

    # Find actual country code (ISO3 for display, directory name for file ops)
    # Special case: MULTI means per-row countries (skip validation)
    if country_input == "MULTI":
        country_iso3 = "MULTI"
        country_dir = "MULTI"  # Will be overridden per-row
    else:
        try:
            country_iso3, country_dir = find_country_code(country_input)
        except Exception as e:
            print(f"Error: Could not resolve country '{country_input}': {e}")
            print("\nPlease provide a valid country name, ISO2, or ISO3 code")
            print("Examples: Albania, ALB, AL")
            return 1

    # Auto-generate source name if not provided
    if not args.source:
        country_part = country_iso3 if country_iso3 else "Unknown"
        source_name = f"Research Import {country_part} {datetime.now().strftime('%Y-%m-%d')}"
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

    # Count existing facilities before import
    if country_dir != "MULTI":
        existing_facilities = load_existing_facilities(country_dir)
        initial_count = len(existing_facilities)
    else:
        # Multi-country file, can't count initial facilities easily
        existing_facilities = {}
        initial_count = 0

    # Process
    result = process_report(report_text, country_iso3, country_dir, source_name)

    if 'error' in result:
        print(f"\nError: {result['error']}")
        return 1

    # Write files
    if result['facilities']:
        written = write_facilities(result['facilities'], country_dir)
        logger.info(f"Wrote {written} facility files to {country_dir}/")
    else:
        logger.warning("No new facilities to write (all may be duplicates)")

    # Count final facilities after import
    if country_dir != "MULTI":
        final_facilities = load_existing_facilities(country_dir)
        final_count = len(final_facilities)
    else:
        # Multi-country file, count from result
        final_count = initial_count + len(result['facilities'])

    # Get country name for display
    if country_iso3 == "MULTI":
        country_display = "Multiple Countries (per-row detection)"
    else:
        try:
            from utils.country_utils import iso3_to_country_name
            country_name = iso3_to_country_name(country_iso3)
            country_display = f"{country_iso3} - {country_name}" if country_name else country_iso3
        except:
            country_display = country_iso3

    # Write report
    report = write_report(result, country_iso3, source_name)

    # Print summary
    print("\n" + "="*60)
    print("IMPORT COMPLETE")
    print("="*60)
    print(f"Country: {country_display}")
    print(f"Source: {source_name}")
    print()
    print(f"Before import:  {initial_count} facilities")
    print(f"After import:   {final_count} facilities")
    print(f"Added:          {final_count - initial_count} new facilities")
    print(f"Duplicates:     {report['summary']['duplicates_skipped']} skipped")
    if report['summary']['errors']:
        print(f"Errors:         {report['summary']['errors']}")

    if result['stats'].get('company_mentions_extracted', 0) > 0:
        print(f"\nPhase 1 Extraction:")
        print(f"  Metals with formulas: {result['stats'].get('metal_resolutions', 0)}")
        print(f"  Company mentions extracted: {result['stats'].get('company_mentions_extracted', 0)}")
        print(f"\nNext: Run 'python scripts/enrich_companies.py' for Phase 2 resolution")

    print("="*60)

    if result['duplicates']:
        print(f"\nDuplicates Skipped:")
        for dup in result['duplicates'][:5]:  # Show first 5
            print(f"  • '{dup['name']}' (exists as {dup['existing_id']})")
        if len(result['duplicates']) > 5:
            print(f"  ... and {len(result['duplicates']) - 5} more")
        print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
