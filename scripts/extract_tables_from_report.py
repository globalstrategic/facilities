#!/usr/bin/env python3
"""
Extract facility tables from research reports and convert to CSV.

This helper script parses markdown tables from research reports and outputs
them as CSV files ready for import into the facilities database.

Usage:
    python extract_tables_from_report.py report.txt --output algeria_facilities.csv
    python extract_tables_from_report.py report.md --output afghanistan_facilities.csv --format json
"""

import re
import csv
import json
import argparse
import pathlib
from typing import List, Dict, Optional


def extract_markdown_tables(text: str) -> List[Dict[str, List[Dict]]]:
    """
    Extract all markdown tables from text.
    Returns list of dicts with 'headers' and 'rows' keys.
    """
    tables = []

    # Pattern to match markdown tables
    # Looking for lines with | separators
    lines = text.split('\n')

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Check if this line looks like a table row (has | characters)
        if '|' in line and line.count('|') >= 2:
            # Found potential table start
            table_lines = []

            # Collect all consecutive lines that look like table rows
            while i < len(lines):
                line = lines[i].strip()
                if '|' in line and line.count('|') >= 2:
                    table_lines.append(line)
                    i += 1
                elif line == '':
                    # Empty line might be part of table
                    i += 1
                    if i < len(lines) and '|' in lines[i]:
                        continue
                    else:
                        break
                else:
                    break

            if len(table_lines) >= 2:  # Need at least header + separator
                tables.append(parse_markdown_table(table_lines))
        else:
            i += 1

    return tables


def parse_markdown_table(lines: List[str]) -> Dict[str, any]:
    """Parse a markdown table from lines."""
    # Remove leading/trailing pipes and split by |
    def split_row(line):
        # Remove leading/trailing pipes
        line = line.strip()
        if line.startswith('|'):
            line = line[1:]
        if line.endswith('|'):
            line = line[:-1]
        return [cell.strip() for cell in line.split('|')]

    if len(lines) < 2:
        return None

    # First line is headers
    headers = split_row(lines[0])

    # Second line should be separator (---|----|---)
    # Skip it
    separator_idx = 1
    while separator_idx < len(lines) and re.match(r'^[\s|:-]+$', lines[separator_idx]):
        separator_idx += 1

    # Rest are data rows
    rows = []
    for line in lines[separator_idx:]:
        if line.strip() and not re.match(r'^[\s|:-]+$', line):
            row_data = split_row(line)
            # Pad row if needed
            while len(row_data) < len(headers):
                row_data.append('')
            rows.append(dict(zip(headers, row_data)))

    return {
        'headers': headers,
        'rows': rows
    }


def is_facility_table(table: Dict) -> bool:
    """
    Determine if a table contains facility data.
    Look for key columns like name, coordinates, commodities.
    """
    if not table or 'headers' not in table:
        return False

    headers_lower = [h.lower() for h in table['headers']]

    # Check for facility-related headers
    facility_indicators = [
        'site', 'mine', 'facility', 'name', 'deposit', 'project',
        'latitude', 'longitude', 'commodity', 'metal',
        'location', 'operator', 'status'
    ]

    matches = sum(1 for indicator in facility_indicators
                  if any(indicator in h for h in headers_lower))

    return matches >= 3  # Need at least 3 matching indicators


def normalize_headers(headers: List[str]) -> List[str]:
    """Normalize header names to match expected schema."""
    normalized = []
    for h in headers:
        h_lower = h.lower().strip()

        # Map various header names to standard names
        if any(x in h_lower for x in ['site', 'mine name', 'facility name', 'asset name']):
            normalized.append('Site/Mine Name')
        elif 'synonym' in h_lower or 'alias' in h_lower:
            normalized.append('Synonyms')
        elif 'latitude' in h_lower or h_lower == 'lat':
            normalized.append('Latitude')
        elif 'longitude' in h_lower or h_lower == 'lon':
            normalized.append('Longitude')
        elif 'primary commodity' in h_lower or (h_lower == 'primary' and 'commodity' in ' '.join(headers).lower()):
            normalized.append('Primary Commodity')
        elif 'other commodities' in h_lower or 'secondary commodity' in h_lower:
            normalized.append('Other Commodities')
        elif 'deposit type' in h_lower or 'geology' in h_lower:
            normalized.append('Deposit Type/Geology')
        elif 'asset type' in h_lower or 'facility type' in h_lower or h_lower == 'types':
            normalized.append('Asset Type')
        elif 'status' in h_lower or 'operational' in h_lower:
            normalized.append('Operational Status')
        elif 'operator' in h_lower or 'stakeholder' in h_lower or 'owner' in h_lower:
            normalized.append('Operator(s) / Key Stakeholders')
        elif 'note' in h_lower or 'analyst' in h_lower:
            normalized.append('Analyst Notes & Key Snippet IDs')
        else:
            normalized.append(h)

    return normalized


def write_csv(tables: List[Dict], output_path: pathlib.Path):
    """Write facility tables to CSV."""
    # Combine all facility tables
    all_rows = []

    for table in tables:
        if is_facility_table(table):
            # Normalize headers
            headers = normalize_headers(table['headers'])

            # Add rows with normalized headers
            for row in table['rows']:
                normalized_row = {}
                for i, (orig_header, norm_header) in enumerate(zip(table['headers'], headers)):
                    if i < len(row):
                        normalized_row[norm_header] = list(row.values())[i]
                all_rows.append(normalized_row)

    if not all_rows:
        print("No facility tables found in report")
        return

    # Get all unique headers
    all_headers = set()
    for row in all_rows:
        all_headers.update(row.keys())

    # Standard headers first, then others
    standard_headers = [
        'Site/Mine Name', 'Synonyms', 'Latitude', 'Longitude',
        'Primary Commodity', 'Other Commodities', 'Asset Type',
        'Operational Status', 'Operator(s) / Key Stakeholders',
        'Deposit Type/Geology', 'Analyst Notes & Key Snippet IDs'
    ]

    final_headers = [h for h in standard_headers if h in all_headers]
    final_headers.extend([h for h in sorted(all_headers) if h not in final_headers])

    # Write CSV
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=final_headers)
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)

    print(f"Wrote {len(all_rows)} facilities to {output_path}")


def write_json(tables: List[Dict], output_path: pathlib.Path):
    """Write facility tables to JSON."""
    all_rows = []

    for table in tables:
        if is_facility_table(table):
            headers = normalize_headers(table['headers'])
            for row in table['rows']:
                normalized_row = {}
                for i, (orig_header, norm_header) in enumerate(zip(table['headers'], headers)):
                    if i < len(row):
                        normalized_row[norm_header] = list(row.values())[i]
                all_rows.append(normalized_row)

    if not all_rows:
        print("No facility tables found in report")
        return

    output_data = {
        'extracted_date': pathlib.Path().cwd().name,
        'facilities': all_rows
    }

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(all_rows)} facilities to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Extract facility tables from research reports"
    )
    parser.add_argument("input_file", help="Input report file (txt or md)")
    parser.add_argument("--output", required=True, help="Output file path")
    parser.add_argument("--format", choices=["csv", "json"], default="csv",
                       help="Output format")

    args = parser.parse_args()

    input_path = pathlib.Path(args.input_file)
    output_path = pathlib.Path(args.output)

    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}")
        return 1

    # Read input
    with open(input_path, 'r', encoding='utf-8') as f:
        text = f.read()

    # Extract tables
    print(f"Extracting tables from {input_path}...")
    tables = extract_markdown_tables(text)
    print(f"Found {len(tables)} tables")

    # Filter to facility tables
    facility_tables = [t for t in tables if is_facility_table(t)]
    print(f"Found {len(facility_tables)} facility tables")

    # Write output
    if args.format == "csv":
        write_csv(facility_tables, output_path)
    else:
        write_json(facility_tables, output_path)

    return 0


if __name__ == "__main__":
    exit(main())
