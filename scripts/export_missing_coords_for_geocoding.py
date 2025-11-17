#!/usr/bin/env python3
"""
Export facilities with missing coordinates in the Mines.csv format.

This creates a CSV compatible with the standard Mines.csv format that can be:
1. Geocoded manually or via external tools
2. Re-imported using import_from_report.py

Output format matches gt/Mines.csv:
- Confidence Factor
- Mine Name
- Companies
- Latitude (empty for missing)
- Longitude (empty for missing)
- Asset Type
- Country or Region
- Primary Commodity
- Secondary Commodity
- Other Commodities
"""

import json
import csv
from pathlib import Path
from collections import defaultdict


def load_all_facilities(facilities_dir):
    """Load all facility JSON files."""
    facilities = []

    for country_dir in sorted(facilities_dir.iterdir()):
        if not country_dir.is_dir():
            continue

        for json_file in sorted(country_dir.glob("*.json")):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    facility = json.load(f)
                    facility['_file_path'] = str(json_file)
                    facilities.append(facility)
            except Exception as e:
                print(f"Error loading {json_file}: {e}")

    return facilities


def has_complete_coords(facility):
    """Check if facility has complete coordinates."""
    location = facility.get('location', {})

    if not isinstance(location, dict):
        return False

    lat = location.get('lat')
    lon = location.get('lon')

    if lat is None or lon is None:
        return False

    try:
        lat_f = float(lat)
        lon_f = float(lon)

        # Exclude (0,0) as it's likely a placeholder
        if lat_f == 0.0 and lon_f == 0.0:
            return False

        return True
    except (ValueError, TypeError):
        return False


def get_confidence_label(confidence):
    """Convert numeric confidence to label."""
    if confidence is None:
        return "Unknown"
    elif confidence >= 0.8:
        return "High"
    elif confidence >= 0.6:
        return "Moderate"
    elif confidence >= 0.4:
        return "Low"
    else:
        return "Very Low"


def get_asset_type(facility):
    """Get asset type from types field."""
    types = facility.get('types', [])
    primary_type = facility.get('primary_type')

    if primary_type:
        return primary_type.capitalize()
    elif types:
        return types[0].capitalize()
    else:
        return "Unknown"


def get_country_name(iso3):
    """Convert ISO3 to country name (simplified)."""
    # This is a simplified version - could use pycountry for full mapping
    country_map = {
        'USA': 'United States', 'CHN': 'China', 'AUS': 'Australia',
        'ZAF': 'South Africa', 'IND': 'India', 'CAN': 'Canada',
        'BRA': 'Brazil', 'RUS': 'Russia', 'IDN': 'Indonesia',
        'PER': 'Peru', 'MEX': 'Mexico', 'CHL': 'Chile',
        'ARE': 'United Arab Emirates', 'MAR': 'Morocco', 'KAZ': 'Kazakhstan',
        'ZWE': 'Zimbabwe', 'BEL': 'Belgium', 'NPL': 'Nepal',
        'GRL': 'Greenland', 'GAB': 'Gabon', 'BHR': 'Bahrain',
        'TCD': 'Chad', 'CMR': 'Cameroon', 'BDI': 'Burundi',
        'AFG': 'Afghanistan', 'ALB': 'Albania', 'DZA': 'Algeria',
        'ARG': 'Argentina', 'ARM': 'Armenia', 'AUT': 'Austria',
        'AZE': 'Azerbaijan', 'BGD': 'Bangladesh', 'BGR': 'Bulgaria',
        'BFA': 'Burkina Faso', 'BOL': 'Bolivia', 'BIH': 'Bosnia and Herzegovina',
        'BWA': 'Botswana', 'MMR': 'Myanmar', 'KHM': 'Cambodia',
        'CAF': 'Central African Republic', 'COD': 'Democratic Republic of the Congo',
        'COG': 'Republic of the Congo', 'COL': 'Colombia', 'CRI': 'Costa Rica',
        'CIV': "Côte d'Ivoire", 'CUB': 'Cuba', 'CYP': 'Cyprus',
        'CZE': 'Czech Republic', 'DEU': 'Germany', 'DOM': 'Dominican Republic',
        'ECU': 'Ecuador', 'EGY': 'Egypt', 'ERI': 'Eritrea',
        'ESP': 'Spain', 'ETH': 'Ethiopia', 'FIN': 'Finland',
        'FJI': 'Fiji', 'FRA': 'France', 'GAB': 'Gabon',
        'GEO': 'Georgia', 'GHA': 'Ghana', 'GRC': 'Greece',
        'GTM': 'Guatemala', 'GIN': 'Guinea', 'GUY': 'Guyana',
        'HND': 'Honduras', 'HUN': 'Hungary', 'ISL': 'Iceland',
        'IRN': 'Iran', 'IRL': 'Ireland', 'ITA': 'Italy',
        'JAM': 'Jamaica', 'JPN': 'Japan', 'KGZ': 'Kyrgyzstan',
        'KOR': 'South Korea', 'LAO': 'Laos', 'LBR': 'Liberia',
        'LUX': 'Luxembourg', 'MDG': 'Madagascar', 'MWI': 'Malawi',
        'MYS': 'Malaysia', 'MLI': 'Mali', 'MRT': 'Mauritania',
        'MCO': 'Monaco', 'MNG': 'Mongolia', 'MNE': 'Montenegro',
        'MOZ': 'Mozambique', 'NAM': 'Namibia', 'NCL': 'New Caledonia',
        'NER': 'Niger', 'NGA': 'Nigeria', 'NIC': 'Nicaragua',
        'NOR': 'Norway', 'NZL': 'New Zealand', 'OMN': 'Oman',
        'PAK': 'Pakistan', 'PAN': 'Panama', 'PNG': 'Papua New Guinea',
        'PHL': 'Philippines', 'POL': 'Poland', 'PRT': 'Portugal',
        'PRK': 'North Korea', 'QAT': 'Qatar', 'ROU': 'Romania',
        'ROMANIAN': 'Romania', 'RWA': 'Rwanda', 'SAU': 'Saudi Arabia',
        'SDN': 'Sudan', 'SEN': 'Senegal', 'SRB': 'Serbia',
        'SLE': 'Sierra Leone', 'SVK': 'Slovakia', 'SVN': 'Slovenia',
        'SUR': 'Suriname', 'SWE': 'Sweden', 'SWZ': 'Eswatini',
        'TJK': 'Tajikistan', 'TZA': 'Tanzania', 'THA': 'Thailand',
        'TKM': 'Turkmenistan', 'TTO': 'Trinidad and Tobago', 'TUN': 'Tunisia',
        'TUR': 'Turkey', 'TWN': 'Taiwan', 'UGA': 'Uganda',
        'UKR': 'Ukraine', 'UZB': 'Uzbekistan', 'VEN': 'Venezuela',
        'VNM': 'Vietnam', 'ZMB': 'Zambia', 'GBR': 'United Kingdom',
        'NLD': 'Netherlands', 'BTN': 'Bhutan', 'GUF': 'French Guiana',
        'AGO': 'Angola', 'MKD': 'North Macedonia'
    }

    return country_map.get(iso3, iso3)


def get_commodities(facility):
    """Extract primary, secondary, and other commodities."""
    commodities = facility.get('commodities', [])

    primary = None
    secondary = None
    others = []

    for i, comm in enumerate(commodities):
        metal = comm.get('metal', '')
        is_primary = comm.get('primary', False)

        if is_primary or i == 0:
            primary = metal
        elif i == 1:
            secondary = metal
        else:
            others.append(metal)

    return primary or '', secondary or '', '; '.join(others)


def get_companies(facility):
    """Extract company names."""
    companies = []

    # Get operator
    operator = facility.get('operator', '').strip()
    if operator:
        companies.append(operator)

    # Get owners
    owner = facility.get('owner', '').strip()
    if owner and owner != operator:
        companies.append(owner)

    # Fallback to company mentions
    if not companies:
        mentions = facility.get('company_mentions', [])
        for mention in mentions[:3]:  # Limit to 3
            name = mention.get('name', '').strip()
            if name and name not in companies:
                companies.append(name)

    return '; '.join(companies)


def export_to_mines_format(facilities, output_path):
    """Export facilities to Mines.csv format."""
    if not facilities:
        print("No facilities to export!")
        return

    columns = [
        'Confidence Factor',
        'Mine Name',
        'Companies',
        'Latitude',
        'Longitude',
        'Asset Type',
        'Country or Region',
        'Primary Commodity',
        'Secondary Commodity',
        'Other Commodities'
    ]

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(columns)

        for facility in facilities:
            # Get location data
            location = facility.get('location', {})
            lat = location.get('lat', '') if isinstance(location, dict) else ''
            lon = location.get('lon', '') if isinstance(location, dict) else ''

            # Get confidence
            confidence = facility.get('verification', {}).get('confidence')
            confidence_label = get_confidence_label(confidence)

            # Get commodities
            primary, secondary, others = get_commodities(facility)

            # Get companies
            companies = get_companies(facility)

            row = [
                confidence_label,
                facility.get('name', ''),
                companies,
                lat,  # Leave empty for missing coords
                lon,  # Leave empty for missing coords
                get_asset_type(facility),
                get_country_name(facility.get('country_iso3', '')),
                primary,
                secondary,
                others
            ]

            writer.writerow(row)

    print(f"Exported {len(facilities)} facilities to {output_path}")


def main():
    # Setup paths
    script_dir = Path(__file__).parent
    facilities_dir = script_dir.parent / "facilities"
    output_dir = script_dir.parent / "output"
    output_dir.mkdir(exist_ok=True)

    # Load all facilities
    print("Loading all facilities...")
    facilities = load_all_facilities(facilities_dir)
    print(f"Loaded {len(facilities)} facilities")

    # Filter to missing coordinates
    missing_coords = []
    by_country = defaultdict(list)

    for facility in facilities:
        if not has_complete_coords(facility):
            missing_coords.append(facility)
            by_country[facility.get('country_iso3', 'UNKNOWN')].append(facility)

    print(f"\nFound {len(missing_coords)} facilities with missing coordinates")

    # Export to Mines.csv format
    csv_path = output_dir / "Missing_Coords_For_Geocoding.csv"
    print(f"\nExporting to {csv_path}...")
    export_to_mines_format(missing_coords, csv_path)

    # Print summary by country
    print(f"\nBREAKDOWN BY COUNTRY:")
    print(f"{'Country':<10} {'Missing':<10} {'Country Name':<30}")
    print("-" * 52)

    sorted_countries = sorted(by_country.items(), key=lambda x: len(x[1]), reverse=True)
    for country, facilities_list in sorted_countries[:20]:
        print(f"{country:<10} {len(facilities_list):<10} {get_country_name(country):<30}")

    print(f"\nOutput file: {csv_path}")
    print("\nThis file can be:")
    print("  1. Geocoded manually in Excel/Google Sheets")
    print("  2. Processed with external geocoding tools")
    print("  3. Re-imported using: python scripts/import_from_report.py")
    print("\nOr use automated geocoding:")
    print("  python scripts/backfill.py geocode --all")


if __name__ == '__main__':
    main()
