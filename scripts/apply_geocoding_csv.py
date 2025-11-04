#!/usr/bin/env python3
"""Apply geocoding enrichments from CSV with proper hygiene."""

import json
import csv
from pathlib import Path
from io import StringIO
import unicodedata

# Try ftfy for mojibake fixing
try:
    import ftfy
except ImportError:
    ftfy = None

CSV_DATA = """facility_id,country_iso3,raw_name,canonical_name,operator_display,primary_type,lat,lon,precision,town,region,aliases,commodities,notes
# --- ARMENIA ---
arm-kajaran-fac,ARM,Kajaran,Kajaran Mine,Zangezur Copper & Molybdenum Combine (ZCMC),mine,39.14389,46.13778,site,Kajaran,Syunik,,Copper|Molybdenum,Mindat coordinates for Kajaran pit; ZCMC operator.
arm-teghut-mine-fac,ARM,Teghut Mine,Teghut Mine,Teghout CJSC (historic: Vallex/ACP),mine,41.11806,44.84583,region,Teghut,Lori,,Copper|Molybdenum,Village centroid near mine; mine page/wikidata corroboration.
arm-chaarat-kapan-fac,ARM,Chaarat Kapan,Kapan Mine,Chaarat Kapan,mine,39.20659,46.43303,site,Kapan,Syunik,,Gold|Silver|Copper|Zinc,Shahumyan underground within Kapan complex.
arm-amulsar-project-fac,ARM,Amulsar Project,Amulsar Gold Mine,Lydian Armenia,mine,39.736988,45.606874,site,Jermuk,Vayots Dzor,,Gold,HLF coordinates (Bellingcat); ESIA bounding box corroborates.
arm-ararat-gold-extraction-plant-fac,ARM,Ararat Gold Extraction Plant,Ararat Gold Recovery Company Plant,GeoProMining Gold,plant,39.796331,44.726260,site,Ararat,Ararat,,Gold,Off-site processing plant for Sotk/Zod ore.
arm-sotk-mine-fac,ARM,Sotk Mine,Sotk Gold Mine,GPM Gold (GeoProMining),mine,40.235663,45.971034,site,Sotk,Gegharkunik,,Gold,Mine (Armenian side of Zod/Sotk).
arm-akhtala-shamlugh-mine-fac,ARM,Akhtala-Shamlugh Mine,Akhtala Mine,Akhtala Mining Plant,mine,41.150347,44.770628,town,Akhtala,Lori,,Copper|Lead|Silver,Town centroid near mine & plant.
arm-agarak-mine-fac,ARM,Agarak Mine,Agarak Copper-Molybdenum Mine,GeoProMining,mine,38.91611,46.18944,site,Agarak,Syunik,,Copper|Molybdenum,Combine complex coordinates.
# --- BELGIUM ---
bel-hoboken-umicore-fac,BEL,Hoboken (Umicore),Hoboken Smelter / Precious Metals Refining,Umicore PMR,smelter,51.16583,4.33778,site,Hoboken,Antwerp,,Precious metals,Site coordinates + site address.
bel-olen-umicore-fac,BEL,Olen (Umicore),Olen Refinery / Research Campus,Umicore Olen,refinery,51.14306,4.90278,town,Olen,Antwerp,,Cobalt|Nickel|Germanium,Use town precision; address confirmed.
bel-prayon-engis-smelter-fac,BEL,Prayon-Engis smelter,Prayon Engis Plant,Prayon,plant,50.57556,5.38861,site,Engis,Liège,,Phosphates,Plant site coordinates.
# --- BURKINA FASO ---
bfa-essakane-fac,BFA,Essakane,Essakane Mine,IAMGOLD (85%) & Government (15%),mine,14.38306,0.07611,site,Essakane (near Gorom-Gorom),Sahel,,Gold,Mine coordinates; operator page corroboration.
bfa-hounde-fac,BFA,Hounde,Houndé Mine,Endeavour Mining (90%) & Government (10%),mine,11.42306,-3.53528,site,Houndé,Tuy,,Gold,Houndé pit vicinity; Endeavour ownership.
bfa-wahgnion-fac,BFA,Wahgnion,Wahgnion Mine,Government of Burkina Faso (current operator),mine,10.37528,-5.38498,region,Banfora area,Cascades,,Gold,Plant/power block inside mine area; ownership per recent updates.
bfa-yaramoko-fac,BFA,Yaramoko,Yaramoko Mine,Fortuna Silver Mines,mine,11.75000,-3.28000,region,Safané area,Balé,,Gold,Centroid of 55 Zone deposit (technical report).
# --- SOUTH AFRICA (AAP / Sibanye) ---
zaf-amandelbult-fac,ZAF,Amandelbult,Amandelbult Mine,Anglo American Platinum,mine,-24.78576,27.35435,site,Thabazimbi,Limpopo,,PGMs,Mindat mine coordinates (Amandelbult complex).
zaf-bathopele-fac,ZAF,Bathopele,Bathopele Mine,Sibanye-Stillwater,mine,-25.68736,27.30605,site,Rustenburg,North West,,PGMs,Mechanised mine; precise coordinates (MineTracker dataset).
zaf-anglo-american-converter-plant-waterval-smelter-fac,ZAF,Anglo American Converter Plant (Waterval Smelter),Waterval Smelter,Anglo American Platinum,smelter,-25.67526,27.32613,site,Rustenburg,North West,,PGMs,Smelter/ACP complex coordinates.
zaf-anglo-american-platinum-base-metals-refinery-fac,ZAF,Anglo American Platinum Base Metals Refinery,Rustenburg Base Metals Refinery,Anglo American Platinum,refinery,-25.68778,27.33806,site,Rustenburg,North West,,Nickel|Copper|Cobalt,RBMR footprint coordinates.
zaf-anglo-american-platinum-precious-metals-refinery-fac,ZAF,Anglo American Platinum Precious Metals Refinery,Precious Metals Refinery,Anglo American Platinum,refinery,,,town,Brakpan,Gauteng,,PGMs,1 Platinum Road Vulcania (address); set town precision until plot centroid is pinned."""

def clean_text(text):
    """Clean text with NFC normalization and mojibake fixing."""
    if not text:
        return text

    # NFC normalize
    text = unicodedata.normalize("NFC", text)

    # Fix mojibake if ftfy available
    if ftfy:
        text = ftfy.fix_text(text)

    return text

def parse_csv_data():
    """Parse CSV data, skipping comments and cleaning text."""
    lines = [line for line in CSV_DATA.strip().split('\n') if not line.strip().startswith('#')]
    csv_text = '\n'.join(lines)

    reader = csv.DictReader(StringIO(csv_text))
    enrichments = []

    for row in reader:
        # Clean all text fields
        for key in row:
            if isinstance(row[key], str):
                row[key] = clean_text(row[key])

        # Convert lat/lon
        if row.get('lat'):
            try:
                row['lat'] = float(row['lat'])
            except:
                row['lat'] = None
        else:
            row['lat'] = None

        if row.get('lon'):
            try:
                row['lon'] = float(row['lon'])
            except:
                row['lon'] = None
        else:
            row['lon'] = None

        enrichments.append(row)

    return enrichments

def apply_enrichment(facility, enrich):
    """Apply enrichment data to facility, only overwriting when non-empty."""
    # Update location
    if "location" not in facility:
        facility["location"] = {}

    # Only update if provided
    if enrich.get('lat') is not None:
        facility["location"]["lat"] = enrich['lat']
    if enrich.get('lon') is not None:
        facility["location"]["lon"] = enrich['lon']

    if enrich.get('precision'):
        facility["location"]["precision"] = enrich['precision']

    # Update town if provided (or clear for region-level)
    if enrich.get('town'):
        facility["location"]["town"] = enrich['town']
    elif enrich.get('precision') == 'region' and not enrich.get('town'):
        facility["location"]["town"] = None

    # Update region if provided
    if enrich.get('region'):
        facility["location"]["region"] = enrich['region']

    # Update operator display if provided
    if enrich.get('operator_display'):
        facility["operator_display"] = enrich['operator_display']

    # Update primary type if provided and meaningful
    if enrich.get('primary_type') and enrich['primary_type'] != 'facility':
        facility["primary_type"] = enrich['primary_type']
        # Set confidence based on precision
        if enrich.get('precision') in ['site', 'exact']:
            facility["type_confidence"] = 0.95
        elif enrich.get('precision') == 'town':
            facility["type_confidence"] = 0.85
        else:
            facility["type_confidence"] = 0.75

    # Update data quality flags
    if "data_quality" not in facility:
        facility["data_quality"] = {}
    if "flags" not in facility["data_quality"]:
        facility["data_quality"]["flags"] = {}

    # Update town_missing flag
    if facility["location"].get("town"):
        facility["data_quality"]["flags"]["town_missing"] = False
    else:
        facility["data_quality"]["flags"]["town_missing"] = True

    # Update operator flag
    if enrich.get('operator_display'):
        facility["data_quality"]["flags"]["operator_unresolved"] = False

    # Add geocoding source to verification
    if "verification" not in facility:
        facility["verification"] = {}
    if enrich.get('notes'):
        facility["verification"]["geocoding_source"] = enrich['notes']

    return facility

def main():
    """Apply geocoding enrichments."""
    enrichments = parse_csv_data()
    print(f"Loaded {len(enrichments)} enrichments")

    applied = 0
    skipped = 0
    errors = 0

    for enrich in enrichments:
        fac_id = enrich['facility_id']
        country = fac_id.split('-')[0].upper()
        fac_path = Path(f"facilities/{country}/{fac_id}.json")

        if not fac_path.exists():
            print(f"Warning: {fac_path} not found")
            skipped += 1
            continue

        try:
            # Load facility
            with open(fac_path, 'r', encoding='utf-8') as f:
                facility = json.load(f)

            # Apply enrichment
            facility = apply_enrichment(facility, enrich)

            # Write back with UTF-8
            with open(fac_path, 'w', encoding='utf-8') as f:
                json.dump(facility, f, ensure_ascii=False, indent=2)
                f.write("\n")

            applied += 1
            if applied % 5 == 0:
                print(f"Applied {applied} enrichments...")

        except Exception as e:
            print(f"Error updating {fac_id}: {e}")
            errors += 1

    print(f"\n✓ Applied {applied} geocoding enrichments")
    if skipped:
        print(f"⚠ Skipped {skipped} facilities (not found)")
    if errors:
        print(f"⚠ {errors} facilities had errors")

if __name__ == "__main__":
    main()