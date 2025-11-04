#!/usr/bin/env python3
"""Apply geocoding enrichments batch 2 from CSV data."""

import json
import csv
from pathlib import Path
from io import StringIO
import unicodedata

# Try ftfy for mojibake fixing
try:
    import ftfy
    HAS_FTFY = True
except ImportError:
    HAS_FTFY = False

# Geocoding enrichments batch 2 - cited coordinates
CSV_DATA = """facility_id,country_iso3,raw_name,canonical_name,operator_display,primary_type,lat,lon,precision,town,region,aliases,commodities,notes
arm-kajaran-fac,ARM,Kajaran Mine,Kajaran Mine,Zangezur Copper & Molybdenum Combine,mine,39.14389,46.13778,site,Kajaran,Syunik,,Copper|Molybdenum,Mindat site coords.
arm-teghut-mine-fac,ARM,Teghut Mine,Teghut Mine,Teghout CJSC (historic Vallex/ACP),mine,41.11806,44.84583,region,Teghut,Lori,,Copper|Molybdenum,Village centroid; mine suspended 2018.
arm-kapan-fac,ARM,Kapan Mine,Kapan Mine,Chaarat Kapan,mine,39.20659,46.43303,site,Kapan,Syunik,,Gold|Silver|Copper|Zinc,Kapan complex / Shahumyan.
arm-amulsar-project-fac,ARM,Amulsar,Amulsar Gold Mine,Lydian Armenia,mine,39.736988,45.606874,site,Jermuk,Vayots Dzor,,Gold,HLF pad coords.
arm-ararat-gold-extraction-plant-fac,ARM,Ararat Plant,Ararat Gold Recovery Company Plant,GeoProMining Gold,plant,39.84472,44.74861,town,Ararat,Ararat,,Gold,Town/plant area; address corroboration.
arm-sotk-mine-fac,ARM,Sotk,Zod/Sotk Gold Mine,GeoProMining Gold,mine,40.23566,45.97103,site,Sotk,Gegharkunik,,Gold,Mindat locality; cross-checked.
arm-agarak-mine-fac,ARM,Agarak CMC,Agarak Copper-Molybdenum Mine,GeoProMining,processing_plant,38.91611,46.18944,site,Agarak,Syunik,,Copper|Molybdenum,Combine campus.
bel-hoboken-umicore-fac,BEL,Hoboken (Umicore),Hoboken PMR,Umicore PMR,smelter,51.16667,4.36667,town,Hoboken,Antwerp,,Precious metals,Plant at Adolf Greinerstraat 14; Hoboken centroid used.
bel-olen-umicore-fac,BEL,Olen (Umicore),Olen Multi-business Site,Umicore Olen,refinery,51.143,4.969,town,Olen,Antwerp,,Cobalt|Nickel|Germanium,Watertorenstraat 33 Olen; set town precision.
bel-prayon-engis-smelter-fac,BEL,Prayon Engis,Prayon Engis Plant,Prayon,plant,50.57400,5.38660,site,Engis,Liège,,Phosphates,Rue Joseph Wauters 144; site coords.
bfa-essakane-fac,BFA,Essakane,Essakane Mine,IAMGOLD 85% + Gov 15%,mine,14.38306,0.07611,site,Essakane/Séno,Sahel,,Gold,Mindat lat/lon; operator page.
bfa-hounde-fac,BFA,Houndé,Houndé Mine,Endeavour Mining 90% + Gov 10%,mine,11.42306,-3.53528,site,Houndé,Tuy,,Gold,Plant/power block area; Endeavour docs.
bfa-wahgnion-fac,BFA,Wahgnion,Wahgnion Mine,Government of Burkina Faso,mine,10.37528,-5.38498,region,Banfora,Cascades,,Gold,Power plant/industrial pad inside mine area.
bfa-yaramoko-fac,BFA,Yaramoko,Yaramoko Mine,Fortuna Silver Mines,mine,11.80000,-3.28333,region,Safané,Balé,,Gold,Mine location ~42 km S of Safané; mine TR.
bfa-mana-fac,BFA,Mana,Mana Mine,Endeavour Mining (historic),mine,11.99333,-3.41778,site,Kona Dept.,Boucle du Mouhoun,,Gold,Wona–Kona deposit centroids (Mana).
bfa-taparko-fac,BFA,Taparko,Taparko Mine,Nordgold (historic),mine,13.52167,-0.34833,site,Taparko,Namentenga,,Gold,Mine locality coordinates.
bfa-karma-fac,BFA,Karma,Karma Mine,Néré Mining,mine,13.604164,-2.291971,region,Ouahigouya,Yatenga,,Gold,Recent report lat/lon.
zaf-amandelbult-fac,ZAF,Amandelbult,Amandelbult Mine,Anglo American Platinum,mine,-24.78576,27.35435,site,Thabazimbi,Limpopo,,PGMs,Mine locality coordinates.
zaf-bathopele-fac,ZAF,Bathopele,Bathopele Mine,Sibanye-Stillwater,mine,-25.6745,27.3417,region,Rustenburg,North West,,PGMs,Near Waterval/Frank shafts; confirm on next pass.
zaf-anglo-american-converter-plant-waterval-smelter-fac,ZAF,Waterval,Waterval Smelter,Anglo American Platinum,smelter,-25.67526,27.32613,site,Rustenburg,North West,,PGMs,Smelter campus coords.
zaf-anglo-american-platinum-base-metals-refinery-fac,ZAF,RBMR,Rustenburg Base Metals Refinery,Anglo American Platinum,refinery,-25.68778,27.33806,site,Rustenburg,North West,,Nickel|Copper|Cobalt,RBMR footprint; corporate docs corroborate.
zaf-anglo-american-platinum-precious-metals-refinery-fac,ZAF,PMR,Precious Metals Refinery,Anglo American Platinum,refinery,,,town,Brakpan,Gauteng,,PGMs,1 Platinum Road Vulcania; set town precision until plot pin."""

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
    """Apply geocoding enrichments batch 2."""
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
