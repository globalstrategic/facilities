#!/usr/bin/env python3
"""Apply geocoding enrichments from CSV data."""

import json
import csv
from pathlib import Path
from io import StringIO

# Geocoding enrichments batch
CSV_DATA = """facility_id,country_iso3,raw_name,canonical_name,operator_display,primary_type,lat,lon,precision,town,region,aliases,commodities,notes
alb-burrel-ferrochrome-plant-fac,ALB,Burrel Ferrochrome Plant,Burrel Ferrochrome Smelter,AlbChrome (YILDIRIM Group),smelter,41.60833,20.01111,town,Burrel,Dibër,,Ferrochrome,Plant near Burrel; AlbChrome acquired by YILDIRIM (Jan 2022)
alb-lak-roshi-mine-fac,ALB,Lak Roshi Mine,Lak Roshi Mine,,mine,42.05200,20.08800,site,Fushë-Arrëz,Shkodër,,Copper,Mindat locality gives deposit centroid
alb-mamez-deposit-fac,ALB,Mamez Deposit,Mamez,,facility,42.06139,20.37139,town,Mamëz,Kukës,,Nickel|Iron Ore|Cobalt,Populated-place centroid for Mamëz
alb-munella-mine-fac,ALB,Munella Mine,Munella Mine,,mine,41.96700,20.10000,site,Mirditë,Lezhë,,Copper|Gold|Silver|Zinc,Munellë mine locality coords
alb-spa-mine-fac,ALB,Spaç Mine,Spaç Mine,,mine,41.896265,20.056377,site,Orosh,Lezhë,,Copper,Spaç copper mine coordinates
are-arabian-gulf-steel-industries-fac,ARE,Arabian Gulf Steel Industries (AGSI),Arabian Gulf Steel Industries Steel Plant,Arabian Gulf Steel Industries,steel_plant,24.283335,54.463384,site,Abu Dhabi,Mussafah – ICAD II,,Steel,Exact plant coords (ICAD II)
are-emirates-gold-dmcc-fac,ARE,Emirates Gold DMCC,Emirates Gold DMCC Refinery,Emirates Gold DMCC,refinery,25.068103,55.139089,site,Dubai,JLT – Cluster I (Platinum Tower),,Gold|Silver,Office/plant address in Platinum Tower; tower centroid used
are-emirates-minting-factory-llc-fac,ARE,Emirates Minting Factory LLC,Emirates Minting Factory LLC Refinery,Emirates Minting Factory LLC,refinery,25.134415,55.245258,approximate,Dubai,Al Quoz Industrial Area 2,,Gold|Silver,District centroid; address confirmed in Al Quoz Ind. 2
are-emirates-national-copper-factory-fac,ARE,Emirates National Copper Factory (NUHAS),Emirates National Copper Factory Processing Plant,Emirates National Copper Factory,processing_plant,24.272500,54.473333,site,Abu Dhabi,Mussafah – ICAD 3,,Copper|rods|PICC,Factory ICAD-3; DMS→DD conversion
are-emirates-steel-llc-fac,ARE,Emirates Steel LLC,Emirates Steel LLC Steel Plant,Emirates Steel Arkan,steel_plant,24.322033,54.467987,site,Abu Dhabi,Mussafah – ICAD,,Steel,Exact plant coords
are-fujairah-gold-fzc-fac,ARE,Fujairah Gold FZC,Fujairah Gold FZC Refinery,Fujairah Gold FZC (Vedanta),refinery,25.411076,56.248228,site,Fujairah,Fujairah Free Zone II,,Gold|precious metals,FFZ II plant centroid
are-gulf-gold-refinery-fze-fac,ARE,Gulf Gold Refinery FZE,Gulf Gold FZE Refinery,Gulf Gold Refinery FZE,refinery,25.345453,55.485305,site,Sharjah,SAIF Zone,,Gold|Silver,SAIF warehouse/office coordinates
are-jebel-ali-smelter-fac,ARE,Jebel Ali Smelter (DUBAL),Jebel Ali Smelter,Emirates Global Aluminium (EGA),smelter,25.044801,55.155846,site,Dubai,Jebel Ali,,aluminum,Exact power/smelter campus coordinates
are-gulf-cement-company-fac,ARE,Gulf Cement Company,Gulf Cement Company Plant,Gulf Cement Company PJSC,plant,25.972902,56.070428,site,Ras Al Khaimah,Khor Khwair,,cement|clinker|ggbs,Exact plant coords
are-rak-white-cement-fac,ARE,RAK White Cement,RAK White Cement Plant,RAK Co. for White Cement & Construction Materials,plant,25.983932,56.075012,town,Ghalilah (Ras Al Khaimah),Khor Khwair/Ghalilah,,white cement,Town centroid near plant cluster
are-khor-khwair-quarry-fac,ARE,Khor Khwair Quarry,Khor Khwair Quarry Mine,Stevin Rock,mine,,,region,Khor Khwair,Ras Al Khaimah,,limestone|aggregates|armour rock,Large multi-operator quarry district; leave lat/lon blank until pit centroid chosen
are-kadra-quarry-fac,ARE,Kadra Quarry,Kadra Quarry Mine,,mine,25.19013,56.00324,region,Kadrah,Ras Al Khaimah,,gabbro|aggregates,Village centroid; refine to pit polygon later
are-al-ghail-quarry-fac,ARE,Al Ghail Quarry,Al Ghail Quarry Mine,,mine,25.398400,56.055512,region,Al Ghail,Ras Al Khaimah,,dolomite|limestone,Industrial park centroid
are-hamriyah-steel-fac,ARE,Hamriyah Steel,Hamriyah Steel Steel Plant,Hamriyah Steel FZC,steel_plant,25.450185,55.504045,region,Al Hamriyah,HFZA (Sharjah),,Steel|HBI|Iron Ore,HFZA coordinate; refine to plot-level when available"""

def parse_csv_data():
    """Parse the CSV data into dictionaries."""
    reader = csv.DictReader(StringIO(CSV_DATA))
    enrichments = []
    for row in reader:
        # Convert lat/lon to float if present
        if row['lat']:
            row['lat'] = float(row['lat'])
        else:
            row['lat'] = None
        if row['lon']:
            row['lon'] = float(row['lon'])
        else:
            row['lon'] = None
        enrichments.append(row)
    return enrichments

def apply_enrichment(facility, enrich):
    """Apply enrichment data to facility JSON."""
    # Update location
    if "location" not in facility:
        facility["location"] = {}

    if enrich['lat'] is not None:
        facility["location"]["lat"] = enrich['lat']
    if enrich['lon'] is not None:
        facility["location"]["lon"] = enrich['lon']

    facility["location"]["precision"] = enrich['precision']

    # Handle town
    if enrich['town']:
        facility["location"]["town"] = enrich['town']
    elif enrich['precision'] == 'region':
        # Clear town for region-level
        facility["location"]["town"] = None

    # Handle region
    if enrich['region']:
        facility["location"]["region"] = enrich['region']

    # Update operator display if provided
    if enrich['operator_display']:
        facility["operator_display"] = enrich['operator_display']

    # Update primary type if provided and different
    if enrich['primary_type'] and enrich['primary_type'] != 'facility':
        facility["primary_type"] = enrich['primary_type']
        # Estimate confidence based on data quality
        if enrich['precision'] in ['site', 'exact']:
            facility["type_confidence"] = 0.95
        elif enrich['precision'] == 'town':
            facility["type_confidence"] = 0.85
        else:
            facility["type_confidence"] = 0.75

    # Set data quality flags
    if "data_quality" not in facility:
        facility["data_quality"] = {}
    if "flags" not in facility["data_quality"]:
        facility["data_quality"]["flags"] = {}

    # Update flags based on enrichment
    if not enrich['town']:
        facility["data_quality"]["flags"]["town_missing"] = True
    else:
        facility["data_quality"]["flags"]["town_missing"] = False

    if not enrich['operator_display']:
        facility["data_quality"]["flags"]["operator_unresolved"] = True
    else:
        facility["data_quality"]["flags"]["operator_unresolved"] = False

    # Add geocoding source to verification
    if "verification" not in facility:
        facility["verification"] = {}
    facility["verification"]["geocoding_source"] = enrich.get('notes', 'External enrichment')

    return facility

def main():
    """Apply geocoding enrichments to facilities."""
    enrichments = parse_csv_data()
    print(f"Loaded {len(enrichments)} enrichments")

    applied = 0
    errors = 0

    for enrich in enrichments:
        fac_id = enrich['facility_id']
        country = fac_id.split('-')[0].upper()
        fac_path = Path(f"facilities/{country}/{fac_id}.json")

        if not fac_path.exists():
            print(f"Warning: {fac_path} not found")
            errors += 1
            continue

        try:
            # Load facility
            with open(fac_path, 'r', encoding='utf-8') as f:
                facility = json.load(f)

            # Apply enrichment
            facility = apply_enrichment(facility, enrich)

            # Write back
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
    if errors:
        print(f"⚠ {errors} facilities had errors")

if __name__ == "__main__":
    main()