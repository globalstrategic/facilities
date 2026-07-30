#!/usr/bin/env python3
"""
Extract every facility for a given commodity into a self-contained package.

Scans facilities/{ISO3}/*.json, selects the facilities matching a commodity's
mineral/compound term list, and writes three views of the same result set:

    lithium-facilities/facilities/   individual JSONs (byte-identical copies)
    lithium-facilities/index.json    single-file manifest + summary counts
    lithium-facilities/README.md     folder landing page
    docs/LITHIUM_FACILITIES.md       human-readable report
    docs/lithium_facilities.csv      flat one-row-per-facility export

Facilities are matched on three tiers of evidence:

    A  the commodity appears in the facility's commodities[]
    B  the commodity appears in the facility name or aliases
    C  the commodity appears only in verification/enrichment notes

Tier C is noisy - "lithium" turns up inside company names such as "Mali
Lithium" or "Sinowin Lithium" on facilities that mine gold or mineral sands.
Tier C therefore requires an explicit allowlist (VETTED_TIER_C); everything
else that only matches in notes is routed to the excluded-review list with a
reason, rather than being silently kept or dropped.

Usage:
    python scripts/tools/extract_commodity_package.py                # lithium
    python scripts/tools/extract_commodity_package.py --check        # no writes
"""

import argparse
import csv
import json
import os
import re
import shutil
import sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
FACILITIES = REPO / "facilities"

# --------------------------------------------------------------------------
# Commodity definition
# --------------------------------------------------------------------------

LITHIUM = {
    "slug": "lithium",
    "label": "Lithium",
    "out_dir": "lithium-facilities",
    "report_md": "docs/LITHIUM_FACILITIES.md",
    "report_csv": "docs/lithium_facilities.csv",
    # Minerals and compounds that mean "this site is a lithium site".
    "terms": [
        "lithium", "spodumene", "petalite", "lepidolite", "amblygonite",
        "zinnwaldite", "eucryptite", "hectorite", "jadarite", "li2o", "lce",
    ],
    # Tier C facilities reviewed by hand and confirmed as genuine lithium sites.
    # Each has lithium documented in its notes but missing from commodities[].
    "vetted_tier_c": {
        "arg-centenario-ratones-fac", "arg-f-nix-project-fac", "arg-mariana-fac",
        "aus-mount-edwards-fac", "chl-la-negra-plant-fac", "chl-salar-del-carmen-fac",
        "cmr-ndom-licence-area-fac", "cod-manono-kitolo-project-fac",
        "cze-c-novec-project-fac", "fra-echassi-res-district-fac",
        "irl-avalonia-project-fac", "irl-leinster-project-fac",
        "mdg-millie-s-reward-project-fac", "zwe-emerald-mine-fac",
        "zwe-sabi-star-mine-fac", "zwe-sandawana-mine-fac",
    },
    # Tier C facilities reviewed by hand and rejected, with the reason shown in
    # the report so the call can be audited or reversed.
    "rejected_tier_c": {
        "alb-reps-fac":
            "Copper mine in Albania; note describes Albemarle's Silver Peak (Nevada) "
            "lithium site and the province field says 'Nevada' - record looks corrupted.",
        "can-becancour-fac":
            "Aluminium/silicon smelter complex. Nemaska Lithium is one of nine companies "
            "listed; Becancour does host a separate Nemaska lithium plant.",
        "mli-morila-fac":
            "Gold mine. 'Mali Lithium' is a former corporate name of Firefinch, not a commodity.",
        "moz-corridor-sands-projects-fac":
            "Heavy mineral sands (Ti/Zr/Hf). 'Sinowin Lithium' is a company name only.",
        "chn-huichun-smelter-fac":
            "Copper smelter; note only says companies are 'involved in lithium mining' generically.",
        "chl-el-rincon-fac":
            "Listed as Copper in Antofagasta. Rio Tinto's Rincon lithium project is in Salta, "
            "Argentina - probable conflation.",
        "aus-galaxy-fac":
            "Listed as Gold, but owners (Galaxy Resources / Orocobre) and coordinates near "
            "Ravensthorpe WA point to the Mt Cattlin lithium mine.",
        "aus-pertha-may-fac":
            "Listed as Gold at Perth CBD coordinates, but companies are the Greenbushes owner "
            "set (Talison/Tianqi/Albemarle/IGO). Probable junk record.",
        "chn-xiangyuan-jinxing-coal-mine-fac":
            "Note says Zijin's 'Xiangyuan Hard-Rock Lithium-containing Polymetallic Mine', but "
            "commodities say met coal, province says Hunan and coordinates are in Shanxi.",
    },
}

# --------------------------------------------------------------------------
# Normalisation
# --------------------------------------------------------------------------

# Collapse casing and spelling variants so the by-product tally does not split
# "Iron Ore"/"iron ore" or "Cesium"/"Caesium" into separate rows.
SYNONYM = {
    "cesium": "Caesium",
    "caesium": "Caesium",
    "iron ore": "Iron ore",
    "rees": "Rare earths",
    "rare earths": "Rare earths",
    "rare earth elements": "Rare earths",
    "neodymium-praseodymium": "Neodymium-praseodymium (NdPr)",
    "aluminum (from bauxite)": "Aluminium (from bauxite)",
    "niobium (columbite)": "Niobium (columbite)",
    "nickel (as mhp/sulfate)": "Nickel (as MHP/sulfate)",
}

# Placeholder values that appear as literal commodity strings on a few records.
PLACEHOLDER = {"n/a", "not specified", "other critical minerals", ""}

ISO3_NAME = {
    "AFG": "Afghanistan", "ALB": "Albania", "ARE": "United Arab Emirates", "ARG": "Argentina",
    "AUS": "Australia", "AUT": "Austria", "BEL": "Belgium", "BGR": "Bulgaria", "BOL": "Bolivia",
    "BRA": "Brazil", "BWA": "Botswana", "CAN": "Canada", "CHE": "Switzerland", "CHL": "Chile",
    "CHN": "China", "CIV": "Cote d'Ivoire", "CMR": "Cameroon", "COD": "DR Congo",
    "COG": "Congo-Brazzaville", "COL": "Colombia", "CZE": "Czechia", "DEU": "Germany",
    "EGY": "Egypt", "ESP": "Spain", "ETH": "Ethiopia", "FIN": "Finland", "FRA": "France",
    "GBR": "United Kingdom", "GHA": "Ghana", "GRC": "Greece", "HUN": "Hungary", "IDN": "Indonesia",
    "IND": "India", "IRL": "Ireland", "IRN": "Iran", "ISR": "Israel", "ITA": "Italy",
    "JPN": "Japan", "KAZ": "Kazakhstan", "KEN": "Kenya", "KOR": "South Korea", "LAO": "Laos",
    "LUX": "Luxembourg", "MAR": "Morocco", "MDG": "Madagascar", "MEX": "Mexico", "MLI": "Mali",
    "MMR": "Myanmar", "MNE": "Montenegro", "MNG": "Mongolia", "MOZ": "Mozambique",
    "MWI": "Malawi", "MYS": "Malaysia", "NAM": "Namibia", "NER": "Niger", "NGA": "Nigeria",
    "NLD": "Netherlands", "NOR": "Norway", "NPL": "Nepal", "NZL": "New Zealand",
    "PAK": "Pakistan", "PER": "Peru", "PHL": "Philippines", "POL": "Poland", "PRT": "Portugal",
    "ROU": "Romania", "RUS": "Russia", "RWA": "Rwanda", "SAU": "Saudi Arabia", "SLE": "Sierra Leone",
    "SRB": "Serbia", "SVK": "Slovakia", "SVN": "Slovenia", "SWE": "Sweden", "THA": "Thailand",
    "TKM": "Turkmenistan", "TUR": "Turkey", "TWN": "Taiwan", "TZA": "Tanzania", "UGA": "Uganda",
    "UKR": "Ukraine", "USA": "United States", "UZB": "Uzbekistan", "VNM": "Vietnam",
    "ZAF": "South Africa", "ZMB": "Zambia", "ZWE": "Zimbabwe",
}


def country_name(iso3):
    return ISO3_NAME.get(iso3, iso3)


def clean_metal(raw):
    """Strip trailing footnote digits and collapse whitespace.

    Source reports leave artefacts like 'gallium 28' or 'tungsten (scheelite) 38'.
    """
    m = (raw or "").strip()
    m = re.sub(r"\s+\d+$", "", m)
    m = re.sub(r"\s+", " ", m)
    return m


def canon_metal(m):
    key = (m or "").strip().lower()
    return SYNONYM.get(key, (m or "").strip())


def canon_list(metals):
    out, seen = [], set()
    for m in metals:
        c = canon_metal(m)
        if c.lower() in PLACEHOLDER or c.lower() in seen:
            continue
        seen.add(c.lower())
        out.append(c)
    return sorted(out)


def titlecase(m):
    """Uppercase a leading lowercase letter, leaving acronyms and MixedCase alone."""
    if not m:
        return m
    if m.isupper() or (m[0].isupper() and any(c.isupper() for c in m[1:])):
        return m
    return m[0].upper() + m[1:]


def companies_of(rec):
    """company_mentions holds either bare strings or {name, role, ...} dicts."""
    out = []
    for c in rec.get("company_mentions") or []:
        if isinstance(c, dict):
            name = (c.get("name") or "").strip()
            role = (c.get("role") or "").strip()
            if name and name.lower() not in {"unclear", "not specified", "unknown"}:
                out.append(f"{name} ({role})" if role else name)
        elif isinstance(c, str) and c.strip():
            out.append(c.strip())
    seen, uniq = set(), []
    for c in out:
        if c.lower() not in seen:
            seen.add(c.lower())
            uniq.append(c)
    return uniq


def location_of(rec):
    parts = []
    loc = rec.get("location") or {}
    town = rec.get("town") or loc.get("town")
    if town:
        parts.append(town)
    if loc.get("region"):
        parts.append(loc["region"])
    if rec.get("province"):
        parts.append(rec["province"])
    parts.append(country_name(rec.get("country_iso3")))
    seen, uniq = set(), []
    for p in parts:
        if p and p.lower() not in seen:
            seen.add(p.lower())
            uniq.append(p)
    return ", ".join(uniq)


def coords_of(rec):
    loc = rec.get("location") or {}
    lat, lon = loc.get("lat"), loc.get("lon")
    if lat is None or lon is None or (lat == 0 and lon == 0):
        return ""
    return f"{lat:.4f}, {lon:.4f}"


def notes_blob(rec):
    return json.dumps(rec.get("verification", {})) + json.dumps(rec.get("enrichment_notes", {}))


# --------------------------------------------------------------------------
# Selection
# --------------------------------------------------------------------------

def select(spec):
    pattern = re.compile(r"\b(" + "|".join(spec["terms"]) + r")\b", re.I)
    selected, rejected, unreviewed = [], [], []
    total = 0

    for path in sorted(FACILITIES.glob("*/*.json")):
        try:
            rec = json.loads(path.read_text())
        except (json.JSONDecodeError, OSError) as exc:
            print(f"  skip {path.name}: {exc}", file=sys.stderr)
            continue
        total += 1

        fid = rec.get("facility_id", "")
        commodities = [c for c in (clean_metal(c.get("metal", ""))
                                   for c in (rec.get("commodities") or [])) if c]
        target = [c for c in commodities if pattern.search(c)]
        others = [c for c in commodities if not pattern.search(c)]

        name_blob = " ".join(
            [str(rec.get("name", "")), str(rec.get("canonical_name", "")),
             str(rec.get("display_name", ""))] + list(rec.get("aliases") or []))
        in_name = bool(pattern.search(name_blob))
        in_notes = bool(pattern.search(notes_blob(rec)))

        if target:
            tier, evidence = "A", f"{spec['slug']} listed in commodities"
        elif in_name:
            tier, evidence = "B", f"{spec['slug']} in facility name/aliases"
        elif in_notes and fid in spec["vetted_tier_c"]:
            tier, evidence = "C", f"{spec['slug']} documented in verification/research notes"
        elif in_notes:
            entry = {
                "facility_id": fid,
                "name": rec.get("canonical_name") or rec.get("name"),
                "country": country_name(rec.get("country_iso3")),
                "location": location_of(rec),
                "coords": coords_of(rec),
                "commodities": commodities,
                "reason": spec["rejected_tier_c"].get(
                    fid, "notes-only match, not yet reviewed"),
                "path": str(path.relative_to(REPO)),
            }
            (rejected if fid in spec["rejected_tier_c"] else unreviewed).append(entry)
            continue
        else:
            continue

        selected.append({
            "facility_id": fid,
            "name": rec.get("canonical_name") or rec.get("name"),
            "display_name": rec.get("display_name") or rec.get("name"),
            "country_iso3": rec.get("country_iso3", "???"),
            "country": country_name(rec.get("country_iso3")),
            "location": location_of(rec),
            "province": rec.get("province"),
            "town": rec.get("town") or (rec.get("location") or {}).get("town"),
            "lat": (rec.get("location") or {}).get("lat"),
            "lon": (rec.get("location") or {}).get("lon"),
            "coordinate_precision": (rec.get("location") or {}).get("precision"),
            "coords": coords_of(rec),
            "types": rec.get("types") or [],
            "primary_type": rec.get("primary_type"),
            "status": rec.get("status") or "unknown",
            "commodity_forms": sorted({titlecase(t) for t in target}),
            "coproducts_byproducts": canon_list(titlecase(o) for o in others),
            "companies": companies_of(rec),
            "evidence_tier": tier,
            "evidence": evidence,
            "verification_confidence": (rec.get("verification") or {}).get("confidence"),
            "source_file": str(path.relative_to(REPO)),
            "file": f"facilities/{path.name}",
        })

    selected.sort(key=lambda r: (r["country"] or "", r["name"] or ""))
    review = sorted(rejected + unreviewed, key=lambda r: (r["country"] or "", r["facility_id"]))
    return selected, review, total


# --------------------------------------------------------------------------
# Writers
# --------------------------------------------------------------------------

def esc(s):
    return (s or "").replace("|", "\\|")


def joined(xs, empty="—"):
    return ", ".join(xs) if xs else empty


def build_manifest(spec, recs, review, total):
    by_country = Counter(r["country"] for r in recs)
    byprod = Counter()
    for r in recs:
        byprod.update(r["coproducts_byproducts"])
    return {
        "title": f"{spec['label']} facilities",
        "description": (f"Every facility in the parent database that produces, refines, recycles "
                        f"or is being explored for {spec['slug']}, with location and co-product "
                        f"metals."),
        "generated": date.today().isoformat(),
        "source": "facilities/{ISO3}/*.json",
        "source_record_count": total,
        "facility_count": len(recs),
        "country_count": len(by_country),
        "facilities_with_coproducts": sum(1 for r in recs if r["coproducts_byproducts"]),
        "selection_criteria": {
            "terms": spec["terms"],
            "tiers": {
                "A": f"{spec['slug']} listed in the facility's commodities[]",
                "B": f"{spec['slug']} in the facility name or aliases (commodities incomplete)",
                "C": f"{spec['slug']} documented only in research/verification notes (vetted)",
            },
            "tier_counts": dict(Counter(r["evidence_tier"] for r in recs)),
            "excluded": ("matches where the commodity name appeared only inside a company name; "
                         "see excluded_review"),
        },
        "counts_by_country": dict(by_country.most_common()),
        "counts_by_type": dict(Counter((r["types"] or ["unknown"])[0] for r in recs).most_common()),
        "counts_by_status": dict(Counter(r["status"] for r in recs).most_common()),
        "coproduct_frequency": dict(byprod.most_common()),
        "excluded_review": review,
        "facilities": recs,
    }


def write_package(spec, manifest, recs):
    out = REPO / spec["out_dir"]
    if out.is_dir():
        shutil.rmtree(out)
    (out / "facilities").mkdir(parents=True)

    for r in recs:
        shutil.copy2(REPO / r["source_file"], out / "facilities" / Path(r["source_file"]).name)

    payload = dict(manifest)
    # Drop the per-record display-only field from the manifest copy.
    payload["facilities"] = [{k: v for k, v in r.items() if k != "coords"} for r in recs]
    (out / "index.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n")

    write_folder_readme(spec, manifest, recs, out)
    return out


def write_folder_readme(spec, manifest, recs, out):
    by_country = Counter(r["country"] for r in recs)
    iso_of = {r["country"]: r["country_iso3"] for r in recs}
    byprod = Counter(manifest["coproduct_frequency"])
    md, A = [], None
    A = md.append

    A(f"# {spec['out_dir']}")
    A("")
    A(f"{len(recs)} {spec['slug']} facilities across {len(by_country)} countries, extracted from "
      f"the {manifest['source_record_count']:,} records in [`../facilities/`](../facilities).")
    A("")
    A("## Contents")
    A("")
    A("| Path | What it is |")
    A("|------|------------|")
    A("| [`index.json`](index.json) | Single-file manifest: every facility with location, "
      "commodity form, co-products, companies and evidence tier, plus summary counts and the "
      "excluded-match review list. |")
    A(f"| [`facilities/`](facilities) | {len(recs)} individual facility JSONs, byte-identical "
      "copies of their source records in `../facilities/{ISO3}/`. Filenames carry the ISO3 "
      "prefix, so an alphabetical listing groups by country. |")
    A(f"| [`../{spec['report_md']}`](../{spec['report_md']}) | Human-readable report with the full "
      "tables and analysis. |")
    A(f"| [`../{spec['report_csv']}`](../{spec['report_csv']}) | Flat CSV, one row per facility. |")
    A("")
    A("The per-facility JSONs are unmodified copies. All commodity-specific annotation "
      "(`evidence_tier`, cleaned `coproducts_byproducts`) lives in `index.json` only, keyed by "
      "`facility_id`.")
    A("")
    A("## Selection")
    A("")
    A("A facility is included if one of these terms appears in its commodities, name/aliases, or "
      "research notes: " + ", ".join(f"`{t}`" for t in spec["terms"]) + ".")
    A("")
    A("| Tier | Evidence | Count |")
    A("|------|----------|------:|")
    for t in ("A", "B", "C"):
        A(f"| {t} | {manifest['selection_criteria']['tiers'][t]} | "
          f"{manifest['selection_criteria']['tier_counts'].get(t, 0)} |")
    A("")
    A(f"{len(manifest['excluded_review'])} records where the commodity name appeared only inside a "
      "company name were excluded; they are listed under `excluded_review` in `index.json`.")
    A("")
    A("## Countries")
    A("")
    A("| Country | ISO3 | Facilities |")
    A("|---------|------|-----------:|")
    for c, n in by_country.most_common():
        A(f"| {esc(c)} | {iso_of[c]} | {n} |")
    A("")
    A("## Co-products and by-products")
    A("")
    A(f"{manifest['facilities_with_coproducts']} of {len(recs)} facilities have at least one "
      "other metal recorded. Top 15:")
    A("")
    A("| Metal / mineral | Sites |")
    A("|-----------------|------:|")
    for m, n in byprod.most_common(15):
        A(f"| {esc(m)} | {n} |")
    A("")
    A("Full frequency table in `index.json` (`coproduct_frequency`).")
    A("")
    A("## Regenerating")
    A("")
    A("This folder is derived output. Rebuild it rather than editing files here — edits made here "
      "do not flow back to the source records.")
    A("")
    A("```bash")
    A("python scripts/tools/extract_commodity_package.py")
    A("```")
    A("")
    (out / "README.md").write_text("\n".join(md) + "\n")


def write_csv(spec, recs):
    path = REPO / spec["report_csv"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["facility_id", "name", "country_iso3", "country", "location", "lat_lon",
                    "type", "status", f"{spec['slug']}_form", "coproducts_byproducts",
                    "companies", "evidence_tier", "source_file"])
        for r in recs:
            w.writerow([r["facility_id"], r["name"], r["country_iso3"], r["country"],
                        r["location"], r["coords"], ", ".join(r["types"]), r["status"],
                        "; ".join(r["commodity_forms"]), "; ".join(r["coproducts_byproducts"]),
                        "; ".join(r["companies"]), r["evidence_tier"], r["source_file"]])
    return path


def write_report(spec, manifest, recs):
    by_country = Counter(r["country"] for r in recs)
    byprod = Counter(manifest["coproduct_frequency"])
    byprod_sites = defaultdict(list)
    for r in recs:
        for m in r["coproducts_byproducts"]:
            byprod_sites[m].append(r["name"])

    md, A = [], None
    A = md.append
    A(f"# {spec['label']} Facilities")
    A("")
    A(f"Every facility in this database that produces, refines, recycles or is being explored for "
      f"{spec['slug']} — with its location and any other metals recorded at the same site.")
    A("")
    A(f"**{len(recs)} facilities** across **{len(by_country)} countries**. "
      f"**{manifest['facilities_with_coproducts']}** have at least one co-product or by-product "
      f"recorded.")
    A("")
    A(f"*Generated {manifest['generated']} from `facilities/{{ISO3}}/*.json` "
      f"({len(recs)} of {manifest['source_record_count']:,} facility records matched). "
      f"Machine-readable versions: [`{Path(spec['report_csv']).name}`]"
      f"({Path(spec['report_csv']).name}) and "
      f"[`{spec['out_dir']}/index.json`](../{spec['out_dir']}/index.json).*")
    A("")
    A("---")
    A("")
    A("## How facilities were selected")
    A("")
    A("A facility is included if its commodities, name/aliases, or research notes contain a "
      "lithium mineral or compound ("
      + ", ".join(f"`{t}`" for t in spec["terms"]) + "):")
    A("")
    A("| Tier | Evidence | Count |")
    A("|------|----------|-------|")
    for t in ("A", "B", "C"):
        A(f"| {t} | {manifest['selection_criteria']['tiers'][t]} | "
          f"{manifest['selection_criteria']['tier_counts'].get(t, 0)} |")
    A("")
    A("Matches where *lithium* appeared only inside a company name (e.g. \"Mali Lithium\", "
      "\"Sinowin Lithium\") were excluded and are listed under "
      "[Needs review](#needs-review--excluded-matches) instead.")
    A("")
    A("**Co-products and by-products** are the non-lithium entries in each facility's "
      "`commodities[]` array. This reflects what the source reports recorded for the site, not a "
      "metallurgical assessment — some entries are district-level records that aggregate several "
      "deposits, so a long metal list does not always mean all of it comes out of the lithium "
      "ore.")
    A("")
    A("---")
    A("")
    A("## Summary")
    A("")
    A("### By country")
    A("")
    A("| Country | Facilities |")
    A("|---------|-----------:|")
    for c, n in by_country.most_common():
        A(f"| {esc(c)} | {n} |")
    A("")
    A("### By facility type")
    A("")
    A("| Type | Facilities |")
    A("|------|-----------:|")
    for t, n in Counter((r["types"] or ["unknown"])[0] for r in recs).most_common():
        A(f"| {esc(t)} | {n} |")
    A("")
    A("### By status")
    A("")
    A("| Status | Facilities |")
    A("|--------|-----------:|")
    for s, n in Counter(r["status"] for r in recs).most_common():
        A(f"| {esc(s)} | {n} |")
    A("")
    A("### Co-products and by-products, by frequency")
    A("")
    A("Metals recorded alongside lithium, across all lithium facilities.")
    A("")
    A("| Metal / mineral | Sites | Example sites |")
    A("|-----------------|------:|---------------|")
    for m, n in byprod.most_common():
        ex = byprod_sites[m][:3]
        more = f" (+{len(byprod_sites[m]) - 3} more)" if len(byprod_sites[m]) > 3 else ""
        A(f"| {esc(m)} | {n} | {esc(', '.join(ex))}{more} |")
    A("")
    A("Reading the table:")
    A("")
    A("- **Tantalum (22), niobium (15) and tin (13)** are the classic hard-rock pegmatite "
      "companions and the most commonly recovered genuine by-products of spodumene mining — "
      "Pilgangoora, Mt Holland, Mt Marion, Kenticha, Keliber, Uis, Kolmozerskoye, Mina do Romano, "
      "Zambezia, and the whole Ugandan pegmatite set. Tin alone carries Greenbushes, Wodgina, "
      "Bald Hill and Mibra.")
    A("- **Caesium (4) and rubidium (2)** show up at the most fractionated pegmatites — Arcadia, "
      "Kenticha, The Mount, Comet Vale, Uis.")
    A("- **Potash and magnesium** are brine by-products, recorded at Salar de Atacama, where they "
      "are separated from the lithium-bearing brine.")
    A("- **Feldspar and quartz (5 each), plus kaolin** are industrial-mineral by-products of "
      "pegmatite processing — the whole Portuguese cluster and Wolfsberg, plus Greenbushes for "
      "kaolin.")
    A("- **Nickel (13), gold (15), copper (11) and cobalt (7)** mostly do *not* come out of "
      "lithium ore. They appear because the record covers a WA district or camp where a lithium "
      "deposit sits beside an existing nickel or gold operation (Kalgoorlie, Norseman, "
      "Forrestania, Higginsville). Treat these as \"other metals at this site\", not as lithium "
      "by-products.")
    A("- **Cobalt, nickel and graphite in sulfate/hydroxide form** come from the battery-recycling "
      "and refining plants in the list (Circu Li-ion, SK Tes Rotterdam, EcoNiLi, "
      "COBCO/Managem), where they are co-recovered from black mass alongside lithium.")
    A("- The **16-element list on Nikšić (Montenegro)** is a bauxite / red-mud critical-minerals "
      "record — lithium, gallium, vanadium and REEs recovered from alumina residue rather than "
      "from a lithium deposit. It is the only entry of its kind here and skews the single-site "
      "rows in the table above.")
    A("")
    A("---")
    A("")
    A("## Full listing")
    A("")
    A("Grouped by country, alphabetical by facility name.")
    A("")
    current = None
    for r in recs:
        if r["country"] != current:
            current = r["country"]
            A("")
            A(f"### {esc(current)} ({r['country_iso3']}) — {by_country[current]}")
            A("")
            A("| Facility | Location | Coordinates | Type | Status | Lithium form | "
              "Co-products / by-products |")
            A("|----------|----------|-------------|------|--------|--------------|"
              "---------------------------|")
        form = joined(r["commodity_forms"], "Lithium *(from notes)*")
        A(f"| **{esc(r['name'])}** | {esc(r['location'])} | `{r['coords']}` | "
          f"{esc(', '.join(r['types']))} | {esc(r['status'])} | {esc(form)} | "
          f"{esc(joined(r['coproducts_byproducts']))} |")
    A("")
    A("---")
    A("")
    A("## Appendix A — operators and owners named")
    A("")
    A("Company mentions recorded against each facility (Phase 1 raw mentions, not resolved to "
      "canonical company IDs).")
    A("")
    A("| Facility | Country | Companies mentioned |")
    A("|----------|---------|---------------------|")
    for r in recs:
        if r["companies"]:
            A(f"| {esc(r['name'])} | {esc(r['country'])} | {esc(', '.join(r['companies']))} |")
    A("")
    A("## Needs review — excluded matches")
    A("")
    A(f"{len(manifest['excluded_review'])} records mention lithium in their research notes but "
      "were **not** counted as lithium facilities. Most are cases where a company name contains "
      "the word \"lithium\". Several look like genuine data errors worth fixing.")
    A("")
    A("| Facility | Country | Recorded commodities | Why excluded |")
    A("|----------|---------|----------------------|--------------|")
    for x in manifest["excluded_review"]:
        A(f"| `{esc(x['facility_id'])}` | {esc(x['country'])} | "
          f"{esc(joined(x['commodities']))} | {esc(x['reason'])} |")
    A("")
    A("## Data gaps found while building this list")
    A("")
    gaps = [r for r in recs if r["evidence_tier"] == "C"]
    A(f"- **{len(gaps)} facilities have an empty or non-lithium `commodities[]` array** despite "
      "being documented lithium sites in their notes. They are included here at tier C, but the "
      "underlying JSON should be backfilled:")
    A("")
    for r in gaps:
        extra = (f" (currently lists: {esc(joined(r['coproducts_byproducts']))})"
                 if r["coproducts_byproducts"] else " (commodities empty)")
        A(f"  - `{r['facility_id']}` — {esc(r['name'])}, {esc(r['country'])}{extra}")
    A("")
    A("- Several commodity strings carry footnote digits or parentheticals from the source "
      "reports (`gallium 28`, `lithium (as lithium carbonate)`, `nickel (as MHP/sulfate)`). These "
      "were cleaned for display here but remain raw in the JSON.")
    A("- `not specified`, `n/a` and `other critical minerals` appear as literal commodity values "
      "on a few records and were dropped from the by-product tally.")
    A("")

    path = REPO / spec["report_md"]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(md) + "\n")
    return path


# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check", action="store_true",
                    help="report what would be selected without writing anything")
    args = ap.parse_args()

    spec = LITHIUM
    recs, review, total = select(spec)
    manifest = build_manifest(spec, recs, review, total)

    tiers = manifest["selection_criteria"]["tier_counts"]
    print(f"scanned  {total:,} facility records")
    print(f"selected {len(recs)} {spec['slug']} facilities in "
          f"{manifest['country_count']} countries "
          f"(A={tiers.get('A', 0)} B={tiers.get('B', 0)} C={tiers.get('C', 0)})")
    print(f"excluded {len(review)} notes-only matches")

    unreviewed = [x for x in review if x["reason"].endswith("not yet reviewed")]
    if unreviewed:
        print(f"\nWARNING: {len(unreviewed)} notes-only matches are not in the vetted or rejected "
              f"lists. Review them and add each to vetted_tier_c or rejected_tier_c:")
        for x in unreviewed:
            print(f"  {x['facility_id']}  {x['name']} ({x['country']}) — {x['commodities']}")

    if args.check:
        print("\n--check: no files written")
        return 0

    out = write_package(spec, manifest, recs)
    csv_path = write_csv(spec, recs)
    md_path = write_report(spec, manifest, recs)
    print(f"\nwrote {out.relative_to(REPO)}/  ({len(recs)} JSONs + index.json + README.md)")
    print(f"wrote {md_path.relative_to(REPO)}")
    print(f"wrote {csv_path.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
