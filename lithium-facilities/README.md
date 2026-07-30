# lithium-facilities

159 lithium facilities across 53 countries, extracted from the 10,444 records in [`../facilities/`](../facilities).

## Contents

| Path | What it is |
|------|------------|
| [`index.json`](index.json) | Single-file manifest: every facility with location, commodity form, co-products, companies and evidence tier, plus summary counts and the excluded-match review list. |
| [`facilities/`](facilities) | 159 individual facility JSONs, byte-identical copies of their source records in `../facilities/{ISO3}/`. Filenames carry the ISO3 prefix, so an alphabetical listing groups by country. |
| [`../docs/LITHIUM_FACILITIES.md`](../docs/LITHIUM_FACILITIES.md) | Human-readable report with the full tables and analysis. |
| [`../docs/lithium_facilities.csv`](../docs/lithium_facilities.csv) | Flat CSV, one row per facility. |

The per-facility JSONs are unmodified copies. All commodity-specific annotation (`evidence_tier`, cleaned `coproducts_byproducts`) lives in `index.json` only, keyed by `facility_id`.

## Selection

A facility is included if one of these terms appears in its commodities, name/aliases, or research notes: `lithium`, `spodumene`, `petalite`, `lepidolite`, `amblygonite`, `zinnwaldite`, `eucryptite`, `hectorite`, `jadarite`, `li2o`, `lce`.

| Tier | Evidence | Count |
|------|----------|------:|
| A | lithium listed in the facility's commodities[] | 137 |
| B | lithium in the facility name or aliases (commodities incomplete) | 6 |
| C | lithium documented only in research/verification notes (vetted) | 16 |

9 records where the commodity name appeared only inside a company name were excluded; they are listed under `excluded_review` in `index.json`.

## Countries

| Country | ISO3 | Facilities |
|---------|------|-----------:|
| Australia | AUS | 24 |
| United States | USA | 19 |
| Argentina | ARG | 10 |
| Portugal | PRT | 9 |
| Zimbabwe | ZWE | 8 |
| Uganda | UGA | 6 |
| Canada | CAN | 5 |
| Chile | CHL | 5 |
| Brazil | BRA | 4 |
| Ukraine | UKR | 4 |
| China | CHN | 3 |
| Cote d'Ivoire | CIV | 3 |
| DR Congo | COD | 3 |
| France | FRA | 3 |
| Mali | MLI | 3 |
| Mongolia | MNG | 3 |
| Morocco | MAR | 3 |
| Nigeria | NGA | 3 |
| Bolivia | BOL | 2 |
| Finland | FIN | 2 |
| Ireland | IRL | 2 |
| Namibia | NAM | 2 |
| Niger | NER | 2 |
| Pakistan | PAK | 2 |
| Austria | AUT | 1 |
| Cameroon | CMR | 1 |
| Czechia | CZE | 1 |
| Ethiopia | ETH | 1 |
| Ghana | GHA | 1 |
| India | IND | 1 |
| Indonesia | IDN | 1 |
| Luxembourg | LUX | 1 |
| Madagascar | MDG | 1 |
| Malawi | MWI | 1 |
| Malaysia | MYS | 1 |
| Mexico | MEX | 1 |
| Montenegro | MNE | 1 |
| Mozambique | MOZ | 1 |
| Nepal | NPL | 1 |
| Netherlands | NLD | 1 |
| New Zealand | NZL | 1 |
| Peru | PER | 1 |
| Russia | RUS | 1 |
| Saudi Arabia | SAU | 1 |
| Serbia | SRB | 1 |
| Sierra Leone | SLE | 1 |
| South Korea | KOR | 1 |
| Tanzania | TZA | 1 |
| Thailand | THA | 1 |
| Turkmenistan | TKM | 1 |
| Uzbekistan | UZB | 1 |
| Vietnam | VNM | 1 |
| Zambia | ZMB | 1 |

## Co-products and by-products

63 of 159 facilities have at least one other metal recorded. Top 15:

| Metal / mineral | Sites |
|-----------------|------:|
| Tantalum | 22 |
| Gold | 15 |
| Niobium | 15 |
| Tin | 13 |
| Nickel | 13 |
| Copper | 11 |
| Cobalt | 7 |
| Silver | 6 |
| Neodymium-praseodymium (NdPr) | 6 |
| Feldspar | 5 |
| Quartz | 5 |
| Iron ore | 4 |
| Bismuth | 4 |
| Caesium | 4 |
| Rare earths | 4 |

Full frequency table in `index.json` (`coproduct_frequency`).

## Regenerating

This folder is derived output. Rebuild it rather than editing files here — edits made here do not flow back to the source records.

```bash
python scripts/tools/extract_commodity_package.py
```

