# Lithium Facilities

Every facility in this database that produces, refines, recycles or is being explored for lithium — with its location and any other metals recorded at the same site.

**159 facilities** across **53 countries**. **63** have at least one co-product or by-product recorded.

*Generated 2026-07-30 from `facilities/{ISO3}/*.json` (159 of 10,444 facility records matched). Machine-readable versions: [`lithium_facilities.csv`](lithium_facilities.csv) and [`lithium-facilities/index.json`](../lithium-facilities/index.json).*

---

## How facilities were selected

A facility is included if its commodities, name/aliases, or research notes contain a lithium mineral or compound (`lithium`, `spodumene`, `petalite`, `lepidolite`, `amblygonite`, `zinnwaldite`, `eucryptite`, `hectorite`, `jadarite`, `li2o`, `lce`):

| Tier | Evidence | Count |
|------|----------|-------|
| A | lithium listed in the facility's commodities[] | 137 |
| B | lithium in the facility name or aliases (commodities incomplete) | 6 |
| C | lithium documented only in research/verification notes (vetted) | 16 |

Matches where *lithium* appeared only inside a company name (e.g. "Mali Lithium", "Sinowin Lithium") were excluded and are listed under [Needs review](#needs-review--excluded-matches) instead.

**Co-products and by-products** are the non-lithium entries in each facility's `commodities[]` array. This reflects what the source reports recorded for the site, not a metallurgical assessment — some entries are district-level records that aggregate several deposits, so a long metal list does not always mean all of it comes out of the lithium ore.

---

## Summary

### By country

| Country | Facilities |
|---------|-----------:|
| Australia | 24 |
| United States | 19 |
| Argentina | 10 |
| Portugal | 9 |
| Zimbabwe | 8 |
| Uganda | 6 |
| Canada | 5 |
| Chile | 5 |
| Brazil | 4 |
| Ukraine | 4 |
| China | 3 |
| Cote d'Ivoire | 3 |
| DR Congo | 3 |
| France | 3 |
| Mali | 3 |
| Mongolia | 3 |
| Morocco | 3 |
| Nigeria | 3 |
| Bolivia | 2 |
| Finland | 2 |
| Ireland | 2 |
| Namibia | 2 |
| Niger | 2 |
| Pakistan | 2 |
| Austria | 1 |
| Cameroon | 1 |
| Czechia | 1 |
| Ethiopia | 1 |
| Ghana | 1 |
| India | 1 |
| Indonesia | 1 |
| Luxembourg | 1 |
| Madagascar | 1 |
| Malawi | 1 |
| Malaysia | 1 |
| Mexico | 1 |
| Montenegro | 1 |
| Mozambique | 1 |
| Nepal | 1 |
| Netherlands | 1 |
| New Zealand | 1 |
| Peru | 1 |
| Russia | 1 |
| Saudi Arabia | 1 |
| Serbia | 1 |
| Sierra Leone | 1 |
| South Korea | 1 |
| Tanzania | 1 |
| Thailand | 1 |
| Turkmenistan | 1 |
| Uzbekistan | 1 |
| Vietnam | 1 |
| Zambia | 1 |

### By facility type

| Type | Facilities |
|------|-----------:|
| mine | 99 |
| plant | 50 |
| development | 4 |
| refinery | 2 |
| exploration | 2 |
| project-exploration | 1 |
| battery_recycling | 1 |

### By status

| Status | Facilities |
|--------|-----------:|
| unknown | 146 |
| operating | 9 |
| planned | 1 |
| suspended | 1 |
| closed | 1 |
| development | 1 |

### Co-products and by-products, by frequency

Metals recorded alongside lithium, across all lithium facilities.

| Metal / mineral | Sites | Example sites |
|-----------------|------:|---------------|
| Tantalum | 22 | Central Norseman Mine, Golden Eagle Nullagine Mine, Kalgoorlie Gold Ops Mine (+19 more) |
| Gold | 15 | Altura Mine, Bellevue Mine, British King Mine (+12 more) |
| Niobium | 15 | Bald Hill Central Mine, Finniss Mine, Kalgoorlie Gold Ops Mine (+12 more) |
| Tin | 13 | Altura Mine, Bald Hill Central Mine, Finniss Mine (+10 more) |
| Nickel | 13 | Bellevue Mine, Central Norseman Mine, Comet Vale Mine (+10 more) |
| Copper | 11 | Altura Mine, Comet Vale Mine, Higginsville Mine (+8 more) |
| Cobalt | 7 | Central Norseman Mine, Comet Vale Mine, Forrestania Nickel Mine (+4 more) |
| Silver | 6 | Altura Mine, Central Norseman Mine, Comet Vale Mine (+3 more) |
| Neodymium-praseodymium (NdPr) | 6 | Mityana Exploration, Mutaka Exploration, Ntungamo Exploration (+3 more) |
| Feldspar | 5 | Wolfsberg Lithium Mine, Adagói Facility, Canedo Covas Facility (+2 more) |
| Quartz | 5 | Wolfsberg Lithium Mine, Adagói Facility, Canedo Covas Facility (+2 more) |
| Iron ore | 4 | Altura Mine, White Dam Olary Mine, Wodgina Mine (+1 more) |
| Bismuth | 4 | Comet Vale Mine, Salar Del Carmen/salar De Atacama Plant, Hyakule Phakuwa Pegmatites Mine (+1 more) |
| Caesium | 4 | Comet Vale Mine, The Mount Mine, Kenticha Mine (+1 more) |
| Rare earths | 4 | Kenticha Mine, Karonga Lithium Exploration, Neelum Valley Exploration Exploration (+1 more) |
| Lanthanum | 3 | Comet Vale Mine, White Dam Olary Mine, Nikšić Bauxite Mines Facility |
| Tungsten | 3 | Comet Vale Mine, Echassières Mine, Uis Tin Mine |
| Palladium | 3 | Higginsville Mine, White Dam Olary Mine, Wiluna S Mine |
| Platinum | 3 | Higginsville Mine, White Dam Olary Mine, Wiluna S Mine |
| Lead | 2 | Comet Vale Mine, Mt Cattlin Mine |
| Zinc | 2 | Comet Vale Mine, Mt Cattlin Mine |
| Rubidium | 2 | Kalgoorlie Gold Ops Mine, Uis Tin Mine |
| Magnesium | 2 | Mt Marion Mine, Salar De Atacama Mine |
| Antimony | 2 | Wiluna S Mine, Hyakule Phakuwa Pegmatites Mine |
| Potash | 2 | Salar De Atacama Mine, Salar Del Carmen/salar De Atacama Plant |
| Graphite | 2 | Circu Li Ion Battery Upcycling Facility Battery Recycling, SK Tes Rotterdam Battery Recycling |
| Kaolin | 1 | Greenbushes Mine |
| Manganese | 1 | Mt Cattlin Mine |
| Silica | 1 | Mt Cattlin Mine |
| Uranium | 1 | White Dam Olary Mine |
| Nickel (as MHP/sulfate) | 1 | Econili Battery Recycling Plants Battery Recycling |
| Aluminium (from bauxite) | 1 | Nikšić Bauxite Mines Facility |
| Cerium | 1 | Nikšić Bauxite Mines Facility |
| Dysprosium | 1 | Nikšić Bauxite Mines Facility |
| Erbium | 1 | Nikšić Bauxite Mines Facility |
| Europium | 1 | Nikšić Bauxite Mines Facility |
| Gadolinium | 1 | Nikšić Bauxite Mines Facility |
| Gallium | 1 | Nikšić Bauxite Mines Facility |
| Neodymium | 1 | Nikšić Bauxite Mines Facility |
| Praseodymium | 1 | Nikšić Bauxite Mines Facility |
| Samarium | 1 | Nikšić Bauxite Mines Facility |
| Terbium | 1 | Nikšić Bauxite Mines Facility |
| Vanadium | 1 | Nikšić Bauxite Mines Facility |
| Yttrium | 1 | Nikšić Bauxite Mines Facility |
| Cobalt sulfate | 1 | Cobco Battery Materials Refinery |
| Nickel sulfate | 1 | Cobco Battery Materials Refinery |
| Cobalt (recycled) | 1 | Managem Glencore Recycling Refinery |
| Nickel (recycled) | 1 | Managem Glencore Recycling Refinery |
| Niobium (columbite) | 1 | Zambezia Pegmatite Mine |
| Cobalt (as hydroxide) | 1 | SK Tes Rotterdam Battery Recycling |
| Beryllium | 1 | Ntungamo Exploration |

Reading the table:

- **Tantalum (22), niobium (15) and tin (13)** are the classic hard-rock pegmatite companions and the most commonly recovered genuine by-products of spodumene mining — Pilgangoora, Mt Holland, Mt Marion, Kenticha, Keliber, Uis, Kolmozerskoye, Mina do Romano, Zambezia, and the whole Ugandan pegmatite set. Tin alone carries Greenbushes, Wodgina, Bald Hill and Mibra.
- **Caesium (4) and rubidium (2)** show up at the most fractionated pegmatites — Arcadia, Kenticha, The Mount, Comet Vale, Uis.
- **Potash and magnesium** are brine by-products, recorded at Salar de Atacama, where they are separated from the lithium-bearing brine.
- **Feldspar and quartz (5 each), plus kaolin** are industrial-mineral by-products of pegmatite processing — the whole Portuguese cluster and Wolfsberg, plus Greenbushes for kaolin.
- **Nickel (13), gold (15), copper (11) and cobalt (7)** mostly do *not* come out of lithium ore. They appear because the record covers a WA district or camp where a lithium deposit sits beside an existing nickel or gold operation (Kalgoorlie, Norseman, Forrestania, Higginsville). Treat these as "other metals at this site", not as lithium by-products.
- **Cobalt, nickel and graphite in sulfate/hydroxide form** come from the battery-recycling and refining plants in the list (Circu Li-ion, SK Tes Rotterdam, EcoNiLi, COBCO/Managem), where they are co-recovered from black mass alongside lithium.
- The **16-element list on Nikšić (Montenegro)** is a bauxite / red-mud critical-minerals record — lithium, gallium, vanadium and REEs recovered from alumina residue rather than from a lithium deposit. It is the only entry of its kind here and skews the single-site rows in the table above.

---

## Full listing

Grouped by country, alphabetical by facility name.


### Argentina (ARG) — 10

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Cauchari Olaroz Mine** | Jujuy, Argentina | `-23.4270, -66.6470` | mine | unknown | Lithium | — |
| **Centenario Ratones Mine** | Salta, Argentina | `-24.0000, -66.0000` | mine | unknown | Lithium *(from notes)* | — |
| **Fénix Mine** | Catamarca, Argentina | `-25.5000, -66.5000` | mine | unknown | Lithium *(from notes)* | — |
| **Mariana Mine** | Salta, Argentina | `-23.5000, -66.5000` | mine | unknown | Lithium *(from notes)* | — |
| **Muerto North Mine** | Salta, Argentina | `-25.4420, -67.1020` | mine | unknown | Lithium | — |
| **Pastos Grandes Mine** | Salta, Argentina | `-24.5730, -66.7010` | mine | unknown | Lithium | — |
| **Reflejos Del Mar Mine** | Catamarca, Argentina | `-28.4350, -65.4340` | mine | unknown | Lithium | — |
| **Sal De Vida Mine** | Catamarca, Argentina | `-25.4094, -66.9124` | mine | unknown | Lithium | — |
| **Salar De Olaroz Mine** | Jujuy, Argentina | `-23.4624, -66.7025` | mine | unknown | Lithium | — |
| **Salar Ratones Mine** | Salta, Argentina | `-25.1640, -66.7500` | mine | unknown | Lithium | — |

### Australia (AUS) — 24

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Altura Mine** | Western Australia, Australia | `-20.5000, 118.9000` | mine | unknown | Lithium | Copper, Gold, Iron ore, Silver, Tin |
| **Bald Hill Central Mine** | Western Australia, Australia | `-30.0000, 121.0000` | mine | unknown | Lithium | Niobium, Tin |
| **Bellevue Mine** | Western Australia, Australia | `-28.5000, 120.5000` | mine | unknown | Lithium | Gold, Nickel |
| **British King Mine** | Western Australia, Australia | `-28.5000, 121.5000` | mine | unknown | Lithium | Gold |
| **Central Norseman Mine** | Western Australia, Australia | `-32.1960, 121.7960` | mine | unknown | Lithium | Cobalt, Gold, Nickel, Silver, Tantalum |
| **Comet Vale Mine** | Western Australia, Australia | `-29.9600, 121.1300` | mine | unknown | Lithium | Bismuth, Caesium, Cobalt, Copper, Gold, Lanthanum, Lead, Nickel, Silver, Tungsten, Zinc |
| **Finniss Mine** | Northern Territory, Australia | `-12.7130, 130.7890` | mine | unknown | Lithium | Niobium, Tin |
| **Forrestania Nickel Mine** | Western Australia, Australia | `-32.5810, 119.7370` | mine | unknown | Lithium | Cobalt, Nickel |
| **Golden Eagle Mine** | Western Australia, Australia | `-30.0440, 120.6590` | mine, plant | unknown | Lithium | Gold |
| **Golden Eagle Nullagine Mine** | Western Australia, Australia | `-21.9650, 120.1240` | mine | unknown | Lithium | Gold, Tantalum |
| **Greenbushes Mine** | Western Australia, Australia | `-33.7000, 116.1000` | mine | unknown | Lithium | Kaolin, Tin |
| **Higginsville Mine** | Western Australia, Australia | `-33.5000, 121.5000` | mine | unknown | Lithium | Cobalt, Copper, Gold, Nickel, Palladium, Platinum |
| **Kalgoorlie Gold Ops Mine** | Western Australia, Australia | `-28.7000, 121.5000` | mine | unknown | Lithium | Gold, Nickel, Niobium, Rubidium, Silver, Tantalum, Tin |
| **Kathleen Valley Mine** | Western Australia, Australia | `-26.5000, 120.5000` | mine | unknown | Lithium | Gold |
| **Mount Edwards Mine** | Western Australia, Australia | `-31.4630, 121.5320` | mine | unknown | Lithium *(from notes)* | Nickel |
| **Mt Cattlin Mine** | Western Australia, Australia | `-33.5625, 120.0352` | mine | unknown | Lithium | Copper, Gold, Lead, Manganese, Niobium, Silica, Zinc |
| **Mt Holland Mine** | Western Australia, Australia | `-32.0930, 119.7420` | mine | unknown | Lithium, Spodumene | Tantalum |
| **Mt Marion Mine** | Western Australia, Australia | `-28.8000, 121.5000` | mine | unknown | Lithium | Magnesium, Nickel, Tantalum |
| **Nickel West Kwinana Refinery** | Western Australia, Australia | `-32.2000, 115.8000` | refinery, plant | unknown | Lithium | Gold, Nickel |
| **Pilgangoora Mine** | Western Australia, Australia | `-21.0640, 118.9050` | mine | operating | Lithium | Tantalum |
| **The Mount Mine** | Queensland, Australia | `-20.7333, 140.5000` | mine | unknown | Lithium | Caesium, Gold, Tantalum |
| **White Dam Olary Mine** | South Australia, Australia | `-30.0000, 140.0000` | mine | unknown | Lithium | Cobalt, Copper, Iron ore, Lanthanum, Nickel, Niobium, Palladium, Platinum, Silver, Tantalum, Tin, Uranium |
| **Wiluna S Mine** | Western Australia, Australia | `-26.5000, 120.2000` | mine, plant | unknown | Lithium | Antimony, Copper, Gold, Nickel, Palladium, Platinum, Silver |
| **Wodgina Mine** | Western Australia, Australia | `-20.5000, 118.5000` | mine | unknown | Lithium | Iron ore, Tin |

### Austria (AUT) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Wolfsberg Lithium Mine** | Wolfsberg, Carinthia, Austria | `46.8388, 15.1074` | mine | unknown | Lithium | Feldspar, Quartz |

### Bolivia (BOL) — 2

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Salar De Coipasa Mine** | Oruro, Bolivia | `-19.3640, -68.1330` | mine | unknown | Lithium | — |
| **Salar De Uyuni Mine** | Potosí Department, Bolivia | `-20.1333, -66.8333` | mine | unknown | Lithium | — |

### Brazil (BRA) — 4

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Cachoeira Mine** | Belém, Pará, Brazil | `-2.8568, -47.0692` | mine | unknown | Lithium | — |
| **Grota Do Cirilo Mine** | Minas Gerais, Brazil | `-16.7360, -41.9050` | mine | unknown | Lithium | — |
| **Mibra Mine** | Belo Horizonte, Minas Gerais, Brazil | `-20.7508, -44.8277` | mine | unknown | Lithium | Niobium, Tin |
| **Quixeramobim Mine** | Ceará, Brazil | `-5.2000, -39.3000` | mine | unknown | Lithium | — |

### Cameroon (CMR) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Ndom Licence Area Mine** | Adamaoua, Cameroon | `6.3000, 13.8000` | mine | unknown | Lithium *(from notes)* | — |

### Canada (CAN) — 5

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **BÃ©cancour Lithium Hydroxide Refinery** | Quebec, Canada | `46.3674, -72.4040` | refinery | planned | Lithium *(from notes)* | — |
| **Clearwater Lithium Refinery** | Calgary, Alberta, Canada | `51.7737, -114.0575` | plant | unknown | Lithium *(from notes)* | — |
| **James Bay Mine** | Matagami, Quebec, Canada | `51.1061, -77.6300` | mine | unknown | Lithium | — |
| **North American Lithium Mine** | Val d’Or, Quebec, Canada | `48.6419, -77.7876` | mine | unknown | Lithium | — |
| **Whabouchi Mine** | Nemaska, Quebec, Canada | `51.7042, -75.9264` | mine | unknown | Lithium | — |

### Chile (CHL) — 5

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **La Negra Mine** | Antofagasta Region, Chile | `-23.5000, -70.4000` | mine | unknown | Lithium *(from notes)* | — |
| **Maricunga Mine** | Atacama, Chile | `-26.7480, -69.0750` | mine | unknown | Lithium | — |
| **Salar De Atacama Mine** | Antofagasta Region, Chile | `-23.5000, -67.5000` | mine, plant | unknown | Lithium | Magnesium, Potash |
| **Salar Del Carmen Mine** | Antofagasta, Antofagasta Region, Chile | `-23.6200, -70.3500` | mine | unknown | Lithium *(from notes)* | — |
| **Salar Del Carmen/salar De Atacama Plant** | Antofagasta Region, Chile | `-23.5000, -67.5000` | plant | unknown | Lithium | Bismuth, Potash |

### China (CHN) — 3

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Dongtai Jinaier Mine** | Qinghai, China | `37.4420, 94.0800` | mine | unknown | Lithium | — |
| **Qinghai Misc Brines Mine** | Qinghai, China | `31.3800, 83.9780` | mine | unknown | Lithium | — |
| **Xitai Jinaier Mine** | Qinghai, China | `37.6890, 93.5270` | mine | unknown | Lithium | — |

### Cote d'Ivoire (CIV) — 3

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Agboville/adzopÃ© Exploration** | Lagunes, Cote d'Ivoire | `5.9280, -4.2132` | project-exploration | unknown | Lithium | — |
| **Atex Lithium Coltan Exploration** | Boundiali, Côte d'Ivoire, Cote d'Ivoire | `9.8849, -6.4820` | plant | unknown | Lithium | Tantalum |
| **Rubino/agboville Licenses Exploration** | Agnéby-Tiassa, Cote d'Ivoire | `5.9350, -4.2230` | plant | unknown | Lithium | — |

### Czechia (CZE) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Cínovec Development** | Ústí nad Labem, Czechia | `50.7300, 13.7600` | plant | unknown | Lithium *(from notes)* | — |

### DR Congo (COD) — 3

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Kitotolo Mine** | Tanganyika, DR Congo | `-7.3230, 27.3850` | mine | unknown | Lithium | — |
| **Manono Kitolo Mine** | Tanganyika Province, DR Congo | `-7.2881, 27.3939` | mine | unknown | Lithium *(from notes)* | — |
| **Manono Mine** | Tanganyika Province, DR Congo | `-7.2770, 27.4510` | mine | unknown | Lithium | — |

### Ethiopia (ETH) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Kenticha Mine** | Ethiopia | `5.5167, 39.0333` | plant | suspended | Lithium | Caesium, Niobium, Rare earths, Tantalum |

### Finland (FIN) — 2

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Keliber Lithium Mine** | Central Ostrobothnia, Finland | `63.4716, 24.2295` | mine, concentrator, plant | unknown | Lithium *(from notes)* | — |
| **Keliber Mine** | Central Ostrobothnia, Finland | `63.5630, 23.7890` | mine, plant | unknown | Lithium | Tantalum |

### France (FRA) — 3

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Echassières Mine** | Auvergne-Rhône-Alpes, France | `46.1790, 2.9550` | mine | closed | Lithium *(from notes)* | Tungsten |
| **Emili Mine** | Allier, France | `46.0000, 2.6000` | mine | unknown | Lithium | — |
| **Massif Central Mine** | Allier, France | `46.2320, 2.7020` | mine | unknown | Lithium | — |

### Ghana (GHA) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Ewoyaa Mine** | Central Region, Ghana | `5.2400, -1.0530` | mine | unknown | Lithium | — |

### India (IND) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Govindpal Area Mine** | Jharkhand, India | `18.7000, 81.9000` | mine | unknown | Lithium | — |

### Indonesia (IDN) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **PT Chengtok Lithium Indonesia Refinery** | Central Sulawesi, Indonesia | `-2.8256, 122.1554` | plant | operating | Lithium | — |

### Ireland (IRL) — 2

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Avalonia Exploration** | Wicklow and Carlow, Ireland | `52.7478, -6.6233` | plant | operating | Lithium *(from notes)* | — |
| **Leinster Exploration** | Dublin, Leinster, Ireland | `53.2126, -6.5348` | plant | operating | Lithium *(from notes)* | — |

### Luxembourg (LUX) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Circu Li Ion Battery Upcycling Facility Battery Recycling** | Luxembourg | `49.4728, 5.9898` | battery_recycling | operating | Lithium | Cobalt, Graphite, Nickel |

### Madagascar (MDG) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Millie's Reward Development** | Madagascar | `-19.5000, 47.0000` | development | unknown | Lithium *(from notes)* | Copper, Gold |

### Malawi (MWI) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Karonga Lithium Exploration** | Northern Region, Malawi | `-9.9333, 33.9333` | exploration | unknown | Lithium | Copper, Rare earths |

### Malaysia (MYS) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Econili Battery Recycling Plants Battery Recycling** | Malaysia | `4.6500, 101.1100` | plant | operating | Lithium (as lithium carbonate) | Nickel (as MHP/sulfate) |

### Mali (MLI) — 3

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Bougouni Lithium Mine** | Mali | `11.4180, -7.4710` | plant | unknown | Lithium | — |
| **Faraba & Gouna Projects Exploration** | Kayes, Mali | `14.0830, -11.7500` | plant | unknown | Lithium | — |
| **Goulamina Mine** | Mali | `11.3460, -7.9600` | mine | unknown | Lithium | — |

### Mexico (MEX) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Sonora Lithium Facility** | Bacadehuachi, Sonora, Mexico | `29.8820, -109.1260` | plant | unknown | Lithium, Lithium carbonate (planned), Lithium hydroxide (planned) | — |

### Mongolia (MNG) — 3

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Baavhai Uul Exploration** | Sükhbaatar, Sükhbaatar Province, Mongolia | `46.4253, 113.5077` | plant | unknown | Lithium | — |
| **Tsagan Chuluut Exploration** | Ulaanbaatar, Central Mongolia, Mongolia | `47.8352, 111.2880` | plant | unknown | Lithium | — |
| **Urgakh Naran Exploration** | Dorngovi, Mongolia | `44.5000, 109.5000` | plant | unknown | Lithium | — |

### Montenegro (MNE) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Nikšić Bauxite Mines Facility** | Montenegro | `42.7620, 19.0820` | plant | unknown | Lithium | Aluminium (from bauxite), Cerium, Dysprosium, Erbium, Europium, Gadolinium, Gallium, Iron ore, Lanthanum, Neodymium, Niobium, Praseodymium, Samarium, Terbium, Vanadium, Yttrium |

### Morocco (MAR) — 3

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Cobco Battery Materials Refinery** | Casablanca, El Jadida, Morocco | `32.4674, -7.6200` | plant | unknown | Lithium (lfp planned) | Cobalt sulfate, Nickel sulfate |
| **Managem Glencore Recycling Refinery** | Marrakech, Marrakech-Safi, Morocco | `31.6252, -7.5992` | plant | unknown | Lithium carbonate (recycled) | Cobalt (recycled), Nickel (recycled) |
| **ZEN Lithium Exploration** | Ouarzazate, Morocco | `30.9167, -6.9167` | plant | unknown | Lithium | — |

### Mozambique (MOZ) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Zambezia Pegmatite Mine** | Zambezia, Mozambique | `-17.0000, 37.0000` | plant | unknown | Lithium | Niobium (columbite), Tantalum |

### Namibia (NAM) — 2

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Karibib Mine** | Erongo, Namibia | `-21.9722, 15.7733` | mine | unknown | Lithium | — |
| **Uis Tin Mine** | Erongo Region, Namibia | `-21.2167, 14.8811` | mine | unknown | Spodumene | Copper, Niobium, Rubidium, Tantalum, Tin, Tungsten |

### Nepal (NPL) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Hyakule Phakuwa Pegmatites Mine** | Nepal | `27.5000, 87.3000` | plant | unknown | Lithium (petalite, Spodumene) | Antimony, Bismuth, Niobium, Tantalum |

### Netherlands (NLD) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **SK Tes Rotterdam Battery Recycling** | Netherlands | `51.9050, 4.1480` | plant | unknown | Lithium (as carbonate) | Cobalt (as hydroxide), Copper, Graphite, Nickel |

### New Zealand (NZL) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Ohaaki Geothermal Lithium Plant** | Waikato, New Zealand | `-38.5279, 176.2937` | plant | development | Lithium | — |

### Niger (NER) — 2

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Comirex SA Lithium Facility** | Agadez, Niger | `19.5000, 9.5000` | plant | unknown | Lithium | — |
| **Tarouadji Exploration** | Agadez, Niger | `17.3458, 8.4189` | exploration | unknown | Lithium | Tin |

### Nigeria (NGA) — 3

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Continental Lithium Facility** | Lagos State, Nigeria | `8.4966, 4.5421` | development | unknown | Lithium | — |
| **Jiuling/canmax/three Crown 1 Processing Plant** | Kebbi State, Nigeria | `10.4071, 4.7178` | plant | unknown | Lithium | — |
| **Jupiter Mine** | Kaduna State, Nigeria | `10.0000, 7.0000` | plant | unknown | Lithium | — |

### Pakistan (PAK) — 2

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Hubco Lithium Exploration** | Baluchistan, Pakistan | `28.0000, 65.0000` | plant | unknown | Lithium | — |
| **Neelum Valley Exploration Exploration** | Azad Kashmir, Pakistan | `34.8401, 74.2451` | plant | unknown | Lithium | Rare earths |

### Peru (PER) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Falchani Development** | Lima, Puno, Peru | `-14.2360, -71.4671` | plant | unknown | Lithium | — |

### Portugal (PRT) — 9

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Adagói Facility** | Douro, Portugal | `41.2333, -7.1833` | plant | unknown | Lithium | Feldspar, Quartz |
| **Alvarrões Lepidolite Mine** | Guarda, Portugal | `40.6146, -7.1951` | mine | unknown | Lithium | — |
| **Argemela Facility** | Castelo Branco, Portugal | `40.1933, -7.4900` | development | unknown | Lithium | — |
| **Canedo Covas Facility** | Northern Portugal, Portugal | `41.7000, -7.7000` | plant | unknown | Lithium | Feldspar, Quartz |
| **Gondiaes Mine** | Guarda, Portugal | `41.4875, -7.8700` | mine | unknown | Lithium | — |
| **Lousas Facility** | Beja, Portugal | `41.7000, -7.6700` | plant | unknown | Lithium | Feldspar, Quartz |
| **Mina Do Barroso Mine** | Northern Portugal, Portugal | `41.6270, -7.8040` | mine | unknown | Lithium | — |
| **Mina Do Romano Facility** | Trás-os-Montes, Portugal | `41.8000, -7.8000` | development | unknown | Lithium | Niobium, Tantalum, Tin |
| **Veral Facility** | Portugal | `40.4500, -7.3000` | plant | unknown | Lithium | Feldspar, Quartz |

### Russia (RUS) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Kolmozerskoye Facility** | Murmansk Region, Russia | `67.8000, 34.6000` | plant | unknown | Lithium | Niobium, Tantalum |

### Saudi Arabia (SAU) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Lithium Chemicals Processing Plant** | Saudi Arabia | `24.0210, 38.1200` | plant | unknown | Lithium carbonate, Lithium hydroxide | — |

### Serbia (SRB) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Jadar Facility** | Western Serbia, Serbia | `44.5331, 19.3000` | plant | unknown | Lithium | — |

### Sierra Leone (SLE) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Kalangba Exploration** | Bombali District, Sierra Leone | `9.0872, -12.6954` | plant | unknown | Lithium | — |

### South Korea (KOR) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Lithium Hydroxide Plant** | South Jeolla Province, South Korea | `34.9500, 126.9200` | plant | unknown | Lithium *(from notes)* | — |

### Tanzania (TZA) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Hombolo Li Area Mine** | Tanzania | `-5.9180, 35.8910` | mine | unknown | Lithium | — |

### Thailand (THA) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Reung Kiet Lithium Prospect Exploration** | Phang Nga Province, Thailand | `8.4000, 98.5000` | plant | unknown | Lithium | — |

### Turkmenistan (TKM) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Karabogazgol Bay Lithium Brines Facility** | Balkan Province, Turkmenistan | `41.3519, 53.5953` | plant, plant | unknown | Lithium | — |

### Uganda (UGA) — 6

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Gamba Hill Nampeyo Exploration** | Uganda | `0.5850, 32.9180` | plant | unknown | Lithium | Bismuth, Niobium, Tantalum |
| **Mityana Exploration** | Central Uganda, Uganda | `0.4200, 32.0700` | plant | unknown | Lithium | Neodymium-praseodymium (NdPr), Tantalum |
| **Mutaka Exploration** | Uganda | `-0.8500, 30.0500` | plant | unknown | Lithium | Neodymium-praseodymium (NdPr) |
| **Ntungamo Exploration** | Ntungamo District, Uganda | `-0.8800, 30.2600` | plant | unknown | Lithium | Beryllium, Neodymium-praseodymium (NdPr), Tantalum |
| **Rwemeriro Exploration** | UGA, Uganda | `-0.8500, 30.3500` | plant | unknown | Lithium | Neodymium-praseodymium (NdPr) |
| **Wampero Mine** | Uganda | `0.5000, 32.1667` | mine | operating | Lithium | Niobium, Tantalum, Tin |

### Ukraine (UKR) — 4

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Dobra Lithium Mine** | Kirovohrad, Ukraine | `48.3500, 31.0530` | mine | unknown | Lithium | — |
| **Kruta Balka Lithium Mine** | Zaporizhzhia Oblast, Ukraine | `46.9720, 36.8080` | mine | unknown | Lithium | — |
| **Polokhivske Lithium Mine** | Kirovohrad, Ukraine | `48.6000, 31.8000` | mine | unknown | Lithium | — |
| **Shevchenkivske Lithium Mine** | Donetsk Oblast, Ukraine | `47.9090, 36.7130` | mine | unknown | Lithium | — |

### United States (USA) — 19

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Buckhorn Mica Mine** | Washington, United States | `48.7492, -119.4931` | mine | unknown | Lithium | — |
| **Cadiz Dry Lake Mine** | California, United States | `34.2870, -115.3880` | mine | unknown | Lithium | — |
| **Carolina Mine** | South Carolina, United States | `35.3900, -81.2980` | mine | unknown | Lithium | — |
| **Cojade Mine** | United States | `40.3550, -105.2930` | mine | unknown | Lithium | — |
| **Foote Lithium Brine Mine** | Nevada, United States | `37.7670, -117.5850` | mine, plant | unknown | Lithium | — |
| **Foote Mineral Reclamation Mine** | North Carolina, United States | `35.2170, -81.3530` | mine, plant | unknown | Lithium | — |
| **Hallman Beam Mine** | Mount Holly, North Carolina, United States | `39.9389, -74.7877` | mine | unknown | Lithium | — |
| **Huffman Bean Mine** | United States | `35.3390, -81.3160` | mine | unknown | Lithium | — |
| **Morabisi Exploration** | Georgetown, Guyana, United States | `39.0215, -88.8597` | plant | unknown | Lithium | Tantalum |
| **North Morning Star Mine** | United States | `33.9560, -112.5720` | mine | unknown | Lithium | — |
| **Norwich Lithia Mine** | United States | `42.3190, -72.8620` | mine | unknown | Lithium | — |
| **Preston Hanford Sand And Gravel Mine** | California, United States | `38.4082, -121.3712` | mine | unknown | Lithium | — |
| **REE / Lithium / Cobalt Facility** | Idaho, United States | `45.1392, -114.3519` | plant | unknown | Lithium | Cobalt, Neodymium-praseodymium (NdPr), Rare earths |
| **Searles Lake Mine** | California, United States | `35.7670, -117.4010` | mine | unknown | Lithium | — |
| **Silver Peak Mine** | Tonopah, Nevada, United States | `37.7808, -117.6264` | mine | unknown | Lithium | — |
| **Stewart And Other Mines Mine** | California, United States | `33.3670, -117.0680` | mine | unknown | Lithium | — |
| **Taylor Ledge Lithia Mine** | Oregon, United States | `42.4690, -72.8260` | mine | unknown | Lithium | — |
| **Thacker Pass Mine** | Nevada, United States | `41.7030, -118.0700` | mine | unknown | Lithium | — |
| **West Chesterfield Lithia Mine** | United States | `42.4130, -72.8720` | mine | unknown | Lithium | — |

### Uzbekistan (UZB) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Nurlikon Exploration** | Kyzylkum, Uzbekistan | `40.9190, 69.6330` | plant | unknown | Lithium | Neodymium-praseodymium (NdPr) |

### Vietnam (VNM) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **La Vi Lithium Mine** | Vietnam | `15.0000, 108.5000` | mine | unknown | Lithium | — |

### Zambia (ZMB) — 1

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Misika Exploration** | Southern Province, Zambia | `-17.7500, 25.9500` | plant | unknown | Lithium | Niobium, Tantalum, Tin |

### Zimbabwe (ZWE) — 8

| Facility | Location | Coordinates | Type | Status | Lithium form | Co-products / by-products |
|----------|----------|-------------|------|--------|--------------|---------------------------|
| **Arcadia Lithium Mine** | Harare, Zimbabwe | `-17.8000, 31.0500` | mine | unknown | Lithium, Spodumene | Caesium |
| **Arcadia Lithium Mine** | Mashonaland East, Zimbabwe | `-17.7740, 31.4090` | mine | operating | Lithium *(from notes)* | — |
| **Bikita Mine** | Masvingo, Zimbabwe | `-20.0000, 30.0000` | mine | unknown | Lithium, Petalite | — |
| **Emerald Mine** | Midlands, Zimbabwe | `-20.9167, 29.9331` | mine | unknown | Lithium *(from notes)* | Copper |
| **Kamativi Mine** | Matabeleland North, Zimbabwe | `-18.3290, 27.0630` | mine | unknown | Lithium | — |
| **Sabi Star Mine** | Buhera, Manicaland, Zimbabwe | `-19.5065, 31.1741` | mine | operating | Lithium *(from notes)* | — |
| **Sandawana Mine** | Midlands, Zimbabwe | `-20.9167, 29.9331` | mine | unknown | Lithium *(from notes)* | — |
| **Zulu Lithium Mine** | Bulawayo, Matabeleland South, Zimbabwe | `-19.3827, 28.5427` | mine | unknown | Lithium *(from notes)* | — |

---

## Appendix A — operators and owners named

Company mentions recorded against each facility (Phase 1 raw mentions, not resolved to canonical company IDs).

| Facility | Country | Companies mentioned |
|----------|---------|---------------------|
| Cauchari Olaroz Mine | Argentina | Ganfeng Lithium Co. Ltd., Jujuy Energía y Minería Sociedad del Estado (JEMSE), Lithium Argentina AG, Minera Exar S.A. |
| Centenario Ratones Mine | Argentina | Eramet, Eramine Sudamerica, Eramine Sudamérica SA, Tsingshan Holding Group |
| Fénix Mine | Argentina | Arcadium Lithium plc, Minera del Altiplano S.A., Rio Tinto plc |
| Mariana Mine | Argentina | Ganfeng Lithium Group Co., Ltd., Litio Minera Argentina S.A. |
| Muerto North Mine | Argentina | Lithium South, Lithium South Development Corporation, NRG Metals |
| Pastos Grandes Mine | Argentina | Ganfeng Lithium Co. Ltd., Lithium Argentina (Argentina) Corp. |
| Reflejos Del Mar Mine | Argentina | Latin Resources Ltd |
| Sal De Vida Mine | Argentina | Allkem Ltd., Galaxy Lithium (Sal de Vida) S.A., Galaxy Resources Ltd. |
| Salar De Olaroz Mine | Argentina | Allkem Limited, Jujuy Energía y Minería Sociedad del Estado, Sales de Jujuy S.A., Toyota Tsusho Corporation |
| Salar Ratones Mine | Argentina | Eramet, Eramine |
| Altura Mine | Australia | Altura Lithium Operations Pty Ltd, Altura Mining Limited, Pilbara Minerals Limited |
| Bald Hill Central Mine | Australia | Alita Resources Ltd, Alliance Mineral Assets Ltd, Mineral Resources Ltd, Tawana Resources NL |
| Bellevue Mine | Australia | Bellevue Gold Limited |
| British King Mine | Australia | Central Iron Ore, Red 5 Ltd |
| Central Norseman Mine | Australia | Pantoro Limited, Tulla Resources |
| Comet Vale Mine | Australia | Gorilla Gold Mines Ltd |
| Finniss Mine | Australia | Core Lithium Ltd |
| Forrestania Nickel Mine | Australia | IGO Limited, Outokumpu Mining Australia Pty Ltd, Western Areas |
| Golden Eagle Mine | Australia | AIM Mining Corporation |
| Golden Eagle Nullagine Mine | Australia | AIM Mining Corporation, Calidus Resources, Millennium Minerals, Novo Resources |
| Greenbushes Mine | Australia | Albemarle Corporation, IGO Ltd., Talison Lithium Australia Pty Ltd, Tianqi Lithium Corporation |
| Higginsville Mine | Australia | Karora Resources Inc., Westgold Resources Ltd |
| Kalgoorlie Gold Ops Mine | Australia | Barrick Gold, Newmont Goldcorp, Northern Star Resources |
| Kathleen Valley Mine | Australia | Liontown Resources Ltd |
| Mount Edwards Mine | Australia | Consolidated Minerals, Estrella Resources, Neometals, Salt Lake Mining |
| Mt Cattlin Mine | Australia | Arcadium Lithium, Galaxy Lithium Pty Ltd, Rio Tinto plc |
| Mt Holland Mine | Australia | Covalent Lithium Pty Ltd, Kidman Resources Ltd, Sociedad Quimica y Minera de Chile (SQM), Wesfarmers |
| Mt Marion Mine | Australia | Ganfeng Lithium Group Co., Ltd., Mineral Resources Ltd. |
| Nickel West Kwinana Refinery | Australia | BHP Billiton Ltd, BHP Nickel West Pty Ltd |
| Pilgangoora Mine | Australia | Pilbara Minerals Limited |
| The Mount Mine | Australia | Glencore Mount Isa Mines, Glencore Xstrata plc, MIM Holdings Limited |
| White Dam Olary Mine | Australia | GBM Resources Ltd, Olary Gold Mines, Pacgold Limited |
| Wiluna S Mine | Australia | Wiluna Mining Corporation Limited |
| Wodgina Mine | Australia | Albemarle Corporation, Mineral Resources Limited |
| Wolfsberg Lithium Mine | Austria | Critical Metals Corp, Critical Metals Corp., ECM Lithium AT GmbH, European Lithium, European Lithium Limited |
| Salar De Coipasa Mine | Bolivia | Xinjiang TBEA Group-Baocheng, Yacimientos de Litio Bolivianos (YLB) |
| Salar De Uyuni Mine | Bolivia | Corporación Minera de Bolivia (COMIBOL), Yacimientos de Litio Bolivianos (YLB) |
| Cachoeira Mine | Brazil | Brazil Resources Inc., Luna Gold Corp. |
| Grota Do Cirilo Mine | Brazil | Sigma Lithium Corp, Sigma Lithium Resources Corp, Sigma Mineração S.A. |
| Mibra Mine | Brazil | AMG Brazil |
| Quixeramobim Mine | Brazil | MMX Mineração |
| Ndom Licence Area Mine | Cameroon | Oriole Resources PLC |
| BÃ©cancour Lithium Hydroxide Refinery | Canada | Investissement Quebec, Investissement Québec, Livent Corp, Livent Corp., Nemaska Lithium, Nemaska Lithium Inc, Nemaska Lithium Inc., Orion Mine Finance, Pallinghurst Group |
| Clearwater Lithium Refinery | Canada | 1975293 Alberta Ltd., E3 Lithium Ltd. |
| James Bay Mine | Canada | Allkem Ltd., Rio Tinto Group |
| North American Lithium Mine | Canada | Piedmont Lithium, Sayona Mining, Sayona Québec |
| Whabouchi Mine | Canada | Investissement Quebec Inc., Nemaska Lithium Inc., Rio Tinto plc |
| La Negra Mine | Chile | Albemarle Corporation |
| Maricunga Mine | Chile | Codelco, Rio Tinto |
| Salar De Atacama Mine | Chile | Sociedad Quimica y Minera de Chile S.A., Sociedad Quimica y Minera de Chile S.A. (SQM) |
| Salar Del Carmen Mine | Chile | Corporación de Fomento de la Producción (CORFO), Sociedad Química y Minera de Chile (SQM), Sociedad Química y Minera de Chile S.A. (SQM) |
| Salar Del Carmen/salar De Atacama Plant | Chile | Sociedad Química y Minera de Chile (SQM) |
| Dongtai Jinaier Mine | China | Qinghai CITIC Guoan Technology Development (2.16%), Qinghai Dongtai Jinai’er Lithium Resources, Qinghai Lianyu Potash (20.34%), Qinghai State-owned Assets Investment Management (1%), Qinghai Taifeng Xianxing Lithium Energy Technology (49.5%), Western Mining (27%) |
| Qinghai Misc Brines Mine | China | China Minmetals Corporation, China Salt Lake Industry Group Co., Ltd., Qinghai province |
| Xitai Jinaier Mine | China | Qinghai Dongtai Jinaier lithium resources Co., Ltd., Western Mining Group Co., Ltd. |
| Agboville/adzopÃ© Exploration | Cote d'Ivoire | Desert Metals Limited, Khaleesi Resources SARL |
| Atex Lithium Coltan Exploration | Cote d'Ivoire | Alliance Minerals Corporation Sarl, Atex Mining Resources, Firering Holdings Limited, Firering PLC, Firering Strategic Minerals Plc |
| Rubino/agboville Licenses Exploration | Cote d'Ivoire | Atlantic Lithium, Atlantic Lithium Limited, Atlantic Lithium Ltd, Atlantic Lithium Ltd., Khaleesi Resources SARL |
| Cínovec Development | Czechia | CEZ a.s., European Metals Holdings Limited, European Metals Holdings Ltd., European Metals Holdings), GEOMET s.r.o. (ČEZ Group, Geomet a.s., Geomet s.r.o., ČEZ a.s. |
| Kitotolo Mine | DR Congo | AVZ Minerals, Cominière, Force Commodities Ltd, Manono Lithium SAS, Tantalex Resources Corporation, Zijin Mining |
| Manono Kitolo Mine | DR Congo | AVZ Minerals Ltd., La Congolaise d’Exploitation Minière SA, Manono Lithium SAS, Zijin Mining |
| Manono Mine | DR Congo | AVZ Minerals Limited, Jinxiang Lithium Limited, La Congolaise d’Exploitation Minière SA, Zijin Mining |
| Kenticha Mine | Ethiopia | Kenticha Mining Plc (OMSC (operator), AML) (operator) |
| Keliber Lithium Mine | Finland | Finnish Minerals Group, Keliber Oy, Sibanye-Stillwater Ltd, Sibanye-Stillwater Ltd. |
| Keliber Mine | Finland | Finnish Minerals Group, Keliber Oy, Sibanye-Stillwater Ltd. |
| Echassières Mine | France | Imerys |
| Emili Mine | France | Imerys, Imerys S.A. |
| Massif Central Mine | France | Imerys |
| Ewoyaa Mine | Ghana | Atlantic Lithium, Government of Ghana, Minerals Income Investment Fund, Piedmont Lithium |
| Govindpal Area Mine | India | Bharat Coking Coal Ltd., Coal India Ltd. |
| PT Chengtok Lithium Indonesia Refinery | Indonesia | Chengxin Lithium Group, PT ChengTok Lithium Indonesia, PT. ChengTok Lithium Indonesia, Shenzhen Chengxin Lithium Group Co Ltd, Stellar Investment Pte, Tsingshan Holding Group |
| Avalonia Exploration | Ireland | Blackstairs Lithium (Ganfeng, GFL International Co., Limited, Ganfeng Lithium Co., Ltd., Ganfeng Lithium Group Co., Ltd., ILC JV), International Lithium Corp. |
| Leinster Exploration | Ireland | European Lithium, European Lithium Limited, Global Battery Metals Ltd, LRH Resources Limited, Technology Minerals Plc |
| Circu Li Ion Battery Upcycling Facility Battery Recycling | Luxembourg | Circu Li-ion S.A. (operator) |
| Millie's Reward Development | Madagascar | Bass Metals Limited, Bass Metals Ltd, Greenwing Resources Ltd |
| Karonga Lithium Exploration | Malawi | DY6 Metals Ltd, DY6 Metals Ltd. |
| Econili Battery Recycling Plants Battery Recycling | Malaysia | EcoNiLi Battery New Energy Sdn Bhd (operator) |
| Bougouni Lithium Mine | Mali | Hainan Mining Co. Ltd, Hainan Mining Co., Ltd., Kodal Minerals, Kodal Mining (UK) Ltd, Les Mines de Lithium de Bougouni SA, Les Mines de Lithium de Bougouni SA (LMLB), Malian Government |
| Faraba & Gouna Projects Exploration | Mali | Intermin, Intermin Resources Ltd |
| Goulamina Mine | Mali | Ganfeng Lithium, Malian Government |
| Sonora Lithium Facility | Mexico | Bacanora Lithium Plc, Cadence Minerals Plc, Ganfeng Lithium, Ganfeng Lithium Group Co., Ltd., Ganfeng Lithium Ltd |
| Baavhai Uul Exploration | Mongolia | Aranjin Resources Ltd., ION Energy Ltd. |
| Tsagan Chuluut Exploration | Mongolia | Lithium Century, Sinomine Resources Group |
| Urgakh Naran Exploration | Mongolia | ION Energy Limited |
| Nikšić Bauxite Mines Facility | Montenegro | Central European Aluminum Company (CEAC), Government of Montenegro, Uniprom Metal d.o.o. |
| Cobco Battery Materials Refinery | Morocco | Al Mada, CNGR Advanced Materials, COBCO |
| Managem Glencore Recycling Refinery | Morocco | Al Mada, Glencore, Managem |
| ZEN Lithium Exploration | Morocco | Atlas Mining |
| Zambezia Pegmatite Mine | Mozambique | Altona Rare Earths Plc, Highland African Mining Company, Noventa Group |
| Karibib Mine | Namibia | QKR, QKR Namibia (Pty) Ltd |
| Uis Tin Mine | Namibia | AfriTin Mining Ltd, Andrada Mining Ltd, The Small Miners of Uis (SMU) |
| SK Tes Rotterdam Battery Recycling | Netherlands | SK Tes (operator) |
| Ohaaki Geothermal Lithium Plant | New Zealand | Geo40 Limited, Contact Energy |
| Comirex SA Lithium Facility | Niger | Compagnie Minière de Recherche et d’Exploitation (Comirex SA), Nigerien government (40% stake) |
| Tarouadji Exploration | Niger | ENRG Elements Limited |
| Continental Lithium Facility | Nigeria | C&C Minerals Limited, Chariot Corporation, Continental Critical Minerals, Inc., Continental Lithium Limited |
| Jiuling/canmax/three Crown 1 Processing Plant | Nigeria | Canmax Technologies, Canmax Technologies Co. Ltd., Jiuling Lithium Mining Company, Three Crown Mines, Three Crown Mines Limited |
| Jupiter Mine | Nigeria | Basin Mining Limited, Jupiter Lithium Limited, KD Prospect, Pegasus Resources Inc., Range Mining Limited, ReElement Technologies Corporation |
| Hubco Lithium Exploration | Pakistan | Hub Power Company Limited |
| Falchani Development | Peru | American Lithium Corp., Macusani Yellowcake S.A.C. |
| Alvarrões Lepidolite Mine | Portugal | Felmica Minerais Industrias SA, Grupo Mota |
| Argemela Facility | Portugal | Grupo Almina, Lusorecursos Portugal Lithium, S.A. |
| Canedo Covas Facility | Portugal | Savannah Resources |
| Gondiaes Mine | Portugal | Felmica - Minerais Industriais S.A. |
| Lousas Facility | Portugal | Felmica |
| Mina Do Barroso Mine | Portugal | Savannah Resources Plc, Savannah Resources Plc (75%), Slipstream Resources (25%) |
| Mina Do Romano Facility | Portugal | Lusorecursos Portugal Lithium, S.A. |
| Veral Facility | Portugal | Grupo Mota |
| Kolmozerskoye Facility | Russia | Nornickel, Polar Lithium, Rosatom |
| Lithium Chemicals Processing Plant | Saudi Arabia | EV Metals Group (operator) |
| Jadar Facility | Serbia | Rio Tinto Ltd |
| Kalangba Exploration | Sierra Leone | Leone Afric Metals Limited |
| Lithium Hydroxide Plant | South Korea | POSCO, POSCO Pilbara Lithium Solution, POSCO Pilbara Lithium Solution Company (PPLS), Pilbara Minerals, Posco Holdings, Posco Pilbara Lithium Solutions |
| Reung Kiet Lithium Prospect Exploration | Thailand | Pan Asia Metals Limited |
| Mityana Exploration | Uganda | Blaze Minerals Limited, Gecko Minerals Uganda, Javelin Minerals |
| Ntungamo Exploration | Uganda | Blaze Minerals Limited, Gecko Minerals Uganda, Javelin Minerals |
| Rwemeriro Exploration | Uganda | GoldQuest Mining Corp., Unclear |
| Wampero Mine | Uganda | 3T Mining Ltd. (operator) |
| Dobra Lithium Mine | Ukraine | European Lithium, TechMet, UkrLithiumMining |
| Kruta Balka Lithium Mine | Ukraine | European Lithium |
| Polokhivske Lithium Mine | Ukraine | UkrLithiumMining LLC (ULM) |
| Shevchenkivske Lithium Mine | Ukraine | European Lithium, Millstone & Co., Petro Consulting LLC |
| Buckhorn Mica Mine | United States | Crown Resources-Kettle River Operations, Kinross Gold Corp |
| Cadiz Dry Lake Mine | United States | Delta Chemical Co., Hill Brothers Chemical Co., Lee Chemical Co., Standard Lithium Ltd., Tetra Technologies, Inc. |
| Carolina Mine | United States | OceanaGold Corporation |
| Foote Lithium Brine Mine | United States | Albemarle Corporation, Foote Mineral Company |
| Foote Mineral Reclamation Mine | United States | Berry Brothers Incorporated |
| Hallman Beam Mine | United States | FMC Corporation, Lithium Corporation of America |
| Morabisi Exploration | United States | Greenpower Energy, Greenpower Energy Ltd, Guyana Strategic Metals Inc. |
| Preston Hanford Sand And Gravel Mine | United States | Hanford Sand & Gravel, Inc. |
| REE / Lithium / Cobalt Facility | United States | Ecobalt Solutions, Jervois Global |
| Searles Lake Mine | United States | HOOKER CHEMICAL CO., Kerr-McGee Chemical Corporation, NORTH AMERICAN CHEMICAL CO., STAUFFER CHEMICAL CO., Searles Valley Minerals Operations, Inc. |
| Silver Peak Mine | United States | Albemarle Corporation, Albemarle U.S., Inc. |
| Stewart And Other Mines Mine | United States | A. E. Almind, F. H. Stewart, Jack Koterske, Pala Gem Mining Company, W. A. T. Agard |
| Taylor Ledge Lithia Mine | United States | H.W. Eichemeyer, Ray Whiting |
| Thacker Pass Mine | United States | General Motors Holdings LLC, Lithium Americas Corp. |
| Nurlikon Exploration | Uzbekistan | Navoiy Uran, Nurlikum Mining LLC, Orano |
| Misika Exploration | Zambia | First Africa Metals, First Africa Metals Ltd |
| Arcadia Lithium Mine | Zimbabwe | Prospect Lithium Zimbabwe, Prospect Resources, Zhejiang Huayou Cobalt |
| Arcadia Lithium Mine | Zimbabwe | Prospect Lithium Zimbabwe (pvt) ltd, Prospect Resources Limited, Zhejiang Huayou Cobalt |
| Bikita Mine | Zimbabwe | Bikita Minerals (Private) Limited, Sinomine Resource Group |
| Emerald Mine | Zimbabwe | Kuvimba Mining House, Rio Tinto Zinc, Sandawana Mines Ltd. |
| Kamativi Mine | Zimbabwe | Kamativi Mining Company, Sichuan PD Technology Group, Yahua Group, Zimbabwe Lithium, Zimbabwe Mining Development Corporation |
| Sabi Star Mine | Zimbabwe | Chengxin Lithium Group, Max Mind Investments, Max Minds Investments, Power China, Shenzhen Chengxin Lithium Group |
| Sandawana Mine | Zimbabwe | Kuvimba Mining House (Private) Limited |
| Zulu Lithium Mine | Zimbabwe | Premafrimin, Premier African Minerals Limited |

## Needs review — excluded matches

9 records mention lithium in their research notes but were **not** counted as lithium facilities. Most are cases where a company name contains the word "lithium". Several look like genuine data errors worth fixing.

| Facility | Country | Recorded commodities | Why excluded |
|----------|---------|----------------------|--------------|
| `alb-reps-fac` | Albania | Copper | Copper mine in Albania; note describes Albemarle's Silver Peak (Nevada) lithium site and the province field says 'Nevada' - record looks corrupted. |
| `aus-galaxy-fac` | Australia | Gold | Listed as Gold, but owners (Galaxy Resources / Orocobre) and coordinates near Ravensthorpe WA point to the Mt Cattlin lithium mine. |
| `aus-pertha-may-fac` | Australia | Gold | Listed as Gold at Perth CBD coordinates, but companies are the Greenbushes owner set (Talison/Tianqi/Albemarle/IGO). Probable junk record. |
| `can-becancour-fac` | Canada | aluminum | Aluminium/silicon smelter complex. Nemaska Lithium is one of nine companies listed; Becancour does host a separate Nemaska lithium plant. |
| `chl-el-rincon-fac` | Chile | Copper | Listed as Copper in Antofagasta. Rio Tinto's Rincon lithium project is in Salta, Argentina - probable conflation. |
| `chn-huichun-smelter-fac` | China | Copper | Copper smelter; note only says companies are 'involved in lithium mining' generically. |
| `chn-xiangyuan-jinxing-coal-mine-fac` | China | metallurgical coal | Note says Zijin's 'Xiangyuan Hard-Rock Lithium-containing Polymetallic Mine', but commodities say met coal, province says Hunan and coordinates are in Shanxi. |
| `mli-morila-fac` | Mali | Gold | Gold mine. 'Mali Lithium' is a former corporate name of Firefinch, not a commodity. |
| `moz-corridor-sands-projects-fac` | Mozambique | titanium (heavy mineral sands), zirconium (heavy mineral sands), hafnium (from zircon) | Heavy mineral sands (Ti/Zr/Hf). 'Sinowin Lithium' is a company name only. |

## Data gaps found while building this list

- **16 facilities have an empty or non-lithium `commodities[]` array** despite being documented lithium sites in their notes. They are included here at tier C, but the underlying JSON should be backfilled:

  - `arg-centenario-ratones-fac` — Centenario Ratones Mine, Argentina (commodities empty)
  - `arg-f-nix-project-fac` — Fénix Mine, Argentina (commodities empty)
  - `arg-mariana-fac` — Mariana Mine, Argentina (commodities empty)
  - `aus-mount-edwards-fac` — Mount Edwards Mine, Australia (currently lists: Nickel)
  - `cmr-ndom-licence-area-fac` — Ndom Licence Area Mine, Cameroon (commodities empty)
  - `chl-la-negra-plant-fac` — La Negra Mine, Chile (commodities empty)
  - `chl-salar-del-carmen-fac` — Salar Del Carmen Mine, Chile (commodities empty)
  - `cze-c-novec-project-fac` — Cínovec Development, Czechia (commodities empty)
  - `cod-manono-kitolo-project-fac` — Manono Kitolo Mine, DR Congo (commodities empty)
  - `fra-echassi-res-district-fac` — Echassières Mine, France (currently lists: Tungsten)
  - `irl-avalonia-project-fac` — Avalonia Exploration, Ireland (commodities empty)
  - `irl-leinster-project-fac` — Leinster Exploration, Ireland (commodities empty)
  - `mdg-millie-s-reward-project-fac` — Millie's Reward Development, Madagascar (currently lists: Copper, Gold)
  - `zwe-emerald-mine-fac` — Emerald Mine, Zimbabwe (currently lists: Copper)
  - `zwe-sabi-star-mine-fac` — Sabi Star Mine, Zimbabwe (commodities empty)
  - `zwe-sandawana-mine-fac` — Sandawana Mine, Zimbabwe (commodities empty)

- Several commodity strings carry footnote digits or parentheticals from the source reports (`gallium 28`, `lithium (as lithium carbonate)`, `nickel (as MHP/sulfate)`). These were cleaned for display here but remain raw in the JSON.
- `not specified`, `n/a` and `other critical minerals` appear as literal commodity values on a few records and were dropped from the by-product tally.

