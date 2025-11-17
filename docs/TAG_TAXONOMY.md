# Tag Taxonomy for Measurables

All `APPLICABILITY_TAGS` must use namespace:value format. No free text.

## Namespaces

### facility:*
Facility type classification.

**Values:**
- `facility:mine`
- `facility:smelter`
- `facility:refinery`
- `facility:concentrator`
- `facility:plant`
- `facility:tailings`
- `facility:port`

### process:*
Process technologies in the value chain.

**Values:**
- `process:flotation`
- `process:heap_leach`
- `process:sxew`
- `process:hpal`
- `process:pox`
- `process:bioleach`
- `process:gravity_separation`
- `process:magnetic_separation`
- `process:blast_furnace`
- `process:electric_arc_furnace`
- `process:reverb_furnace`
- `process:flash_smelting`
- `process:electrolytic_refining`
- `process:pyrometallurgical_refining`
- `process:hydrometallurgical_refining`
- `process:roasting`
- `process:calcining`
- `process:sintering`
- `process:pelletizing`
- `process:direct_reduction`

### metal:*
Commodity/metal produced.

**Values:**
- `metal:Cu` (Copper)
- `metal:Ni` (Nickel)
- `metal:Co` (Cobalt)
- `metal:PGM` (Platinum Group Metals)
- `metal:Al` (Aluminum)
- `metal:Zn` (Zinc)
- `metal:Pb` (Lead)
- `metal:Sn` (Tin)
- `metal:W` (Tungsten)
- `metal:REE` (Rare Earth Elements)
- `metal:FeSi` (Ferrosilicon)
- `metal:Si` (Silicon)
- `metal:Fe` (Iron)
- `metal:Mn` (Manganese)
- `metal:Cr` (Chromium)
- `metal:Au` (Gold)
- `metal:Ag` (Silver)
- `metal:Li` (Lithium)
- `metal:graphite`
- `metal:coal`
- `metal:uranium`

### country:*
ISO3 country codes.

**Values:**
- `country:ZAF` (South Africa)
- `country:CHL` (Chile)
- `country:PER` (Peru)
- `country:AUS` (Australia)
- `country:USA` (United States)
- `country:CAN` (Canada)
- `country:CHN` (China)
- `country:IDN` (Indonesia)
- `country:RUS` (Russia)
- ... (any ISO3 code)

### risk:*
Risk/dependency characteristics.

**Values:**
- `risk:power_intensive`
- `risk:security`
- `risk:rain_sensitive`
- `risk:acid_dependent`
- `risk:port_dependent`
- `risk:loadshedding_grid`
- `risk:water_constrained`
- `risk:community_conflict`
- `risk:tailings_critical`

### climate:*
Climate zone classification.

**Values:**
- `climate:tropical`
- `climate:monsoon`
- `climate:arid`
- `climate:temperate`
- `climate:continental`
- `climate:polar`

### status:*
Operational status applicability.

**Values:**
- `status:producing`
- `status:care_and_maintenance`
- `status:suspended`
- `status:construction`
- `status:commissioning`
- `status:closed`

## Usage Examples

```json
{
  "json_id": "supply.facility.status.current_operational_state",
  "applicability_tags": ["facility:all"]
}

{
  "json_id": "supply.facility.power.loadshedding_risk_current",
  "applicability_tags": ["risk:power_intensive", "risk:loadshedding_grid", "country:ZAF"]
}

{
  "json_id": "supply.facility.acid.onsite_plant_uptime",
  "applicability_tags": ["process:sxew", "process:hpal", "risk:acid_dependent"]
}

{
  "json_id": "supply.facility.climate.rainfall_disruption_last_30d",
  "applicability_tags": ["facility:mine", "climate:tropical", "climate:monsoon", "risk:rain_sensitive"]
}

{
  "json_id": "supply.facility.feedstock.concentrate_availability",
  "applicability_tags": ["facility:smelter", "facility:refinery", "metal:Cu", "metal:Ni"]
}
```

## Router Matching Logic

The router matches tags against `facility_features`:

```python
# Match facility: tags
if "facility:mine" in tags:
    match = facility.primary_type == "mine"

# Match process: tags
if "process:sxew" in tags:
    match = "sxew" in facility.facility_features.process_chain

# Match metal: tags
if "metal:Cu" in tags:
    match = "Cu" in facility.facility_features.metals

# Match country: tags
if "country:ZAF" in tags:
    match = facility.country_iso3 == "ZAF"

# Match risk: tags
if "risk:power_intensive" in tags:
    match = facility.facility_features.power_intensity in ["very_high", "high"]

# Match climate: tags
if "climate:tropical" in tags:
    match = facility.facility_features.climate_zone == "tropical"

# Match status: tags
if "status:care_and_maintenance" in tags:
    match = facility.status == "care_and_maintenance"
```

## Special Tag: `facility:all`

Use `["facility:all"]` for Core Pack measurables that apply to all facilities regardless of type.
