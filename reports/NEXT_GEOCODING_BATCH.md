# Next Geocoding Batch Request

## Status
- **Current town coverage**: 0.3% (37 facilities)
- **Target**: 5-10% (500-1000 facilities)
- **Next batch size**: 150-200 facilities
- **Focus countries**: BFA (Burkina Faso) or ZAF (South Africa) - single country preferred

## Facilities Needing Correction (7 ID mismatches)

The following facilities from your provided CSV need correct facility IDs:

### Belgium (BEL)
1. **Hoboken Umicore** - Provided: `bel-hoboken-umicore-fac`, Actual: `bel-hoboken-fac`
2. **Olen Umicore** - Provided: `bel-olen-umicore-fac`, Need to locate
3. **Prayon Engis** - Provided: `bel-prayon-engis-smelter-fac`, Need to locate

### South Africa (ZAF)
4. **Bathopele** - Provided: `zaf-bathopele-fac`, Need to locate
5. **Waterval Smelter** - Provided: `zaf-anglo-american-converter-plant-waterval-smelter-fac`  
   Possible match: `zaf-waterval-smelter-complex-fac`
6. **RBMR** - Provided: `zaf-anglo-american-platinum-base-metals-refinery-fac`, Need to locate
7. **PMR Brakpan** - Provided: `zaf-anglo-american-platinum-precious-metals-refinery-fac`, Need to locate

## Priority Geocoding Request

Please provide the next 150-200 facilities with cited coordinates in the same CSV format:

```csv
facility_id,country_iso3,raw_name,canonical_name,operator_display,primary_type,lat,lon,precision,town,region,aliases,commodities,notes
```

### Preferred Focus
- **Option 1**: Burkina Faso (BFA) - 16 facilities total, could complete entire country
- **Option 2**: South Africa (ZAF) - 630 facilities, focus on major mining districts:
  - Rustenburg PGM complex (missing towns for many Impala/Lonmin/Anglo operations)
  - Witwatersrand gold fields (Johannesburg/Carletonville/Klerksdorp)
  - Northern Cape iron/manganese (Sishen, Kolomela, Hotazel)

### Current Top 20 from reports/geocoding_request.csv (missing coords)

ARE facilities dominate the list but lack authoritative sources. Suggest focusing on countries with better data availability:

| Country | Facilities Needing Coords | Data Availability |
|---------|--------------------------|-------------------|
| ARE | 19 | Low |
| ARG | 12 | Medium |
| AUS | 23 | High |
| BRA | 8 | Medium |
| CAN | 15 | High |
| CHN | 42 | Medium |
| IND | 18 | Medium |
| USA | 31 | High |
| ZAF | 14 | High |

## Suggested Next Action

Provide 150-200 cited facility records for **one** of:
1. **ZAF**: Major mining districts (Rustenburg, Witwatersrand, Northern Cape)
2. **AUS**: Major mines with Mindat/state govt sources
3. **CAN**: Mines with provincial geological survey data
4. **USA**: USGS/state sources

This will push town coverage from 0.3% → 2-3% in one batch.

---

*Generated: 2025-11-04 12:59 UTC*
