"""
Facility Feature Tagger

Derives facility_features object from facility metadata for measurables routing.
Computes process_type, mine_method, acid_dependency, power_intensity, climate_zone,
port_dependency, water_intensity, country_risk_bucket, and consequentiality_score.

Usage:
    from scripts.llm_measurables.feature_tagger import FacilityFeatureTagger

    tagger = FacilityFeatureTagger()
    features = tagger.tag_facility(facility_json)

    # Or tag all facilities
    tagger.tag_all_facilities(output_dir="facilities/")
"""

import json
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class FacilityFeatureTagger:
    """Derives facility_features from facility metadata."""

    # Climate zone mapping (simplified Koppen) - indexed by country_iso3
    # This is a starter; could be enhanced with lat/lon-based lookup
    CLIMATE_ZONES = {
        "IDN": "tropical", "MYS": "tropical", "PHL": "tropical", "THA": "tropical",
        "VNM": "tropical", "IND": "monsoon", "BGD": "monsoon", "MMR": "monsoon",
        "AUS": "arid", "SAU": "arid", "ARE": "arid", "OMN": "arid",
        "USA": "temperate", "CAN": "continental", "RUS": "continental", "CHN": "temperate",
        "ZAF": "temperate", "BRA": "tropical", "CHL": "temperate", "PER": "temperate",
        "NOR": "polar", "SWE": "polar", "FIN": "polar", "ISL": "polar"
    }

    # Country risk buckets (1=lowest, 5=highest)
    # Based on geopolitical stability, regulatory risk, security
    COUNTRY_RISK = {
        "USA": 1, "CAN": 1, "AUS": 1, "NOR": 1, "SWE": 1, "FIN": 1, "CHE": 1, "DEU": 1,
        "GBR": 1, "JPN": 1, "NZL": 1, "AUT": 1, "DNK": 1, "NLD": 1, "BEL": 1, "FRA": 1,
        "CHL": 2, "BRA": 2, "ZAF": 2, "MEX": 2, "ARG": 2, "POL": 2, "ESP": 2, "ITA": 2,
        "PER": 3, "COL": 3, "IDN": 3, "PHL": 3, "THA": 3, "IND": 3, "TUR": 3, "RUS": 3,
        "KAZ": 3, "MNG": 3, "ZMB": 3, "NAM": 3, "BWA": 3, "GHA": 3, "CIV": 3,
        "COD": 4, "ZWE": 4, "VEN": 4, "MMR": 4, "PNG": 4, "BOL": 4, "NIC": 4,
        "SDN": 5, "SOM": 5, "AFG": 5, "SYR": 5, "YEM": 5, "LBY": 5, "IRQ": 5
    }

    # Metal criticality scores (0-100) - based on US DOE/EU critical minerals lists
    METAL_CRITICALITY = {
        "lithium": 95, "cobalt": 95, "rare earth elements": 95, "graphite": 90,
        "nickel": 85, "manganese": 80, "platinum group metals": 90, "palladium": 90,
        "rhodium": 90, "iridium": 90, "platinum": 90, "tungsten": 85, "antimony": 85,
        "magnesium": 80, "gallium": 90, "germanium": 90, "indium": 85, "tellurium": 85,
        "copper": 70, "aluminum": 60, "zinc": 60, "lead": 50, "tin": 70, "chromium": 75,
        "vanadium": 75, "molybdenum": 70, "titanium": 70, "zirconium": 70,
        "gold": 40, "silver": 55, "iron": 30, "coal": 20, "uranium": 85
    }

    def __init__(self, global_supply_data: Optional[Dict[str, float]] = None):
        """
        Initialize feature tagger.

        Args:
            global_supply_data: Optional dict mapping facility_id -> global_supply_share (0-100)
                                If not provided, will use capacity-based heuristics
        """
        self.global_supply_data = global_supply_data or {}

    def tag_facility(self, facility: Dict[str, Any]) -> Dict[str, Any]:
        """
        Tag a single facility with derived features.

        Args:
            facility: Facility JSON dict

        Returns:
            facility_features dict
        """
        facility_id = facility.get("facility_id")
        primary_type = facility.get("primary_type")
        types = facility.get("types", [])
        commodities = facility.get("commodities", [])
        country_iso3 = facility.get("country_iso3")
        location = facility.get("location", {})

        # Extract primary metal
        primary_metal = None
        for commodity in commodities:
            if commodity.get("primary"):
                primary_metal = commodity.get("metal", "").lower()
                break

        # Derive features
        features = {
            "process_type": self._derive_process_type(primary_type, types, commodities),
            "mine_method": self._derive_mine_method(primary_type, types),
            "acid_dependency": self._derive_acid_dependency(primary_type, types, commodities),
            "power_intensity": self._derive_power_intensity(primary_type, primary_metal),
            "climate_zone": self._derive_climate_zone(country_iso3, location),
            "port_dependency": self._derive_port_dependency(country_iso3, primary_type),
            "water_intensity": self._derive_water_intensity(primary_type, types),
            "country_risk_bucket": self.COUNTRY_RISK.get(country_iso3, 3),  # Default to medium
            "consequentiality_score": self._compute_fcs(facility_id, primary_metal, country_iso3),
            "single_point_failure": self._is_single_point_failure(facility_id),
            "last_feature_update": datetime.utcnow().isoformat() + "Z",
            "feature_confidence": self._compute_feature_confidence(facility)
        }

        return features

    def _derive_process_type(self, primary_type: Optional[str],
                            types: List[str],
                            commodities: List[Dict]) -> Optional[str]:
        """Derive primary process technology."""
        types_lower = [t.lower() for t in types]

        # Direct mapping from types
        if "heap_leach" in types_lower:
            return "heap_leach"
        if "sxew" in types_lower or "sx-ew" in " ".join(types_lower):
            return "sxew"
        if "hpal" in types_lower:
            return "hpal"
        if "flotation" in types_lower or "concentrator" in types_lower:
            return "flotation"
        if "blast_furnace" in types_lower:
            return "blast_furnace"
        if "electric_arc_furnace" in types_lower or "eaf" in types_lower:
            return "electric_arc_furnace"
        if "flash_smelting" in types_lower:
            return "flash_smelting"

        # Heuristics based on primary_type + commodities
        if primary_type == "mine":
            # Default mines to flotation unless heap leach detected
            return "flotation"

        if primary_type in ["smelter", "refinery"]:
            # Guess based on metal
            for commodity in commodities:
                metal = commodity.get("metal", "").lower()
                if "copper" in metal:
                    return "flash_smelting"
                if "aluminum" in metal or "aluminium" in metal:
                    return "electrolytic_refining"
                if "zinc" in metal:
                    return "electrolytic_refining"

        return None

    def _derive_mine_method(self, primary_type: Optional[str], types: List[str]) -> Optional[str]:
        """Derive mining method."""
        if primary_type != "mine":
            return "not_applicable"

        types_lower = [t.lower() for t in types]

        # Look for explicit mentions
        has_open_pit = any("open" in t or "pit" in t for t in types_lower)
        has_underground = any("underground" in t or "ug" in t for t in types_lower)

        if has_open_pit and has_underground:
            return "both"
        if has_open_pit:
            return "open_pit"
        if has_underground:
            return "underground"

        # Default to open_pit (more common)
        return "open_pit"

    def _derive_acid_dependency(self, primary_type: Optional[str],
                                types: List[str],
                                commodities: List[Dict]) -> Optional[str]:
        """Derive sulfuric acid dependency level."""
        types_lower = [t.lower() for t in types]

        # High dependency
        if "sxew" in types_lower or "sx-ew" in " ".join(types_lower):
            return "high"
        if "hpal" in types_lower:
            return "high"

        # Medium dependency
        if "heap_leach" in types_lower:
            return "medium"

        # Low dependency (some flotation circuits use acid for pH control)
        if "flotation" in types_lower or "concentrator" in types_lower:
            return "low"

        # Check for copper (often uses acid processes)
        for commodity in commodities:
            metal = commodity.get("metal", "").lower()
            if "copper" in metal and primary_type == "mine":
                return "medium"  # Many Cu mines use heap leach or SXEW

        return "none"

    def _derive_power_intensity(self, primary_type: Optional[str],
                               primary_metal: Optional[str]) -> Optional[str]:
        """Derive electrical power intensity."""
        if not primary_metal:
            return "low"

        # Very high intensity
        if primary_metal in ["aluminum", "aluminium", "ferrosilicon", "silicon", "magnesium"]:
            return "very_high"

        # High intensity
        if primary_metal in ["zinc", "copper"] and primary_type in ["smelter", "refinery"]:
            return "high"
        if "electro" in (primary_type or ""):
            return "high"

        # Medium intensity
        if primary_type in ["concentrator", "flotation"]:
            return "medium"

        return "low"

    def _derive_climate_zone(self, country_iso3: str, location: Dict) -> Optional[str]:
        """Derive climate zone."""
        # Use country lookup first
        climate = self.CLIMATE_ZONES.get(country_iso3)
        if climate:
            return climate

        # Could enhance with lat/lon-based Koppen lookup here
        # For now, default to temperate
        return "temperate"

    def _derive_port_dependency(self, country_iso3: str, primary_type: Optional[str]) -> Optional[str]:
        """Derive port/shipping dependency."""
        # Island nations with smelters/refiners are critically dependent
        island_nations = ["IDN", "PHL", "PNG", "JPN", "TWN", "MYS", "SGP", "ISL", "CYP", "MLT"]

        if country_iso3 in island_nations and primary_type in ["smelter", "refinery"]:
            return "critical"

        # Large exporters
        major_exporters = ["CHL", "PER", "AUS", "BRA", "ZAF", "CAN", "RUS", "KAZ"]
        if country_iso3 in major_exporters:
            return "high"

        # Default to medium for mines, low for others
        if primary_type == "mine":
            return "medium"

        return "low"

    def _derive_water_intensity(self, primary_type: Optional[str], types: List[str]) -> Optional[str]:
        """Derive water consumption intensity."""
        types_lower = [t.lower() for t in types]

        # Very high
        if "flotation" in types_lower or "concentrator" in types_lower:
            return "very_high"
        if "hpal" in types_lower:
            return "very_high"

        # High
        if "sxew" in types_lower or "sx-ew" in " ".join(types_lower):
            return "high"

        # Medium
        if "heap_leach" in types_lower:
            return "medium"

        return "low"

    def _compute_fcs(self, facility_id: str, primary_metal: Optional[str],
                    country_iso3: str) -> float:
        """
        Compute Facility Consequentiality Score (FCS).

        FCS = 0.45*global_supply_share + 0.2*metal_criticality +
              0.15*supply_concentration + 0.1*single_point_failure + 0.1*recent_volatility

        Scale: 0-100
        """
        # Global supply share (0-100)
        global_supply_share = self.global_supply_data.get(facility_id, 0.0)

        # Metal criticality (0-100)
        metal_criticality = 0.0
        if primary_metal:
            metal_criticality = self.METAL_CRITICALITY.get(primary_metal, 50.0)

        # Supply concentration (HHI-based, 0-100)
        # For now, use country risk as proxy (higher risk = higher concentration)
        country_risk = self.COUNTRY_RISK.get(country_iso3, 3)
        supply_concentration = (6 - country_risk) * 20  # Inverse: low risk = high concentration

        # Single point failure flag (0 or 100)
        spf = 100.0 if self._is_single_point_failure(facility_id) else 0.0

        # Recent volatility (0-100) - placeholder, would come from historical measurables
        recent_volatility = 0.0

        fcs = (0.45 * global_supply_share +
               0.2 * metal_criticality +
               0.15 * supply_concentration +
               0.1 * spf +
               0.1 * recent_volatility)

        return round(fcs, 2)

    def _is_single_point_failure(self, facility_id: str) -> bool:
        """Check if facility is >5% of global supply for any commodity."""
        global_share = self.global_supply_data.get(facility_id, 0.0)
        return global_share > 5.0

    def _compute_feature_confidence(self, facility: Dict[str, Any]) -> float:
        """
        Compute overall confidence in derived features (0-1).

        Based on:
        - Data completeness (primary_type, commodities, location)
        - Verification status
        - Data quality flags
        """
        confidence_factors = []

        # Primary type present and confident
        if facility.get("primary_type"):
            type_conf = facility.get("type_confidence", 0.5)
            confidence_factors.append(type_conf)
        else:
            confidence_factors.append(0.3)

        # Commodities present with chemical formulas
        commodities = facility.get("commodities", [])
        if commodities:
            has_formulas = sum(1 for c in commodities if c.get("chemical_formula")) / len(commodities)
            confidence_factors.append(has_formulas)
        else:
            confidence_factors.append(0.3)

        # Location precision
        location = facility.get("location", {})
        precision = location.get("precision", "unknown")
        precision_score = {
            "exact": 1.0, "site": 0.9, "approximate": 0.7, "region": 0.5, "unknown": 0.3
        }.get(precision, 0.3)
        confidence_factors.append(precision_score)

        # Verification confidence
        verification = facility.get("verification", {})
        ver_confidence = verification.get("confidence", 0.5)
        confidence_factors.append(ver_confidence)

        # Average
        overall_confidence = sum(confidence_factors) / len(confidence_factors)
        return round(overall_confidence, 3)

    def tag_all_facilities(self, input_dir: str = "facilities/",
                          output_dir: Optional[str] = None,
                          dry_run: bool = False) -> Dict[str, int]:
        """
        Tag all facilities in the repository.

        Args:
            input_dir: Directory containing country subdirectories with facility JSONs
            output_dir: Directory to write updated JSONs (default: same as input_dir)
            dry_run: If True, only compute and log features without writing

        Returns:
            Dict with stats: {"total": int, "tagged": int, "skipped": int, "errors": int}
        """
        output_dir = output_dir or input_dir
        stats = {"total": 0, "tagged": 0, "skipped": 0, "errors": 0}

        input_path = Path(input_dir)
        output_path = Path(output_dir)

        # Find all facility JSON files
        facility_files = list(input_path.glob("*/*-fac.json"))

        logger.info(f"Found {len(facility_files)} facility files")

        for facility_file in facility_files:
            stats["total"] += 1

            try:
                # Read facility
                with open(facility_file, "r", encoding="utf-8") as f:
                    facility = json.load(f)

                # Tag features
                features = self.tag_facility(facility)

                if dry_run:
                    logger.info(f"{facility.get('facility_id')}: FCS={features['consequentiality_score']:.2f}, "
                               f"process={features['process_type']}, acid={features['acid_dependency']}, "
                               f"power={features['power_intensity']}")
                    stats["tagged"] += 1
                    continue

                # Update facility JSON
                facility["facility_features"] = features

                # Write back
                output_file = output_path / facility_file.relative_to(input_path)
                output_file.parent.mkdir(parents=True, exist_ok=True)

                with open(output_file, "w", encoding="utf-8") as f:
                    json.dump(facility, f, indent=2, ensure_ascii=False)

                stats["tagged"] += 1

                if stats["tagged"] % 100 == 0:
                    logger.info(f"Tagged {stats['tagged']}/{stats['total']} facilities...")

            except Exception as e:
                logger.error(f"Error tagging {facility_file}: {e}")
                stats["errors"] += 1

        logger.info(f"Tagging complete: {stats}")
        return stats


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser(description="Tag facilities with derived features for measurables routing")
    parser.add_argument("--input-dir", default="facilities/", help="Input directory containing facility JSONs")
    parser.add_argument("--output-dir", help="Output directory (default: same as input)")
    parser.add_argument("--dry-run", action="store_true", help="Compute features without writing")
    parser.add_argument("--supply-data", help="Path to JSON file with global supply share data (facility_id -> share)")

    args = parser.parse_args()

    # Load supply data if provided
    global_supply_data = {}
    if args.supply_data:
        with open(args.supply_data, "r") as f:
            global_supply_data = json.load(f)

    tagger = FacilityFeatureTagger(global_supply_data=global_supply_data)
    stats = tagger.tag_all_facilities(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        dry_run=args.dry_run
    )

    print(f"\nTagging complete:")
    print(f"  Total: {stats['total']}")
    print(f"  Tagged: {stats['tagged']}")
    print(f"  Skipped: {stats['skipped']}")
    print(f"  Errors: {stats['errors']}")
