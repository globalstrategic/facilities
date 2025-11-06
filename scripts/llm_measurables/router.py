"""
Measurables Router

Selects which measurable questions to ask for each facility based on facility_features.
Implements the routing logic: Core Pack always, plus conditional packs based on features.

Usage:
    from scripts.llm_measurables.router import MeasurablesRouter

    router = MeasurablesRouter()
    json_ids = router.route_facility(facility_json)
    # Returns: ["supply.facility.status.current_operational_state", ...]
"""

import json
from pathlib import Path
from typing import Dict, Any, List, Set, Optional
import logging

logger = logging.getLogger(__name__)


class MeasurablesRouter:
    """Routes measurables to facilities based on features and FCS."""

    def __init__(self, library_path: str = "schemas/measurables_library.json"):
        """
        Initialize router with measurables library.

        Args:
            library_path: Path to measurables_library.json
        """
        self.library_path = Path(library_path)
        self.measurables = []
        self.pack_index = {}

        self._load_library()

    def _load_library(self):
        """Load measurables library from JSON."""
        with open(self.library_path, "r", encoding="utf-8") as f:
            library = json.load(f)

        self.measurables = library.get("measurables", [])

        # Build pack index
        for measurable in self.measurables:
            pack = measurable.get("pack")
            if pack not in self.pack_index:
                self.pack_index[pack] = []
            self.pack_index[pack].append(measurable)

        logger.info(f"Loaded {len(self.measurables)} measurables across {len(self.pack_index)} packs")

    def route_facility(self, facility: Dict[str, Any],
                      include_packs: Optional[List[str]] = None,
                      exclude_packs: Optional[List[str]] = None) -> List[str]:
        """
        Route measurables for a single facility.

        Args:
            facility: Facility JSON dict with facility_features
            include_packs: Optional list of pack names to force include
            exclude_packs: Optional list of pack names to force exclude

        Returns:
            List of json_ids to query for this facility
        """
        features = facility.get("facility_features", {})

        if not features:
            logger.warning(f"Facility {facility.get('facility_id')} has no facility_features, using Core Pack only")
            return self._get_pack_json_ids("core")

        # Always include Core Pack
        selected_packs = {"core"}

        # Apply conditional routing rules
        selected_packs.update(self._apply_routing_rules(features))

        # Apply manual includes/excludes
        if include_packs:
            selected_packs.update(include_packs)
        if exclude_packs:
            selected_packs.difference_update(exclude_packs)

        # Get all json_ids for selected packs
        json_ids = []
        for pack in selected_packs:
            json_ids.extend(self._get_pack_json_ids(pack))

        return json_ids

    def _apply_routing_rules(self, features: Dict[str, Any]) -> Set[str]:
        """
        Apply routing rules to select conditional packs.

        Rules:
        - acid_dependent: process_type in {sxew, hpal, heap_leach} OR acid_dependency in {high, medium}
        - power_intensive: power_intensity in {very_high, high} OR country_risk_bucket >= 3
        - rain_sensitive: mine_method in {open_pit, both} AND climate_zone in {tropical, monsoon}
        - smelter_refiner: primary_type in {smelter, refinery}
        - security_risk: country_risk_bucket >= 3
        """
        selected_packs = set()

        process_type = features.get("process_type")
        mine_method = features.get("mine_method")
        acid_dependency = features.get("acid_dependency")
        power_intensity = features.get("power_intensity")
        climate_zone = features.get("climate_zone")
        country_risk_bucket = features.get("country_risk_bucket", 3)

        # Get primary_type from parent facility (not in features)
        # For now, we'll need to check measurables with routing_criteria
        # and match against features

        # Acid-dependent pack
        if process_type in ["sxew", "hpal", "heap_leach"] or acid_dependency in ["high", "medium"]:
            selected_packs.add("acid_dependent")

        # Power-intensive pack
        if power_intensity in ["very_high", "high"] or country_risk_bucket >= 3:
            selected_packs.add("power_intensive")

        # Rain-sensitive pack
        if mine_method in ["open_pit", "both"] and climate_zone in ["tropical", "monsoon"]:
            selected_packs.add("rain_sensitive")

        # Smelter/refiner pack - need to check this from measurables with routing_criteria
        # For now, skip (will be added in next iteration with primary_type in features)

        # Security risk pack
        if country_risk_bucket >= 3:
            selected_packs.add("security_risk")

        return selected_packs

    def _get_pack_json_ids(self, pack_name: str) -> List[str]:
        """Get all json_ids for a given pack."""
        measurables = self.pack_index.get(pack_name, [])
        return [m["json_id"] for m in measurables]

    def get_cadence(self, facility: Dict[str, Any]) -> str:
        """
        Get query cadence for a facility based on FCS.

        Cadence rules:
        - FCS >= 90th percentile: daily
        - FCS 70-90th percentile: 3x/week (Mon/Wed/Fri)
        - FCS 40-70th percentile: weekly (Monday)
        - FCS < 40th percentile: monthly (1st of month)

        Returns:
            Cadence string: "daily", "3x_week", "weekly", "monthly"
        """
        features = facility.get("facility_features", {})
        fcs = features.get("consequentiality_score", 0.0)

        # Hardcoded percentile thresholds for now
        # In production, compute from entire facility population
        if fcs >= 80:  # Top decile proxy
            return "daily"
        elif fcs >= 60:  # D2-D5 proxy
            return "3x_week"
        elif fcs >= 30:  # D6-D10 proxy
            return "weekly"
        else:
            return "monthly"

    def route_all_facilities(self, input_dir: str = "facilities/",
                            output_file: Optional[str] = None) -> Dict[str, List[str]]:
        """
        Route measurables for all facilities.

        Args:
            input_dir: Directory containing facility JSONs
            output_file: Optional path to write routing results (facility_id -> [json_ids])

        Returns:
            Dict mapping facility_id -> list of json_ids
        """
        input_path = Path(input_dir)
        facility_files = list(input_path.glob("*/*-fac.json"))

        routing_results = {}
        pack_stats = {}

        for facility_file in facility_files:
            try:
                with open(facility_file, "r", encoding="utf-8") as f:
                    facility = json.load(f)

                facility_id = facility.get("facility_id")
                json_ids = self.route_facility(facility)
                routing_results[facility_id] = json_ids

                # Track pack usage
                features = facility.get("facility_features", {})
                selected_packs = self._apply_routing_rules(features)
                selected_packs.add("core")  # Always included

                for pack in selected_packs:
                    pack_stats[pack] = pack_stats.get(pack, 0) + 1

            except Exception as e:
                logger.error(f"Error routing {facility_file}: {e}")

        logger.info(f"Routed {len(routing_results)} facilities")
        logger.info(f"Pack usage: {pack_stats}")

        if output_file:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(routing_results, f, indent=2)
            logger.info(f"Routing results written to {output_file}")

        return routing_results

    def get_facility_routing_summary(self, facility: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get a detailed routing summary for a facility.

        Returns:
            Dict with routing details including selected packs, json_ids, cadence, and reasoning
        """
        facility_id = facility.get("facility_id")
        features = facility.get("facility_features", {})

        json_ids = self.route_facility(facility)
        selected_packs = self._apply_routing_rules(features)
        selected_packs.add("core")

        cadence = self.get_cadence(facility)

        return {
            "facility_id": facility_id,
            "canonical_name": facility.get("canonical_name"),
            "fcs": features.get("consequentiality_score", 0.0),
            "cadence": cadence,
            "selected_packs": sorted(selected_packs),
            "json_ids": json_ids,
            "count": len(json_ids),
            "features_summary": {
                "process_type": features.get("process_type"),
                "mine_method": features.get("mine_method"),
                "acid_dependency": features.get("acid_dependency"),
                "power_intensity": features.get("power_intensity"),
                "climate_zone": features.get("climate_zone"),
                "country_risk_bucket": features.get("country_risk_bucket")
            }
        }


if __name__ == "__main__":
    import argparse
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser(description="Route measurables to facilities based on features")
    parser.add_argument("--facility", help="Path to single facility JSON to route")
    parser.add_argument("--all", action="store_true", help="Route all facilities")
    parser.add_argument("--input-dir", default="facilities/", help="Input directory for --all mode")
    parser.add_argument("--output", help="Output file for routing results (JSON)")
    parser.add_argument("--library", default="schemas/measurables_library.json", help="Path to measurables library")

    args = parser.parse_args()

    router = MeasurablesRouter(library_path=args.library)

    if args.facility:
        # Single facility mode
        with open(args.facility, "r") as f:
            facility = json.load(f)

        summary = router.get_facility_routing_summary(facility)

        print(f"\n=== Routing Summary for {summary['canonical_name']} ===")
        print(f"Facility ID: {summary['facility_id']}")
        print(f"FCS: {summary['fcs']:.2f}")
        print(f"Cadence: {summary['cadence']}")
        print(f"Selected Packs: {', '.join(summary['selected_packs'])}")
        print(f"Measurable Count: {summary['count']}")
        print(f"\nFeatures:")
        for key, value in summary['features_summary'].items():
            print(f"  {key}: {value}")
        print(f"\nMeasurables:")
        for json_id in summary['json_ids']:
            print(f"  - {json_id}")

    elif args.all:
        # All facilities mode
        routing_results = router.route_all_facilities(
            input_dir=args.input_dir,
            output_file=args.output
        )

        # Summary stats
        total_facilities = len(routing_results)
        total_measurables = sum(len(ids) for ids in routing_results.values())
        avg_measurables = total_measurables / total_facilities if total_facilities > 0 else 0

        print(f"\n=== Routing Summary ===")
        print(f"Total Facilities: {total_facilities}")
        print(f"Total Measurable Queries: {total_measurables}")
        print(f"Average per Facility: {avg_measurables:.1f}")

    else:
        parser.print_help()
        sys.exit(1)
