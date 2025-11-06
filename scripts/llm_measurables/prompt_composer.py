"""
Prompt Composer

Composes LLM prompts by injecting facility context into measurable templates.
Handles variable substitution, context building, and prompt hashing.

Usage:
    from scripts.llm_measurables.prompt_composer import PromptComposer

    composer = PromptComposer()
    prompt, prompt_hash = composer.compose_prompt(facility_json, measurable_json_id)
"""

import json
import hashlib
from pathlib import Path
from typing import Dict, Any, Tuple, Optional
import logging

logger = logging.getLogger(__name__)


class PromptComposer:
    """Composes prompts with facility context injection."""

    def __init__(self, library_path: str = "schemas/measurables_library.json"):
        """
        Initialize composer with measurables library.

        Args:
            library_path: Path to measurables_library.json
        """
        self.library_path = Path(library_path)
        self.measurables_index = {}

        self._load_library()

    def _load_library(self):
        """Load measurables library and index by json_id."""
        with open(self.library_path, "r", encoding="utf-8") as f:
            library = json.load(f)

        measurables = library.get("measurables", [])

        for measurable in measurables:
            json_id = measurable.get("json_id")
            self.measurables_index[json_id] = measurable

        logger.info(f"Loaded {len(self.measurables_index)} measurables into prompt composer")

    def compose_prompt(self, facility: Dict[str, Any], json_id: str) -> Tuple[str, str]:
        """
        Compose prompt for a facility + measurable pair.

        Args:
            facility: Facility JSON dict
            json_id: Measurable JSON_ID

        Returns:
            Tuple of (prompt_text, prompt_hash)
        """
        measurable = self.measurables_index.get(json_id)
        if not measurable:
            raise ValueError(f"Unknown json_id: {json_id}")

        prompt_template = measurable.get("prompt_template")
        if not prompt_template:
            raise ValueError(f"No prompt_template for {json_id}")

        # Build context dict
        context = self._build_context(facility)

        # Substitute variables
        prompt = self._substitute_variables(prompt_template, context)

        # Compute hash
        prompt_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()

        return prompt, prompt_hash

    def _build_context(self, facility: Dict[str, Any]) -> Dict[str, str]:
        """
        Build context dictionary for variable substitution.

        Variables:
        - FACILITY_CANONICAL_NAME
        - ALIASES
        - PROCESS_TYPE
        - FACILITY_TYPE
        - PRIMARY_METALS
        - COUNTRY_REGION
        - LAT_LON (optional)
        """
        canonical_name = facility.get("canonical_name") or facility.get("name", "Unknown Facility")

        aliases = facility.get("aliases", [])
        aliases_str = ", ".join(aliases) if aliases else "no known aliases"

        features = facility.get("facility_features", {})
        process_type = features.get("process_type") or "unspecified process"

        primary_type = facility.get("primary_type") or "facility"

        # Primary metals
        commodities = facility.get("commodities", [])
        primary_metals = []
        for commodity in commodities:
            if commodity.get("primary"):
                metal = commodity.get("metal")
                formula = commodity.get("chemical_formula")
                if formula:
                    primary_metals.append(f"{metal} ({formula})")
                else:
                    primary_metals.append(metal)

        primary_metals_str = ", ".join(primary_metals) if primary_metals else "unspecified commodities"

        # Country/region
        country_iso3 = facility.get("country_iso3", "Unknown")
        location = facility.get("location", {})
        region = location.get("region")
        town = location.get("town")

        country_region_parts = [country_iso3]
        if region:
            country_region_parts.append(region)
        if town:
            country_region_parts.append(f"near {town}")

        country_region_str = ", ".join(country_region_parts)

        # Lat/lon (optional)
        lat = location.get("lat")
        lon = location.get("lon")
        lat_lon_str = f"{lat}, {lon}" if lat and lon else "coordinates unavailable"

        context = {
            "FACILITY_CANONICAL_NAME": canonical_name,
            "ALIASES": aliases_str,
            "PROCESS_TYPE": process_type,
            "FACILITY_TYPE": primary_type,
            "PRIMARY_METALS": primary_metals_str,
            "COUNTRY_REGION": country_region_str,
            "LAT_LON": lat_lon_str
        }

        return context

    def _substitute_variables(self, template: str, context: Dict[str, str]) -> str:
        """
        Substitute {VARIABLE} placeholders with context values.

        Args:
            template: Prompt template with {VARIABLE} placeholders
            context: Dict mapping variable names to values

        Returns:
            Prompt with substitutions applied
        """
        prompt = template

        for var_name, var_value in context.items():
            placeholder = "{" + var_name + "}"
            prompt = prompt.replace(placeholder, var_value)

        return prompt

    def get_measurable_metadata(self, json_id: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a measurable.

        Returns:
            Dict with pack, priority, rationale, acceptance_rules, etc.
        """
        return self.measurables_index.get(json_id)

    def compose_batch(self, facility: Dict[str, Any],
                     json_ids: list[str]) -> list[Tuple[str, str, str]]:
        """
        Compose prompts for multiple measurables for a single facility.

        Args:
            facility: Facility JSON dict
            json_ids: List of measurable JSON_IDs

        Returns:
            List of tuples: (json_id, prompt_text, prompt_hash)
        """
        results = []

        for json_id in json_ids:
            try:
                prompt, prompt_hash = self.compose_prompt(facility, json_id)
                results.append((json_id, prompt, prompt_hash))
            except Exception as e:
                logger.error(f"Error composing prompt for {json_id}: {e}")

        return results


if __name__ == "__main__":
    import argparse
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser(description="Compose prompts for facility measurables")
    parser.add_argument("--facility", required=True, help="Path to facility JSON")
    parser.add_argument("--json-id", required=True, help="Measurable JSON_ID to compose")
    parser.add_argument("--library", default="schemas/measurables_library.json", help="Path to measurables library")
    parser.add_argument("--output", help="Write prompt to file (default: print to stdout)")

    args = parser.parse_args()

    composer = PromptComposer(library_path=args.library)

    with open(args.facility, "r") as f:
        facility = json.load(f)

    try:
        prompt, prompt_hash = composer.compose_prompt(facility, args.json_id)

        if args.output:
            with open(args.output, "w") as f:
                f.write(prompt)
            print(f"Prompt written to {args.output}")
            print(f"Hash: {prompt_hash}")
        else:
            print("=== Composed Prompt ===")
            print(prompt)
            print(f"\n=== Hash: {prompt_hash} ===")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
