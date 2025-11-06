"""
Measurables Orchestrator

Executes measurable queries using LLM providers (Perplexity, OpenAI, Anthropic).
Handles query execution, response validation, acceptance criteria, and result persistence.

Usage:
    from scripts.llm_measurables.orchestrator import MeasurablesOrchestrator

    orchestrator = MeasurablesOrchestrator(
        provider="perplexity",
        api_key=os.getenv("PERPLEXITY_API_KEY")
    )

    results = orchestrator.run_facility(facility, json_ids)
"""

import json
import uuid
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import logging
import requests

from .prompt_composer import PromptComposer
from .router import MeasurablesRouter

logger = logging.getLogger(__name__)


class MeasurablesOrchestrator:
    """Orchestrates LLM queries for facility measurables."""

    def __init__(self,
                 provider: str = "perplexity",
                 api_key: Optional[str] = None,
                 model: Optional[str] = None,
                 library_path: str = "schemas/measurables_library.json",
                 rate_limit_delay: float = 1.0):
        """
        Initialize orchestrator.

        Args:
            provider: LLM provider ("perplexity", "openai", "anthropic")
            api_key: API key for the provider
            model: Model to use (defaults per provider: sonar-pro, gpt-4o, claude-3-5-sonnet)
            library_path: Path to measurables library
            rate_limit_delay: Delay between queries in seconds
        """
        self.provider = provider
        self.api_key = api_key
        self.rate_limit_delay = rate_limit_delay

        # Default models
        default_models = {
            "perplexity": "sonar-pro",
            "openai": "gpt-4o",
            "anthropic": "claude-3-5-sonnet-20241022"
        }
        self.model = model or default_models.get(provider, "gpt-4o")

        self.composer = PromptComposer(library_path=library_path)
        self.router = MeasurablesRouter(library_path=library_path)

        # Load measurables library for acceptance rules
        with open(library_path, "r") as f:
            self.library = json.load(f)

        self.measurables_index = {m["json_id"]: m for m in self.library["measurables"]}

    def run_facility(self, facility: Dict[str, Any],
                    json_ids: Optional[List[str]] = None,
                    run_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Run measurable queries for a facility.

        Args:
            facility: Facility JSON dict
            json_ids: List of measurable JSON_IDs to query (default: auto-route)
            run_id: UUID for this batch run (default: generate new)

        Returns:
            List of result dicts matching measurable_result.schema.json
        """
        facility_id = facility.get("facility_id")

        # Auto-route if json_ids not provided
        if json_ids is None:
            json_ids = self.router.route_facility(facility)
            logger.info(f"Auto-routed {len(json_ids)} measurables for {facility_id}")

        # Generate run_id
        run_id = run_id or str(uuid.uuid4())

        results = []

        for i, json_id in enumerate(json_ids):
            logger.info(f"[{i+1}/{len(json_ids)}] Querying {json_id} for {facility_id}")

            try:
                result = self._query_measurable(facility, json_id, run_id)
                results.append(result)

                # Rate limiting
                if i < len(json_ids) - 1:
                    time.sleep(self.rate_limit_delay)

            except Exception as e:
                logger.error(f"Error querying {json_id}: {e}")
                # Create error result
                results.append(self._create_error_result(facility_id, json_id, run_id, str(e)))

        logger.info(f"Completed {len(results)} queries for {facility_id}")
        return results

    def _query_measurable(self, facility: Dict[str, Any],
                         json_id: str,
                         run_id: str) -> Dict[str, Any]:
        """
        Query a single measurable for a facility.

        Returns:
            Result dict matching measurable_result.schema.json
        """
        facility_id = facility.get("facility_id")

        # Compose prompt
        prompt, prompt_hash = self.composer.compose_prompt(facility, json_id)

        # Execute query
        start_time = time.time()
        llm_response = self._execute_llm_query(prompt)
        query_latency_ms = int((time.time() - start_time) * 1000)

        # Parse and validate response
        parsed_response, validation_errors = self._parse_and_validate_response(llm_response, json_id)

        # Apply acceptance criteria
        measurable = self.measurables_index[json_id]
        acceptance_rules = measurable.get("acceptance_rules", {})
        accepted, acceptance_reason, provisional = self._apply_acceptance_criteria(
            parsed_response, acceptance_rules, validation_errors
        )

        # Build result
        result = {
            "result_id": str(uuid.uuid4()),
            "facility_id": facility_id,
            "json_id": json_id,
            "value": parsed_response.get("value"),
            "unit": parsed_response.get("unit"),
            "as_of_date": parsed_response.get("as_of_date"),
            "confidence": parsed_response.get("confidence", 0),
            "freshness_days": parsed_response.get("freshness_days", 999),
            "evidence": parsed_response.get("evidence", []),
            "method": parsed_response.get("method", "unknown"),
            "notes": parsed_response.get("notes"),
            "query_timestamp": datetime.utcnow().isoformat() + "Z",
            "accepted": accepted,
            "provisional": provisional,
            "acceptance_reason": acceptance_reason,
            "prompt_hash": prompt_hash,
            "run_id": run_id,
            "llm_model": self.model,
            "llm_provider": self.provider,
            "query_latency_ms": query_latency_ms,
            "superseded_by": None,
            "status_change": False,  # TODO: detect status changes
            "validation_errors": validation_errors
        }

        return result

    def _execute_llm_query(self, prompt: str) -> str:
        """
        Execute LLM query via provider API.

        Args:
            prompt: Prompt text

        Returns:
            LLM response text (expected to be JSON)
        """
        if self.provider == "perplexity":
            return self._query_perplexity(prompt)
        elif self.provider == "openai":
            return self._query_openai(prompt)
        elif self.provider == "anthropic":
            return self._query_anthropic(prompt)
        else:
            raise ValueError(f"Unknown provider: {self.provider}")

    def _query_perplexity(self, prompt: str) -> str:
        """Query Perplexity API."""
        url = "https://api.perplexity.ai/chat/completions"

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": "You are a facility data analyst. Return only valid JSON matching the schema provided in the user prompt. Do not include any explanatory text outside the JSON object."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.1,  # Low temperature for factual queries
            "max_tokens": 2000
        }

        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()

        response_json = response.json()
        content = response_json.get("choices", [{}])[0].get("message", {}).get("content", "")

        return content

    def _query_openai(self, prompt: str) -> str:
        """Query OpenAI API."""
        url = "https://api.openai.com/v1/chat/completions"

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": "You are a facility data analyst. Return only valid JSON matching the schema provided in the user prompt."},
                {"role": "user", "content": prompt}
            ],
            "response_format": {"type": "json_object"},  # JSON mode
            "temperature": 0.1,
            "max_tokens": 2000
        }

        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()

        response_json = response.json()
        content = response_json.get("choices", [{}])[0].get("message", {}).get("content", "")

        return content

    def _query_anthropic(self, prompt: str) -> str:
        """Query Anthropic API."""
        url = "https://api.anthropic.com/v1/messages"

        headers = {
            "x-api-key": self.api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json"
        }

        payload = {
            "model": self.model,
            "max_tokens": 2000,
            "temperature": 0.1,
            "messages": [
                {"role": "user", "content": prompt}
            ]
        }

        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()

        response_json = response.json()
        content = response_json.get("content", [{}])[0].get("text", "")

        return content

    def _parse_and_validate_response(self, response_text: str,
                                    json_id: str) -> Tuple[Dict[str, Any], List[str]]:
        """
        Parse and validate LLM response.

        Returns:
            Tuple of (parsed_response, validation_errors)
        """
        validation_errors = []

        # Try to parse JSON
        try:
            # Extract JSON if wrapped in markdown code blocks
            if "```json" in response_text:
                start = response_text.index("```json") + 7
                end = response_text.index("```", start)
                response_text = response_text[start:end].strip()
            elif "```" in response_text:
                start = response_text.index("```") + 3
                end = response_text.index("```", start)
                response_text = response_text[start:end].strip()

            parsed = json.loads(response_text)
        except json.JSONDecodeError as e:
            validation_errors.append(f"JSON parse error: {e}")
            return {}, validation_errors

        # Validate required fields
        required_fields = ["metric", "facility", "value", "as_of_date", "confidence", "freshness_days", "evidence", "method"]

        for field in required_fields:
            if field not in parsed:
                validation_errors.append(f"Missing required field: {field}")

        # Validate metric matches json_id
        if parsed.get("metric") != json_id:
            validation_errors.append(f"Metric mismatch: expected {json_id}, got {parsed.get('metric')}")

        # Validate confidence range
        confidence = parsed.get("confidence")
        if confidence is not None:
            if not isinstance(confidence, (int, float)) or not (0 <= confidence <= 100):
                validation_errors.append(f"Confidence must be 0-100, got {confidence}")

        # Validate freshness_days
        freshness_days = parsed.get("freshness_days")
        if freshness_days is not None:
            if not isinstance(freshness_days, int) or freshness_days < 0:
                validation_errors.append(f"Freshness_days must be non-negative integer, got {freshness_days}")

        # Validate evidence is array
        evidence = parsed.get("evidence")
        if evidence is not None and not isinstance(evidence, list):
            validation_errors.append(f"Evidence must be array, got {type(evidence).__name__}")

        return parsed, validation_errors

    def _apply_acceptance_criteria(self, parsed_response: Dict[str, Any],
                                  acceptance_rules: Dict[str, Any],
                                  validation_errors: List[str]) -> Tuple[bool, str, bool]:
        """
        Apply acceptance criteria to determine if result is accepted.

        Returns:
            Tuple of (accepted, acceptance_reason, provisional)
        """
        # Reject if validation errors
        if validation_errors:
            return False, f"Validation failed: {'; '.join(validation_errors)}", False

        confidence = parsed_response.get("confidence", 0)
        freshness_days = parsed_response.get("freshness_days", 999)

        min_confidence = acceptance_rules.get("min_confidence", 60)
        max_freshness_days = acceptance_rules.get("max_freshness_days", 365)
        require_dated_source = acceptance_rules.get("require_dated_source", True)
        status_change_override = acceptance_rules.get("status_change_override", False)

        # Check evidence
        evidence = parsed_response.get("evidence", [])
        has_dated_source = any(e.get("date") for e in evidence)

        if require_dated_source and not has_dated_source:
            return False, "No dated source in evidence", False

        # Check confidence and freshness
        confidence_ok = confidence >= min_confidence
        freshness_ok = freshness_days <= max_freshness_days

        if confidence_ok and freshness_ok:
            return True, f"Passed: confidence {confidence}, freshness {freshness_days}d", False

        # Status change override (accept even if stale)
        if status_change_override and confidence >= (min_confidence - 10):
            return True, f"Status change override: confidence {confidence}, freshness {freshness_days}d", True

        # Borderline provisional
        if (confidence >= min_confidence - 5) and (freshness_days <= max_freshness_days * 1.5):
            return True, f"Provisional: confidence {confidence}, freshness {freshness_days}d", True

        # Reject
        reasons = []
        if not confidence_ok:
            reasons.append(f"confidence {confidence} < {min_confidence}")
        if not freshness_ok:
            reasons.append(f"freshness {freshness_days}d > {max_freshness_days}d")

        return False, f"Rejected: {'; '.join(reasons)}", False

    def _create_error_result(self, facility_id: str, json_id: str, run_id: str, error_msg: str) -> Dict[str, Any]:
        """Create an error result."""
        return {
            "result_id": str(uuid.uuid4()),
            "facility_id": facility_id,
            "json_id": json_id,
            "value": "error",
            "unit": None,
            "as_of_date": datetime.utcnow().date().isoformat(),
            "confidence": 0,
            "freshness_days": 999,
            "evidence": [],
            "method": "unknown",
            "notes": f"Query error: {error_msg}",
            "query_timestamp": datetime.utcnow().isoformat() + "Z",
            "accepted": False,
            "provisional": False,
            "acceptance_reason": f"Query error: {error_msg}",
            "prompt_hash": "error",
            "run_id": run_id,
            "llm_model": self.model,
            "llm_provider": self.provider,
            "query_latency_ms": 0,
            "superseded_by": None,
            "status_change": False,
            "validation_errors": [error_msg]
        }

    def save_results(self, results: List[Dict[str, Any]], output_file: str):
        """
        Save results to JSON file.

        Args:
            results: List of result dicts
            output_file: Path to output file
        """
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved {len(results)} results to {output_file}")


if __name__ == "__main__":
    import argparse
    import sys
    import os

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser(description="Execute measurable queries for facilities")
    parser.add_argument("--facility", required=True, help="Path to facility JSON")
    parser.add_argument("--json-ids", nargs="+", help="Measurable JSON_IDs to query (default: auto-route)")
    parser.add_argument("--provider", default="perplexity", choices=["perplexity", "openai", "anthropic"], help="LLM provider")
    parser.add_argument("--api-key", help="API key (default: from env)")
    parser.add_argument("--model", help="Model to use (default: provider default)")
    parser.add_argument("--output", help="Output file for results (JSON)")
    parser.add_argument("--library", default="schemas/measurables_library.json", help="Path to measurables library")

    args = parser.parse_args()

    # Get API key
    api_key = args.api_key
    if not api_key:
        env_vars = {
            "perplexity": "PERPLEXITY_API_KEY",
            "openai": "OPENAI_API_KEY",
            "anthropic": "ANTHROPIC_API_KEY"
        }
        env_var = env_vars.get(args.provider)
        api_key = os.getenv(env_var)
        if not api_key:
            print(f"Error: API key not provided and {env_var} not set", file=sys.stderr)
            sys.exit(1)

    # Load facility
    with open(args.facility, "r") as f:
        facility = json.load(f)

    # Initialize orchestrator
    orchestrator = MeasurablesOrchestrator(
        provider=args.provider,
        api_key=api_key,
        model=args.model,
        library_path=args.library
    )

    # Run queries
    results = orchestrator.run_facility(facility, json_ids=args.json_ids)

    # Print summary
    accepted = sum(1 for r in results if r["accepted"])
    provisional = sum(1 for r in results if r["provisional"])
    rejected = sum(1 for r in results if not r["accepted"])

    print(f"\n=== Query Results ===")
    print(f"Total: {len(results)}")
    print(f"Accepted: {accepted}")
    print(f"Provisional: {provisional}")
    print(f"Rejected: {rejected}")

    # Save results
    if args.output:
        orchestrator.save_results(results, args.output)
    else:
        print("\n=== Results (JSON) ===")
        print(json.dumps(results, indent=2))
