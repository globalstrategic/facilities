"""
LLM Measurables System

A framework for systematically querying LLMs about facility operational metrics using
structured prompts, acceptance criteria, and evidenced results.

Components:
- feature_tagger: Derive facility_features from metadata
- router: Select measurables per facility based on features
- prompt_composer: Build prompts with facility context
- orchestrator: Execute queries with LLM providers (Perplexity, OpenAI, Anthropic)

Usage:
    # Tag facilities with features
    from scripts.llm_measurables import FacilityFeatureTagger
    tagger = FacilityFeatureTagger()
    tagger.tag_all_facilities()

    # Route measurables
    from scripts.llm_measurables import MeasurablesRouter
    router = MeasurablesRouter()
    json_ids = router.route_facility(facility)

    # Run queries
    from scripts.llm_measurables import MeasurablesOrchestrator
    orchestrator = MeasurablesOrchestrator(provider="perplexity", api_key="...")
    results = orchestrator.run_facility(facility, json_ids)
"""

from .feature_tagger import FacilityFeatureTagger
from .router import MeasurablesRouter
from .prompt_composer import PromptComposer
from .orchestrator import MeasurablesOrchestrator

__all__ = [
    "FacilityFeatureTagger",
    "MeasurablesRouter",
    "PromptComposer",
    "MeasurablesOrchestrator",
]
