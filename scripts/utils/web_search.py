"""
Unified web search client for facility enrichment.

Consolidates Tavily and Brave search APIs from:
- scripts/tools/geocode_null_island.py
- scripts/enrich_facilities.py

Usage:
    from utils.web_search import WebSearchClient

    client = WebSearchClient()
    results = client.search("Karee Mine South Africa coordinates")
"""

import os
import time
import logging
from typing import Dict, List, Optional

import requests
from requests.exceptions import HTTPError, Timeout, RequestException

logger = logging.getLogger(__name__)


def _should_retry(status_code: Optional[int]) -> bool:
    """Return True if status indicates a transient/rate-limit issue."""
    if status_code is None:
        return False
    return status_code in {408, 420, 429, 430, 431, 432, 499, 500, 502, 503, 504}


class WebSearchClient:
    """
    Unified web search client supporting Tavily and Brave APIs.

    Attributes:
        tavily_key: Tavily API key (from TAVILY_API_KEY env var)
        brave_key: Brave API key (from BRAVE_API_KEY env var)
        preferred_provider: Which API to try first ('tavily' or 'brave')
        fallback: Whether to try the other provider if first fails
    """

    def __init__(
        self,
        tavily_key: Optional[str] = None,
        brave_key: Optional[str] = None,
        preferred_provider: str = 'tavily',
        fallback: bool = True
    ):
        self.tavily_key = tavily_key or os.getenv('TAVILY_API_KEY')
        self.brave_key = brave_key or os.getenv('BRAVE_API_KEY')
        self.preferred_provider = preferred_provider
        self.fallback = fallback

        # Validate at least one provider is available
        if not self.tavily_key and not self.brave_key:
            logger.warning("No web search API keys configured (TAVILY_API_KEY or BRAVE_API_KEY)")

    def search(
        self,
        query: str,
        max_results: int = 10,
        retries: int = 3
    ) -> List[Dict]:
        """
        Search using configured provider(s).

        Args:
            query: Search query string
            max_results: Maximum number of results to return
            retries: Number of retry attempts per provider

        Returns:
            List of search results with 'title', 'url', 'content' keys
        """
        providers = []

        if self.preferred_provider == 'tavily' and self.tavily_key:
            providers.append(('tavily', self.tavily_key))
        elif self.preferred_provider == 'brave' and self.brave_key:
            providers.append(('brave', self.brave_key))

        # Add fallback provider
        if self.fallback:
            if self.preferred_provider == 'tavily' and self.brave_key:
                providers.append(('brave', self.brave_key))
            elif self.preferred_provider == 'brave' and self.tavily_key:
                providers.append(('tavily', self.tavily_key))

        # If preferred isn't available, try the other
        if not providers:
            if self.tavily_key:
                providers.append(('tavily', self.tavily_key))
            elif self.brave_key:
                providers.append(('brave', self.brave_key))

        if not providers:
            logger.error("No search API keys available")
            return []

        for provider, api_key in providers:
            if provider == 'tavily':
                results = self._tavily_search(query, api_key, max_results, retries)
            else:
                results = self._brave_search(query, api_key, max_results, retries)

            if results:
                return results

        return []

    def _tavily_search(
        self,
        query: str,
        api_key: str,
        max_results: int,
        retries: int
    ) -> List[Dict]:
        """Search using Tavily API."""
        url = "https://api.tavily.com/search"
        payload = {
            "api_key": api_key,
            "query": query,
            "search_depth": "advanced",
            "include_answer": True,
            "include_raw_content": False,
            "max_results": max_results,
        }

        for attempt in range(1, retries + 1):
            try:
                response = requests.post(url, json=payload, timeout=30)
                response.raise_for_status()
                data = response.json()
                return data.get("results", [])
            except HTTPError as e:
                status = e.response.status_code if e.response else None
                if _should_retry(status):
                    wait = min(60, attempt * 5)
                    logger.info(f"Tavily rate limit ({status}). Retrying in {wait}s...")
                    time.sleep(wait)
                    continue
                logger.warning(f"Tavily search error ({status}): {e}")
                break
            except (Timeout, RequestException) as e:
                wait = min(60, attempt * 5)
                logger.info(f"Tavily network issue: {e}. Retrying in {wait}s...")
                time.sleep(wait)
            except Exception as e:
                logger.warning(f"Tavily search error: {e}")
                break
        return []

    def _brave_search(
        self,
        query: str,
        api_key: str,
        max_results: int,
        retries: int
    ) -> List[Dict]:
        """Search using Brave Search API."""
        url = "https://api.search.brave.com/res/v1/web/search"
        headers = {
            "Accept": "application/json",
            "X-Subscription-Token": api_key
        }
        params = {
            "q": query,
            "count": max_results
        }

        for attempt in range(1, retries + 1):
            try:
                response = requests.get(url, headers=headers, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                # Normalize Brave results to match Tavily format
                results = []
                for item in data.get("web", {}).get("results", []):
                    results.append({
                        "title": item.get("title", ""),
                        "url": item.get("url", ""),
                        "content": item.get("description", "")
                    })
                return results
            except HTTPError as e:
                status = e.response.status_code if e.response else None
                if _should_retry(status):
                    wait = min(60, attempt * 5)
                    logger.info(f"Brave rate limit ({status}). Retrying in {wait}s...")
                    time.sleep(wait)
                    continue
                logger.warning(f"Brave search error ({status}): {e}")
                break
            except (Timeout, RequestException) as e:
                wait = min(60, attempt * 5)
                logger.info(f"Brave network issue: {e}. Retrying in {wait}s...")
                time.sleep(wait)
            except Exception as e:
                logger.warning(f"Brave search error: {e}")
                break
        return []


# Convenience functions for backward compatibility
def tavily_search(query: str, api_key: str, retries: int = 3) -> List[Dict]:
    """Standalone Tavily search (for backward compatibility)."""
    client = WebSearchClient(tavily_key=api_key, fallback=False)
    return client._tavily_search(query, api_key, max_results=10, retries=retries)


def brave_search(query: str, api_key: str, retries: int = 3) -> List[Dict]:
    """Standalone Brave search (for backward compatibility)."""
    client = WebSearchClient(brave_key=api_key, fallback=False)
    return client._brave_search(query, api_key, max_results=10, retries=retries)
