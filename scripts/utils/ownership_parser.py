"""Parse joint venture and ownership strings for mining facilities.

This module provides utilities to parse ownership text (e.g., "BHP (60%), Rio Tinto (40%)")
and resolve company names to canonical IDs using EntityIdentity.

Example:
    >>> from entityidentity.companies import EnhancedCompanyMatcher
    >>> from scripts.utils.ownership_parser import parse_ownership
    >>>
    >>> matcher = EnhancedCompanyMatcher()
    >>> owners = parse_ownership(
    ...     "Anglo American (50%), Impala Platinum (50%)",
    ...     matcher,
    ...     country_hint="ZAF"
    ... )
    >>> print(owners)
    [{'company_id': 'cmp-LEI_...', 'role': 'joint_venture', 'percentage': 50.0, ...}, ...]
"""

import logging
import re
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def parse_ownership(
    owner_text: str,
    company_matcher,  # EnhancedCompanyMatcher instance
    country_hint: Optional[str] = None
) -> List[Dict]:
    """Parse ownership structure from text and resolve company names.

    Handles various ownership text formats:
        - "BHP (60%), Rio Tinto (40%)"
        - "BHP Billiton 60%, Rio Tinto Ltd 40%"
        - "Joint venture: Anglo American 50%, Impala 50%"
        - "Sibanye-Stillwater" (single owner, no percentage)

    Args:
        owner_text: Raw ownership text
        company_matcher: EntityIdentity EnhancedCompanyMatcher instance
        country_hint: Optional ISO2/ISO3 country code for filtering

    Returns:
        List of owner_links matching facility schema:
        [
            {
                "company_id": "cmp-LEI_...",
                "role": "owner" | "joint_venture" | "minority_owner",
                "percentage": 60.0,  # or None if unknown
                "confidence": 0.92
            },
            ...
        ]

    Example:
        >>> from entityidentity.companies import EnhancedCompanyMatcher
        >>> matcher = EnhancedCompanyMatcher()
        >>> owners = parse_ownership("BHP (60%), Rio (40%)", matcher)
        >>> len(owners)
        2
    """
    if not owner_text or not owner_text.strip():
        return []

    logger.info(f"Parsing ownership: {owner_text}")

    owner_links = []

    # Pattern 1: "Company Name (XX%)" - most common format
    pattern1 = r'([^,\(\)]+?)\s*\((\d+(?:\.\d+)?)\s*%\)'
    matches = re.findall(pattern1, owner_text)

    # Pattern 2: "Company Name XX%" - alternative format without parentheses
    if not matches:
        pattern2 = r'([^,\d]+?)\s+(\d+(?:\.\d+)?)\s*%'
        matches = re.findall(pattern2, owner_text)

    if matches:
        # Parse companies with percentages
        for company_name, percentage in matches:
            company_name = company_name.strip()
            # Remove common prefixes like "Joint venture:", "JV:"
            company_name = re.sub(
                r'^(joint\s+venture|jv)\s*:\s*',
                '',
                company_name,
                flags=re.IGNORECASE
            )

            resolved = _resolve_company(company_name, company_matcher, country_hint)

            if resolved:
                percentage_float = float(percentage)

                # Determine role based on percentage
                if percentage_float > 50:
                    role = "owner"
                elif percentage_float == 50:
                    role = "joint_venture"
                else:
                    role = "minority_owner"

                owner_links.append({
                    "company_id": resolved['company_id'],
                    "role": role,
                    "percentage": percentage_float,
                    "confidence": resolved['confidence']
                })
            else:
                logger.warning(f"Could not resolve owner: {company_name}")
    else:
        # No percentages found - treat as single owner
        owner_text_clean = owner_text.strip()
        # Remove common prefixes
        owner_text_clean = re.sub(
            r'^(joint\s+venture|jv)\s*:\s*',
            '',
            owner_text_clean,
            flags=re.IGNORECASE
        )

        resolved = _resolve_company(owner_text_clean, company_matcher, country_hint)

        if resolved:
            owner_links.append({
                "company_id": resolved['company_id'],
                "role": "owner",
                "percentage": None,  # Unknown percentage
                "confidence": resolved['confidence']
            })
        else:
            logger.warning(f"Could not resolve owner: {owner_text_clean}")

    logger.info(f"Resolved {len(owner_links)} owners from text")
    return owner_links


def _resolve_company(
    company_name: str,
    company_matcher,  # EnhancedCompanyMatcher
    country_hint: Optional[str] = None
) -> Optional[Dict]:
    """Internal helper to resolve a single company name.

    Args:
        company_name: Company name to resolve
        company_matcher: EntityIdentity EnhancedCompanyMatcher instance
        country_hint: Optional country code for filtering

    Returns:
        Dict with company_id and confidence, or None if no match
    """
    if not company_name or not company_name.strip():
        return None

    try:
        # Use EntityIdentity to match company
        results = company_matcher.match_best(
            company_name,
            limit=1,
            min_score=70
        )

        if not results or len(results) == 0:
            return None

        best_match = results[0]

        # Convert score from 0-100 to 0-1
        confidence = best_match.get('score', 70) / 100.0

        # Get LEI from result (goodgleif returns 'lei' field)
        lei = best_match.get('lei', '')
        if not lei:
            lei = best_match.get('company_id', best_match.get('identifier', ''))

        # Convert to facility schema format
        if lei.startswith('cmp-'):
            company_id = lei
        else:
            company_id = f"cmp-{lei}" if lei else "cmp-unknown"

        return {
            "company_id": company_id,
            "confidence": round(confidence, 3)
        }

    except Exception as e:
        logger.error(f"Error resolving company '{company_name}': {e}")
        return None
