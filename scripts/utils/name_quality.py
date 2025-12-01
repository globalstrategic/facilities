#!/usr/bin/env python3
"""
Name quality assessment for facilities.

Identifies nonsensical, generic, or low-quality facility names.
"""

import re
from typing import Dict, List, Tuple


class NameQualityAssessor:
    """Assess the quality of facility names."""

    # Patterns that indicate poor quality names
    GENERIC_PATTERNS = [
        r'^#?\d+\s*$',  # Just numbers: "#1", "5", "123"
        r'^#?\d+\s+(mine|facility|plant|smelter|pit|quarry)$',  # "#1 Mine", "5 Plant"
        r'^\d{4,}\s+',  # Starts with 4+ digit number: "3163 Coal Mine"
        r'^(mine|facility|plant|smelter)\s+#?\d+$',  # "Mine #1", "Facility 5"
        r'^section\s+\d+',  # "Section 002"
        r'^surface\s+\d+',  # "Surface 002"
        r'^strip\s+(job|mine)\s*\d*$',  # "Strip Job", "Strip Mine"
    ]

    # Words that indicate generic names
    GENERIC_WORDS = {
        'mine', 'facility', 'plant', 'smelter', 'pit', 'quarry',
        'site', 'operation', 'project', 'deposit', 'prospect'
    }

    # Abbreviations that need expansion
    COMMON_ABBREVIATIONS = {
        'KY RCC': 'Kentucky River Coal Corporation',
        'KY': 'Kentucky',
        'MR': None,  # Unknown
        'RC': None,  # Unknown
        'RCC': None,  # Unknown
    }

    def __init__(self):
        self.compiled_patterns = [re.compile(p, re.IGNORECASE) for p in self.GENERIC_PATTERNS]

    def assess_name(self, name: str) -> Dict:
        """
        Assess the quality of a facility name.

        Returns:
            {
                'quality_score': float (0.0-1.0),
                'is_generic': bool,
                'issues': List[str],
                'suggestions': List[str],
                'abbreviations': List[str]
            }
        """
        if not name:
            return {
                'quality_score': 0.0,
                'is_generic': True,
                'issues': ['Empty name'],
                'suggestions': [],
                'abbreviations': []
            }

        issues = []
        suggestions = []
        abbreviations = []

        # Check against generic patterns
        is_generic = False
        for pattern in self.compiled_patterns:
            if pattern.search(name):
                is_generic = True
                issues.append(f'Matches generic pattern: {pattern.pattern}')
                break

        # Check for very short names (likely generic)
        if len(name) < 3:
            is_generic = True
            issues.append('Name too short (< 3 characters)')

        # Check if name is mostly numbers
        num_digits = sum(c.isdigit() for c in name)
        if num_digits / len(name) > 0.5:
            is_generic = True
            issues.append(f'Name is {int(num_digits/len(name)*100)}% numbers')

        # Check for known abbreviations
        for abbrev, expansion in self.COMMON_ABBREVIATIONS.items():
            if abbrev in name:
                abbreviations.append(abbrev)
                if expansion:
                    suggestions.append(f'Expand "{abbrev}" to "{expansion}"')
                else:
                    suggestions.append(f'Research abbreviation: {abbrev}')

        # Count generic words
        words = set(re.findall(r'\b\w+\b', name.lower()))
        generic_word_count = len(words & self.GENERIC_WORDS)
        total_words = len(words)

        if total_words > 0 and generic_word_count / total_words > 0.7:
            is_generic = True
            issues.append(f'{int(generic_word_count/total_words*100)}% generic words')

        # Check if name has specific identifiers
        has_specific_identifier = bool(re.search(r'[A-Z][a-z]{3,}', name))  # Proper nouns
        if not has_specific_identifier and len(words) <= 3:
            is_generic = True
            issues.append('No specific identifiers (proper nouns)')

        # Calculate quality score
        quality_score = 1.0
        quality_score -= 0.3 if is_generic else 0.0
        quality_score -= 0.1 * len(issues)
        quality_score -= 0.2 if len(name) < 5 else 0.0
        quality_score += 0.2 if has_specific_identifier else 0.0
        quality_score = max(0.0, min(1.0, quality_score))

        return {
            'quality_score': quality_score,
            'is_generic': is_generic,
            'issues': issues,
            'suggestions': suggestions,
            'abbreviations': abbreviations
        }

    def needs_enrichment(self, facility: Dict) -> Tuple[bool, List[str]]:
        """
        Determine if a facility needs enrichment.

        Returns:
            (needs_enrichment: bool, reasons: List[str])
        """
        reasons = []

        # Check name quality
        name = facility.get('name', '')
        name_assessment = self.assess_name(name)

        if name_assessment['is_generic']:
            reasons.append(f"Generic name (quality: {name_assessment['quality_score']:.2f})")

        # Check for missing data
        if not facility.get('location'):
            reasons.append('Missing coordinates')

        if not facility.get('company_mentions') or len(facility.get('company_mentions', [])) == 0:
            reasons.append('Missing company information')

        if facility.get('status') == 'unknown':
            reasons.append('Unknown status')

        # Check data quality flags
        flags = facility.get('data_quality', {}).get('flags', {})
        if flags.get('town_missing'):
            reasons.append('Missing location details')

        if flags.get('canonical_name_incomplete'):
            reasons.append('Incomplete canonical name')

        # Check confidence
        confidence = facility.get('verification', {}).get('confidence', 0)
        if confidence < 0.75:
            reasons.append(f'Low confidence ({confidence})')

        return (len(reasons) > 0, reasons)


def assess_facility_name(name: str) -> Dict:
    """Convenience function to assess a single name."""
    assessor = NameQualityAssessor()
    return assessor.assess_name(name)


if __name__ == '__main__':
    # Test cases
    assessor = NameQualityAssessor()

    test_names = [
        "#1 Coal Mine",
        "#1 (KY RCC)",
        "Kentucky River Coal Corporation Mine #1",
        "3163 Coal Mine",
        "#1 Strip Mine (KY)",
        "Wiley Miller Mine",
        "Abundance No 1",
        "Premier Elkhorn",
        "Long Branch Surface Mine"
    ]

    print("Name Quality Assessment Tests:")
    print("=" * 80)
    for name in test_names:
        result = assessor.assess_name(name)
        print(f"\n{name}")
        print(f"  Quality Score: {result['quality_score']:.2f}")
        print(f"  Is Generic: {result['is_generic']}")
        if result['issues']:
            print(f"  Issues: {', '.join(result['issues'])}")
        if result['suggestions']:
            print(f"  Suggestions: {', '.join(result['suggestions'])}")
