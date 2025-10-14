#!/usr/bin/env python3
"""
Unit tests for entity resolution utilities.

Tests country detection and metal normalization using the entityidentity library.
"""

import sys
import pathlib
import pytest

# Add the scripts directory to path
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from utils.country_detection import (
    detect_country_from_facility,
    validate_country_code,
    iso2_to_iso3,
    iso3_to_iso2,
)

from utils.metal_normalizer import (
    normalize_commodity,
    normalize_commodities,
    get_metal_info,
    is_valid_metal,
)


# ============================================================================
# Country Detection Tests
# ============================================================================

class TestCountryDetection:
    """Test country detection and normalization."""

    def test_iso2_to_iso3(self):
        """Test ISO2 to ISO3 conversion."""
        assert iso2_to_iso3("DZ") == "DZA"
        assert iso2_to_iso3("US") == "USA"
        assert iso2_to_iso3("GB") == "GBR"
        assert iso2_to_iso3("ZA") == "ZAF"
        assert iso2_to_iso3("AF") == "AFG"

    def test_iso2_to_iso3_lowercase(self):
        """Test ISO2 to ISO3 with lowercase input."""
        assert iso2_to_iso3("dz") == "DZA"
        assert iso2_to_iso3("us") == "USA"

    def test_iso2_to_iso3_invalid(self):
        """Test ISO2 to ISO3 with invalid code."""
        with pytest.raises(ValueError):
            iso2_to_iso3("XX")

    def test_iso3_to_iso2(self):
        """Test ISO3 to ISO2 conversion."""
        assert iso3_to_iso2("DZA") == "DZ"
        assert iso3_to_iso2("USA") == "US"
        assert iso3_to_iso2("GBR") == "GB"
        assert iso3_to_iso2("ZAF") == "ZA"
        assert iso3_to_iso2("AFG") == "AF"

    def test_iso3_to_iso2_invalid(self):
        """Test ISO3 to ISO2 with invalid code."""
        with pytest.raises(ValueError):
            iso3_to_iso2("XXX")

    def test_detect_country_from_iso2(self):
        """Test country detection with ISO2 code in data."""
        facility = {
            "name": "Test Mine",
            "country": "DZ"
        }
        assert detect_country_from_facility(facility) == "DZA"

    def test_detect_country_from_iso3(self):
        """Test country detection with ISO3 code in data."""
        facility = {
            "name": "Test Mine",
            "country": "USA"
        }
        assert detect_country_from_facility(facility) == "USA"

    def test_detect_country_from_name(self):
        """Test country detection with country name."""
        facility = {
            "name": "Test Mine",
            "country": "Algeria"
        }
        assert detect_country_from_facility(facility) == "DZA"

    def test_detect_country_from_name_variations(self):
        """Test country detection with various country names."""
        test_cases = [
            ("United States", "USA"),
            ("South Africa", "ZAF"),
            ("Afghanistan", "AFG"),
        ]

        for country_name, expected_iso3 in test_cases:
            facility = {"name": "Test Mine", "country": country_name}
            assert detect_country_from_facility(facility) == expected_iso3

    def test_detect_country_from_country_iso3_field(self):
        """Test country detection with existing country_iso3 field."""
        facility = {
            "name": "Test Mine",
            "country_iso3": "DZA"
        }
        assert detect_country_from_facility(facility) == "DZA"

    def test_detect_country_from_location_dict(self):
        """Test country detection from location dictionary."""
        facility = {
            "name": "Test Mine",
            "location": {
                "country": "Algeria",
                "lat": 36.0,
                "lon": 3.0
            }
        }
        assert detect_country_from_facility(facility) == "DZA"

    def test_detect_country_from_location_string(self):
        """Test country detection from location string."""
        facility = {
            "name": "Test Mine",
            "location": "Algiers, Algeria"
        }
        assert detect_country_from_facility(facility) == "DZA"

    def test_detect_country_from_facility_name(self):
        """Test country detection from facility name with parentheses."""
        facility = {
            "name": "Test Mine (Algeria)"
        }
        assert detect_country_from_facility(facility) == "DZA"

    def test_detect_country_no_data(self):
        """Test country detection with insufficient data."""
        facility = {
            "name": "Test Mine"
        }
        with pytest.raises(ValueError, match="Cannot auto-detect country"):
            detect_country_from_facility(facility)

    def test_validate_country_code_iso2(self):
        """Test country code validation with ISO2."""
        assert validate_country_code("DZ") == "DZA"
        assert validate_country_code("US") == "USA"

    def test_validate_country_code_iso3(self):
        """Test country code validation with ISO3."""
        assert validate_country_code("DZA") == "DZA"
        assert validate_country_code("USA") == "USA"

    def test_validate_country_code_name(self):
        """Test country code validation with country name."""
        assert validate_country_code("Algeria") == "DZA"
        assert validate_country_code("United States") == "USA"

    def test_validate_country_code_invalid(self):
        """Test country code validation with invalid input."""
        with pytest.raises(ValueError):
            validate_country_code("Invalid Country")

        with pytest.raises(ValueError):
            validate_country_code("")

        with pytest.raises(ValueError):
            validate_country_code("XXXX")


# ============================================================================
# Metal Normalization Tests
# ============================================================================

class TestMetalNormalization:
    """Test metal normalization and commodity resolution."""

    def test_normalize_copper_symbol(self):
        """Test normalizing copper from chemical symbol."""
        result = normalize_commodity("Cu")
        assert result["metal"] == "copper"
        assert result["chemical_formula"] == "Cu"
        assert result["category"] in ["base_metal", "unknown"]

    def test_normalize_copper_name(self):
        """Test normalizing copper from full name."""
        result = normalize_commodity("copper")
        assert result["metal"] == "copper"
        assert result["chemical_formula"] == "Cu"

    def test_normalize_platinum_name(self):
        """Test normalizing platinum."""
        result = normalize_commodity("Platinum")
        assert result["metal"] == "platinum"
        assert result["chemical_formula"] == "Pt"

    def test_normalize_platinum_symbol(self):
        """Test normalizing platinum from symbol."""
        result = normalize_commodity("Pt")
        assert result["metal"] == "platinum"
        assert result["chemical_formula"] == "Pt"

    def test_normalize_gold(self):
        """Test normalizing gold."""
        result = normalize_commodity("gold")
        assert result["metal"] == "gold"
        assert result["chemical_formula"] == "Au"

    def test_normalize_silver(self):
        """Test normalizing silver."""
        result = normalize_commodity("Silver")
        assert result["metal"] == "silver"
        assert result["chemical_formula"] == "Ag"

    def test_normalize_iron(self):
        """Test normalizing iron."""
        result = normalize_commodity("iron")
        # EntityIdentity may resolve to "iron", "iron ore", or related compounds
        assert "iron" in result["metal"].lower()
        # Chemical formula may be "Fe", empty string, or None depending on resolution
        assert result["chemical_formula"] in ["Fe", "", None]

    def test_normalize_lithium_carbonate(self):
        """Test normalizing lithium carbonate compound."""
        result = normalize_commodity("lithium carbonate")
        # Should resolve to lithium or the compound
        assert "lithium" in result["metal"].lower()

    def test_normalize_zinc(self):
        """Test normalizing zinc."""
        result = normalize_commodity("Zinc")
        assert result["metal"] == "zinc"
        assert result["chemical_formula"] == "Zn"

    def test_normalize_lead(self):
        """Test normalizing lead."""
        result = normalize_commodity("lead")
        assert result["metal"] == "lead"
        assert result["chemical_formula"] == "Pb"

    def test_normalize_case_insensitive(self):
        """Test that normalization is case-insensitive."""
        results = [
            normalize_commodity("COPPER"),
            normalize_commodity("Copper"),
            normalize_commodity("copper"),
        ]
        # All should resolve to the same metal
        assert all(r["metal"] == "copper" for r in results)

    def test_normalize_unknown_metal(self):
        """Test normalizing unknown/invalid metal."""
        result = normalize_commodity("unobtanium")
        # Should return the input in lowercase with unknown category
        assert result["metal"] == "unobtanium"
        assert result["chemical_formula"] is None
        assert result["category"] == "unknown"

    def test_normalize_empty_string(self):
        """Test normalizing empty string."""
        result = normalize_commodity("")
        assert result["metal"] == "unknown"
        assert result["chemical_formula"] is None

    def test_normalize_none(self):
        """Test normalizing None."""
        result = normalize_commodity(None)
        assert result["metal"] == "unknown"
        assert result["chemical_formula"] is None

    def test_normalize_commodities_list(self):
        """Test normalizing a list of commodities."""
        commodities = [
            {"metal": "copper", "primary": True},
            {"metal": "gold", "primary": False},
            {"metal": "Pt", "primary": False}
        ]

        normalized = normalize_commodities(commodities)

        assert len(normalized) == 3
        assert normalized[0]["metal"] == "copper"
        assert normalized[0]["chemical_formula"] == "Cu"
        assert normalized[0]["primary"] is True

        assert normalized[1]["metal"] == "gold"
        assert normalized[1]["chemical_formula"] == "Au"
        assert normalized[1]["primary"] is False

        assert normalized[2]["metal"] == "platinum"
        assert normalized[2]["chemical_formula"] == "Pt"

    def test_normalize_commodities_empty_list(self):
        """Test normalizing empty list."""
        assert normalize_commodities([]) == []
        assert normalize_commodities(None) == []

    def test_normalize_commodities_invalid_items(self):
        """Test normalizing list with invalid items."""
        commodities = [
            {"metal": "copper", "primary": True},
            "invalid item",  # Should be skipped
            {"metal": "gold"},  # Valid but no primary flag
        ]

        normalized = normalize_commodities(commodities)

        # Should skip the invalid item
        assert len(normalized) == 2
        assert normalized[0]["metal"] == "copper"
        assert normalized[1]["metal"] == "gold"

    def test_get_metal_info(self):
        """Test getting metal information."""
        info = get_metal_info("copper")
        # Should return some information (structure may vary)
        assert info is not None
        # Check for common fields
        if info:
            assert "name" in info or "symbol" in info

    def test_get_metal_info_invalid(self):
        """Test getting metal info for invalid metal."""
        info = get_metal_info("invalid_metal_xyz")
        assert info is None

    def test_is_valid_metal_true(self):
        """Test checking valid metals."""
        assert is_valid_metal("copper") is True
        assert is_valid_metal("Cu") is True
        assert is_valid_metal("platinum") is True
        assert is_valid_metal("gold") is True

    def test_is_valid_metal_false(self):
        """Test checking invalid metals."""
        assert is_valid_metal("invalid_metal") is False
        assert is_valid_metal("xyz123") is False
        assert is_valid_metal("") is False
        assert is_valid_metal(None) is False

    def test_is_valid_metal_fuzzy_match(self):
        """Test checking metals with fuzzy matching."""
        # These should match with high confidence
        assert is_valid_metal("coppr") is True  # Typo but close enough
        assert is_valid_metal("plat") is True   # Partial match


# ============================================================================
# Integration Tests
# ============================================================================

class TestIntegration:
    """Test integration between country and metal resolution."""

    def test_full_facility_processing(self):
        """Test processing a complete facility with entity resolution."""
        facility = {
            "name": "Gara Djebilet Mine",
            "country": "Algeria",
            "commodities": [
                {"metal": "Cu", "primary": True},  # Use copper instead of Fe for consistent test
                {"metal": "gold", "primary": False}
            ]
        }

        # Detect country
        country_iso3 = detect_country_from_facility(facility)
        assert country_iso3 == "DZA"

        # Normalize commodities
        normalized_commodities = normalize_commodities(facility["commodities"])
        assert len(normalized_commodities) == 2
        assert normalized_commodities[0]["metal"] == "copper"
        assert normalized_commodities[0]["chemical_formula"] == "Cu"
        assert normalized_commodities[1]["metal"] == "gold"
        assert normalized_commodities[1]["chemical_formula"] == "Au"


# ============================================================================
# Main test runner
# ============================================================================

if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])
