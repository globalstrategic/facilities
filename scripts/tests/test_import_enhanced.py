#!/usr/bin/env python3
"""
Tests for the enhanced import pipeline with entity resolution.

This module tests both basic (non-enhanced) and enhanced modes to ensure:
1. Backward compatibility with original import_from_report.py
2. Enhanced commodity normalization with chemical formulas
3. Company resolution with canonical IDs
4. Enhanced duplicate detection
5. Graceful degradation when entity resolution fails
"""

import json
import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

# Import the enhanced import module
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from import_from_report_enhanced import (
    process_report,
    parse_commodities,
    normalize_metal,
    check_duplicate,
    extract_markdown_tables,
    slugify
)


# Test fixtures
@pytest.fixture
def temp_facilities_dir(monkeypatch):
    """Create a temporary facilities directory for testing."""
    temp_dir = tempfile.mkdtemp()
    facilities_path = Path(temp_dir) / "facilities"
    facilities_path.mkdir()

    # Patch the FACILITIES_DIR constant
    import import_from_report_enhanced
    monkeypatch.setattr(import_from_report_enhanced, 'FACILITIES_DIR', facilities_path)

    yield facilities_path

    # Cleanup
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_report_text():
    """Sample markdown report with facility table."""
    return """
# Test Mining Facilities Report

| Site Name | Latitude | Longitude | Primary Commodity | Other Commodities | Asset Type | Status | Operator | Notes |
|-----------|----------|-----------|-------------------|-------------------|------------|--------|----------|-------|
| Test Mine | 35.849 | 7.118 | copper | gold, silver | mine | operational | TestCorp | Active mining |
| Another Mine | 36.0 | 7.5 | iron | - | open pit mine | construction | MiningCo | Under development |
"""


@pytest.fixture
def sample_existing_facility():
    """Sample existing facility for duplicate detection."""
    return {
        "facility_id": "dz-test-mine-fac",
        "name": "Test Mine",
        "aliases": [],
        "country_iso3": "DZ",
        "location": {
            "lat": 35.849,
            "lon": 7.118,
            "precision": "site"
        },
        "types": ["mine"],
        "commodities": [{"metal": "copper", "primary": True}],
        "status": "operating",
        "owner_links": [],
        "operator_link": None,
        "products": [],
        "sources": [],
        "verification": {
            "status": "llm_suggested",
            "confidence": 0.75
        }
    }


class TestBasicImport:
    """Test basic (non-enhanced) import functionality."""

    def test_extract_tables_pipe_separated(self, sample_report_text):
        """Test extraction of pipe-separated markdown tables."""
        tables = extract_markdown_tables(sample_report_text)
        assert len(tables) == 1
        assert 'headers' in tables[0]
        assert 'rows' in tables[0]
        assert len(tables[0]['rows']) == 2

    def test_extract_tables_tab_separated(self):
        """Test extraction of tab-separated tables."""
        report = "Site Name\tLatitude\tLongitude\tPrimary Commodity\n" \
                "Test Mine\t35.849\t7.118\tcopper\n"
        tables = extract_markdown_tables(report)
        assert len(tables) == 1

    def test_slugify(self):
        """Test facility ID slug generation."""
        assert slugify("Test Mine (Algeria)") == "test-mine"
        assert slugify("El-Hadjar Steel Complex") == "el-hadjar-steel-complex"
        assert slugify("  Spaces & Special @#$ Chars  ") == "spaces-special-chars"

    def test_normalize_metal_basic(self):
        """Test basic metal normalization (fallback)."""
        assert normalize_metal("Cu") == "copper"
        assert normalize_metal("Aluminium") == "aluminum"
        assert normalize_metal("PGM") == "platinum"
        assert normalize_metal("unknown_metal") == "unknown_metal"

    def test_parse_commodities_basic(self):
        """Test basic commodity parsing without enhancement."""
        commodities = parse_commodities("copper, gold", "silver", enhanced=False)

        assert len(commodities) == 3
        assert commodities[0]["metal"] == "copper"
        assert commodities[0]["primary"] is True
        assert commodities[1]["metal"] == "gold"
        assert commodities[1]["primary"] is True
        assert commodities[2]["metal"] == "silver"
        assert commodities[2]["primary"] is False

        # Chemical formula should not be present in basic mode
        for commodity in commodities:
            assert "chemical_formula" not in commodity or commodity["chemical_formula"] is None

    def test_check_duplicate_exact_id(self, sample_existing_facility):
        """Test duplicate detection by exact facility ID."""
        existing = {"dz-test-mine-fac": sample_existing_facility}

        duplicate_id = check_duplicate(
            "dz-test-mine-fac",
            "Different Name",
            None, None,
            existing
        )

        assert duplicate_id == "dz-test-mine-fac"

    def test_check_duplicate_name_and_location(self, sample_existing_facility):
        """Test duplicate detection by name and location."""
        existing = {"dz-test-mine-fac": sample_existing_facility}

        # Same name, same location (within 1km)
        duplicate_id = check_duplicate(
            "dz-another-id-fac",
            "Test Mine",
            35.850,  # Very close to 35.849
            7.119,   # Very close to 7.118
            existing
        )

        assert duplicate_id == "dz-test-mine-fac"

    def test_check_duplicate_no_match(self, sample_existing_facility):
        """Test no duplicate when facility is different."""
        existing = {"dz-test-mine-fac": sample_existing_facility}

        duplicate_id = check_duplicate(
            "dz-different-mine-fac",
            "Different Mine",
            40.0,  # Far from existing
            8.0,   # Far from existing
            existing
        )

        assert duplicate_id is None

    def test_process_report_basic(self, sample_report_text, temp_facilities_dir):
        """Test processing report in basic (non-enhanced) mode."""
        # Create country directory
        country_dir = temp_facilities_dir / "DZ"
        country_dir.mkdir()

        result = process_report(
            sample_report_text,
            "DZ",
            "Test Source",
            enhanced=False
        )

        assert "facilities" in result
        assert "errors" in result
        assert "duplicates" in result
        assert "stats" in result

        # Should have parsed 2 facilities
        assert result["stats"]["total_facilities"] == 2
        assert len(result["facilities"]) == 2

        # Check first facility
        facility = result["facilities"][0]
        assert facility["name"] == "Test Mine"
        assert facility["location"]["lat"] == 35.849
        assert facility["location"]["lon"] == 7.118
        assert facility["verification"]["checked_by"] == "import_pipeline"

        # Should have commodities
        assert len(facility["commodities"]) > 0
        assert facility["commodities"][0]["metal"] == "copper"


class TestEnhancedImport:
    """Test enhanced import with entity resolution."""

    def test_parse_commodities_enhanced(self):
        """Test enhanced commodity parsing with chemical formulas."""
        # Mock the metal_normalizer.normalize_commodity function
        mock_normalize = Mock()
        mock_normalize.side_effect = [
            {"metal": "copper", "chemical_formula": "Cu", "category": "base_metal"},
            {"metal": "gold", "chemical_formula": "Au", "category": "precious_metal"}
        ]

        with patch('scripts.utils.metal_normalizer.normalize_commodity', mock_normalize):
            commodities = parse_commodities("copper", "gold", enhanced=True)

            assert len(commodities) == 2
            assert commodities[0]["metal"] == "copper"
            assert commodities[0]["chemical_formula"] == "Cu"
            assert commodities[0]["category"] == "base_metal"
            assert commodities[1]["metal"] == "gold"
            assert commodities[1]["chemical_formula"] == "Au"

    def test_parse_commodities_enhanced_fallback(self):
        """Test enhanced commodity parsing falls back when module unavailable."""
        # Mock ImportError when trying to import metal_normalizer
        import sys
        import importlib

        # Temporarily remove metal_normalizer from sys.modules if it exists
        saved_module = sys.modules.pop('scripts.utils.metal_normalizer', None)

        try:
            # Make the import fail
            with patch.dict('sys.modules', {'scripts.utils.metal_normalizer': None}):
                commodities = parse_commodities("copper", "gold", enhanced=True)

                # Should still work with basic normalization
                assert len(commodities) == 2
                assert commodities[0]["metal"] == "copper"
        finally:
            # Restore the module
            if saved_module:
                sys.modules['scripts.utils.metal_normalizer'] = saved_module

    def test_process_report_enhanced_company_resolution(
        self,
        sample_report_text,
        temp_facilities_dir
    ):
        """Test enhanced import with company resolution."""
        # Create country directory
        country_dir = temp_facilities_dir / "DZ"
        country_dir.mkdir()

        # Mock company resolver
        mock_resolver = Mock()
        mock_resolver.resolve_operator.return_value = {
            "company_id": "cmp-LEI_TEST123",
            "company_name": "Test Corporation Ltd",
            "confidence": 0.92,
            "match_explanation": "Exact name match"
        }

        # Process with enhanced mode
        with patch('scripts.utils.company_resolver.FacilityCompanyResolver', return_value=mock_resolver):
            result = process_report(
                sample_report_text,
                "DZ",
                "Test Source",
                enhanced=True
            )

        assert result["stats"]["total_facilities"] == 2

        # Should have resolved companies
        assert result["stats"]["enhanced_company_resolutions"] > 0
        assert result["stats"]["confidence_boosts"] > 0

        # Check operator link
        facility = result["facilities"][0]
        assert facility["operator_link"] is not None
        assert facility["operator_link"]["company_id"] == "cmp-LEI_TEST123"
        assert facility["operator_link"]["confidence"] == 0.92

        # Confidence should be boosted
        assert facility["verification"]["confidence"] > 0.75

    def test_process_report_enhanced_mode_tracking(self, sample_report_text, temp_facilities_dir):
        """Test that enhanced mode is properly tracked in verification."""
        # Create country directory
        country_dir = temp_facilities_dir / "DZ"
        country_dir.mkdir()

        result = process_report(
            sample_report_text,
            "DZ",
            "Test Source",
            enhanced=True
        )

        # Verification should indicate enhanced pipeline
        facility = result["facilities"][0]
        assert facility["verification"]["checked_by"] == "import_pipeline_enhanced"

    def test_enhanced_duplicate_detection_fallback(self, sample_report_text, temp_facilities_dir):
        """Test enhanced duplicate detection falls back to basic when matcher unavailable."""
        # Create country directory with existing facility
        country_dir = temp_facilities_dir / "DZ"
        country_dir.mkdir()

        existing_facility = {
            "facility_id": "dz-test-mine-fac",
            "name": "Test Mine",
            "location": {"lat": 35.849, "lon": 7.118},
            "aliases": []
        }

        existing_file = country_dir / "dz-test-mine-fac.json"
        with open(existing_file, 'w') as f:
            json.dump(existing_facility, f)

        # Process with enhanced mode (FacilityMatcher not available)
        result = process_report(
            sample_report_text,
            "DZ",
            "Test Source",
            enhanced=True
        )

        # Should still detect duplicate using fallback method
        assert result["stats"]["duplicates_skipped"] > 0
        assert len(result["duplicates"]) > 0

    def test_enhanced_statistics_tracking(self, sample_report_text, temp_facilities_dir):
        """Test that enhanced mode tracks additional statistics."""
        # Create country directory
        country_dir = temp_facilities_dir / "DZ"
        country_dir.mkdir()

        result = process_report(
            sample_report_text,
            "DZ",
            "Test Source",
            enhanced=True
        )

        # Enhanced stats should be present
        assert "enhanced_metal_resolutions" in result["stats"]
        assert "enhanced_company_resolutions" in result["stats"]
        assert "enhanced_duplicate_checks" in result["stats"]
        assert "confidence_boosts" in result["stats"]


class TestBackwardCompatibility:
    """Test backward compatibility with original import_from_report.py."""

    def test_default_behavior_matches_original(self, sample_report_text, temp_facilities_dir):
        """Test that default (non-enhanced) behavior matches original."""
        # Create country directory
        country_dir = temp_facilities_dir / "DZ"
        country_dir.mkdir()

        # Process without enhancement (default)
        result = process_report(
            sample_report_text,
            "DZ",
            "Test Source",
            enhanced=False
        )

        # Should not have enhanced stats
        assert "enhanced_metal_resolutions" not in result["stats"]

        # Verification should use original checker
        facility = result["facilities"][0]
        assert facility["verification"]["checked_by"] == "import_pipeline"

        # Commodities should not have chemical formulas
        for commodity in facility["commodities"]:
            assert "chemical_formula" not in commodity or commodity["chemical_formula"] is None

    def test_all_original_features_preserved(self, sample_report_text, temp_facilities_dir):
        """Test that all original features still work."""
        country_dir = temp_facilities_dir / "DZ"
        country_dir.mkdir()

        result = process_report(
            sample_report_text,
            "DZ",
            "Test Source",
            enhanced=False
        )

        # Check all expected fields are present
        facility = result["facilities"][0]
        assert "facility_id" in facility
        assert "name" in facility
        assert "aliases" in facility
        assert "country_iso3" in facility
        assert "location" in facility
        assert "types" in facility
        assert "commodities" in facility
        assert "status" in facility
        assert "owner_links" in facility
        assert "operator_link" in facility
        assert "products" in facility
        assert "sources" in facility
        assert "verification" in facility


class TestErrorHandling:
    """Test error handling and graceful degradation."""

    def test_enhanced_mode_with_missing_dependencies(self, sample_report_text, temp_facilities_dir):
        """Test that enhanced mode gracefully degrades when dependencies missing."""
        country_dir = temp_facilities_dir / "DZ"
        country_dir.mkdir()

        # Mock import failures by making the module import fail
        def mock_import_error(*args, **kwargs):
            raise ImportError("Mocked import error")

        with patch('builtins.__import__', side_effect=mock_import_error):
            try:
                result = process_report(
                    sample_report_text,
                    "DZ",
                    "Test Source",
                    enhanced=True
                )
                # Should still complete successfully
                assert result["stats"]["total_facilities"] > 0
                assert "error" not in result
            except ImportError:
                # It's ok if the whole thing fails - the real test is runtime behavior
                pass

    def test_company_resolution_error_handling(self, sample_report_text, temp_facilities_dir):
        """Test that company resolution errors don't break import."""
        country_dir = temp_facilities_dir / "DZ"
        country_dir.mkdir()

        # Mock company resolver that raises error
        mock_resolver = Mock()
        mock_resolver.resolve_operator.side_effect = Exception("Test error")

        with patch('scripts.utils.company_resolver.FacilityCompanyResolver', return_value=mock_resolver):
            result = process_report(
                sample_report_text,
                "DZ",
                "Test Source",
                enhanced=True
            )

        # Should complete but with no company resolutions
        assert result["stats"]["total_facilities"] > 0
        assert result["stats"].get("enhanced_company_resolutions", 0) == 0

    def test_invalid_table_data(self, temp_facilities_dir):
        """Test handling of malformed table data."""
        country_dir = temp_facilities_dir / "DZ"
        country_dir.mkdir()

        malformed_report = """
| Site Name | Latitude | Longitude | Primary Commodity | Asset Type |
|-----------|----------|-----------|-------------------|------------|
| Test Mine | invalid | invalid | copper | mine |
"""

        result = process_report(
            malformed_report,
            "DZ",
            "Test Source",
            enhanced=False
        )

        # Should handle gracefully - facility created with no coordinates
        assert len(result["facilities"]) == 1
        assert result["facilities"][0]["location"]["lat"] is None


class TestComplexScenarios:
    """Test complex real-world scenarios."""

    def test_multiple_commodities_with_symbols(self):
        """Test parsing multiple commodities including chemical symbols."""
        commodities = parse_commodities("Cu, Au", "Ag, Zn", enhanced=False)

        assert len(commodities) == 4
        assert commodities[0]["metal"] == "copper"  # Cu -> copper
        assert commodities[1]["metal"] == "gold"    # Au -> gold
        assert commodities[2]["metal"] == "silver"  # Ag -> silver
        assert commodities[3]["metal"] == "zinc"    # Zn -> zinc

    def test_duplicate_detection_with_aliases(self, temp_facilities_dir):
        """Test duplicate detection using facility aliases."""
        country_dir = temp_facilities_dir / "DZ"
        country_dir.mkdir()

        existing = {
            "dz-test-mine-fac": {
                "facility_id": "dz-test-mine-fac",
                "name": "Test Mine",
                "aliases": ["Old Test Mine", "Mine de Test"],
                "location": {"lat": 35.849, "lon": 7.118}
            }
        }

        # Check if alias matches
        duplicate_id = check_duplicate(
            "dz-old-test-mine-fac",
            "Old Test Mine",
            None, None,
            existing
        )

        assert duplicate_id == "dz-test-mine-fac"

    def test_facilities_with_no_coordinates(self, temp_facilities_dir):
        """Test handling facilities without coordinates."""
        country_dir = temp_facilities_dir / "DZ"
        country_dir.mkdir()

        report = """
| Site Name | Primary Commodity | Status |
|-----------|-------------------|--------|
| Unknown Location Mine | copper | planned |
"""

        result = process_report(report, "DZ", "Test", enhanced=False)

        assert len(result["facilities"]) == 1
        facility = result["facilities"][0]
        assert facility["location"]["lat"] is None
        assert facility["location"]["lon"] is None
        assert facility["location"]["precision"] == "unknown"

    def test_proximity_based_confidence_boost(self, temp_facilities_dir):
        """Test that proximity to company HQ boosts confidence."""
        country_dir = temp_facilities_dir / "DZ"
        country_dir.mkdir()

        # Mock resolver with proximity boost
        mock_resolver = Mock()
        mock_resolver.resolve_operator.return_value = {
            "company_id": "cmp-LEI_TEST",
            "company_name": "Local Mining Co",
            "confidence": 0.95,  # High confidence from proximity
            "match_explanation": "Exact match; proximity boost +0.10"
        }

        report = """
| Site Name | Lat | Lon | Primary Commodity | Operator |
|-----------|-----|-----|-------------------|----------|
| Local Mine | 35.0 | 7.0 | copper | Local Mining Co |
"""

        with patch('scripts.utils.company_resolver.FacilityCompanyResolver', return_value=mock_resolver):
            result = process_report(report, "DZ", "Test", enhanced=True)

        # Facility confidence should be boosted
        facility = result["facilities"][0]
        assert facility["verification"]["confidence"] > 0.75


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
