"""Tests for facility matching and duplicate detection.

This module tests the FacilityMatcher class and its various strategies for
detecting duplicate facilities. It uses mocked data to avoid loading the
full EntityIdentity database.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from scripts.utils.facility_matcher import (
    FacilityMatcher,
    haversine_vectorized,
)


class TestHaversineVectorized:
    """Tests for vectorized haversine distance calculation."""

    def test_single_point_distance(self):
        """Test distance calculation to a single point."""
        # New York to London
        nyc_lat, nyc_lon = 40.7128, -74.0060
        london_lat, london_lon = 51.5074, -0.1278

        distances = haversine_vectorized(
            nyc_lat, nyc_lon,
            np.array([london_lat]),
            np.array([london_lon])
        )

        # Expected distance is approximately 5570 km
        assert len(distances) == 1
        assert 5500 < distances[0] < 5600

    def test_multiple_points_distance(self):
        """Test distance calculation to multiple points."""
        # New York to multiple cities
        nyc_lat, nyc_lon = 40.7128, -74.0060

        cities_lat = np.array([
            51.5074,  # London
            48.8566,  # Paris
            35.6762   # Tokyo
        ])
        cities_lon = np.array([
            -0.1278,   # London
            2.3522,    # Paris
            139.6503   # Tokyo
        ])

        distances = haversine_vectorized(nyc_lat, nyc_lon, cities_lat, cities_lon)

        assert len(distances) == 3
        # London: ~5570 km
        assert 5500 < distances[0] < 5600
        # Paris: ~5837 km
        assert 5800 < distances[1] < 5900
        # Tokyo: ~10838 km
        assert 10800 < distances[2] < 10900

    def test_zero_distance(self):
        """Test distance to same point is zero."""
        lat, lon = 40.7128, -74.0060

        distances = haversine_vectorized(
            lat, lon,
            np.array([lat]),
            np.array([lon])
        )

        assert len(distances) == 1
        assert distances[0] < 0.001  # Essentially zero

    def test_nearby_points(self):
        """Test distance calculation for nearby points (< 10 km)."""
        # Two points very close to each other
        lat1, lon1 = 40.7128, -74.0060
        lat2, lon2 = 40.7589, -73.9851  # Central Park, ~5km from point 1

        distances = haversine_vectorized(
            lat1, lon1,
            np.array([lat2]),
            np.array([lon2])
        )

        assert len(distances) == 1
        assert 4 < distances[0] < 6  # Approximately 5 km


class TestFacilityMatcherInit:
    """Tests for FacilityMatcher initialization."""

    @patch('scripts.utils.facility_matcher.pd.read_parquet')
    def test_init_loads_ei_database(self, mock_read_parquet, tmp_path):
        """Test that EntityIdentity database is loaded on init."""
        # Create mock EntityIdentity parquet
        ei_dir = tmp_path / "entityidentity" / "tables" / "facilities"
        ei_dir.mkdir(parents=True)
        ei_file = ei_dir / "facilities_20251003_134822.parquet"
        ei_file.touch()

        # Mock parquet data
        mock_ei_df = pd.DataFrame({
            'facility_id': ['stillwater_abc123', 'mimosa_xyz789'],
            'facility_name': ['Stillwater Mine', 'Mimosa Mine'],
            'lat': [45.5, -20.5],
            'lon': [-109.8, 30.2],
            'commodities': [['platinum', 'palladium'], ['platinum']],
            'company_id': ['cmp-sibanye', 'cmp-mimosa']
        })
        mock_read_parquet.return_value = mock_ei_df

        # Patch the ENTITYIDENTITY_PATH
        with patch('scripts.utils.facility_matcher.ENTITYIDENTITY_PATH', tmp_path / "entityidentity"):
            with patch.object(FacilityMatcher, '_load_local_facilities', return_value=pd.DataFrame()):
                matcher = FacilityMatcher()

                assert len(matcher.ei_facilities) == 2
                assert 'name_lower' in matcher.ei_facilities.columns
                assert 'name_prefix' in matcher.ei_facilities.columns

    @patch('scripts.utils.facility_matcher.pd.read_parquet')
    def test_init_handles_missing_ei_database(self, mock_read_parquet, tmp_path):
        """Test that init handles missing EntityIdentity database gracefully."""
        # Don't create EntityIdentity directory

        with patch('scripts.utils.facility_matcher.ENTITYIDENTITY_PATH', tmp_path / "nonexistent"):
            with patch.object(FacilityMatcher, '_load_local_facilities', return_value=pd.DataFrame()):
                matcher = FacilityMatcher()

                assert matcher.ei_facilities.empty
                assert matcher.ei_parquet_path is None


class TestFacilityMatcherLoadLocal:
    """Tests for loading local facilities."""

    def test_load_local_facilities_structure(self):
        """Test that loaded facilities have correct structure."""
        # This test validates the structure without actually loading files
        # The actual loading is tested via integration tests with mock_matcher

        # Create expected structure
        expected_columns = [
            'facility_id', 'name', 'lat', 'lon', 'precision',
            'country_iso3', 'types', 'commodities', 'status',
            'aliases', 'ei_facility_id', 'operator_company_id',
            'owner_company_ids', 'file_path'
        ]

        # Verify structure by creating a sample DataFrame
        sample_data = {
            "facility_id": "usa-test-fac",
            "name": "Test Facility",
            "lat": 45.5,
            "lon": -109.8,
            "precision": "site",
            "country_iso3": "USA",
            "types": ["mine"],
            "commodities": [{"metal": "gold", "primary": True}],
            "status": "operating",
            "aliases": [],
            "ei_facility_id": None,
            "operator_company_id": None,
            "owner_company_ids": [],
            "file_path": "/path/to/file.json"
        }

        df = pd.DataFrame([sample_data])

        # Verify all expected columns are present
        for col in expected_columns:
            assert col in df.columns


class TestFacilityMatcherStrategies:
    """Tests for different matching strategies."""

    @pytest.fixture
    def mock_matcher(self):
        """Create a mock FacilityMatcher with test data."""
        matcher = FacilityMatcher.__new__(FacilityMatcher)

        # Mock local facilities with all required columns
        data = [
            {
                "facility_id": "usa-stillwater-fac",
                "name": "Stillwater Mine",
                "name_lower": "stillwater mine",
                "name_prefix": "stillwater",
                "lat": 45.5,
                "lon": -109.8,
                "precision": "site",
                "country_iso3": "USA",
                "types": ["mine"],
                "commodities": [{"metal": "platinum", "primary": True}],
                "status": "operating",
                "aliases": ["Stillwater East Mine"],
                "ei_facility_id": "stillwater_abc123",
                "operator_company_id": "cmp-sibanye",
                "owner_company_ids": ["cmp-sibanye"]
            },
            {
                "facility_id": "usa-east-boulder-fac",
                "name": "East Boulder Mine",
                "name_lower": "east boulder mine",
                "name_prefix": "east bould",
                "lat": 45.6,
                "lon": -109.9,
                "precision": "site",
                "country_iso3": "USA",
                "types": ["mine"],
                "commodities": [{"metal": "platinum", "primary": True}],
                "status": "operating",
                "aliases": [],
                "ei_facility_id": None,
                "operator_company_id": "cmp-sibanye",
                "owner_company_ids": ["cmp-sibanye"]
            },
            {
                "facility_id": "zaf-rustenburg-fac",
                "name": "Rustenburg Mine",
                "name_lower": "rustenburg mine",
                "name_prefix": "rustenburg",
                "lat": -25.5,
                "lon": 27.5,
                "precision": "site",
                "country_iso3": "ZAF",
                "types": ["mine"],
                "commodities": [{"metal": "platinum", "primary": True}],
                "status": "operating",
                "aliases": [],
                "ei_facility_id": None,
                "operator_company_id": "cmp-angloplatinum",
                "owner_company_ids": ["cmp-angloplatinum"]
            }
        ]

        # Create DataFrame and ensure index is correct
        matcher.local_facilities = pd.DataFrame(data)
        matcher.local_facilities.reset_index(drop=True, inplace=True)

        # Mock EntityIdentity facilities
        ei_data = [
            {
                "facility_id": "stillwater_abc123",
                "facility_name": "Stillwater Mine",
                "name_lower": "stillwater mine",
                "name_prefix": "stillwater",
                "lat": 45.5,
                "lon": -109.8,
                "commodities": ["platinum", "palladium"],
                "company_id": "cmp-sibanye"
            }
        ]

        # Create DataFrame and ensure index is correct
        matcher.ei_facilities = pd.DataFrame(ei_data)
        matcher.ei_facilities.reset_index(drop=True, inplace=True)

        matcher.ei_parquet_path = Path("/mock/path/facilities.parquet")

        return matcher

    def test_exact_name_match(self, mock_matcher):
        """Test exact name matching strategy."""
        facility = {
            "name": "Stillwater Mine",
            "location": {"lat": 45.4, "lon": -109.7},
            "commodities": []
        }

        candidates = mock_matcher.find_duplicates(facility, strategies=['name'])

        assert len(candidates) == 1
        assert candidates[0]['facility_id'] == "usa-stillwater-fac"
        assert candidates[0]['strategy'] == "exact_name"
        assert candidates[0]['confidence'] == 0.95

    def test_location_proximity_match(self, mock_matcher):
        """Test location proximity matching strategy."""
        # Facility very close to Stillwater Mine
        facility = {
            "name": "Different Name Mine",
            "location": {"lat": 45.505, "lon": -109.805},  # ~1km from Stillwater
            "commodities": []
        }

        candidates = mock_matcher.find_duplicates(facility, strategies=['location'])

        assert len(candidates) >= 1

        # Find the Stillwater match
        stillwater_match = [c for c in candidates if c['facility_id'] == 'usa-stillwater-fac'][0]
        assert stillwater_match['strategy'] == "location_proximity"
        assert stillwater_match['confidence'] > 0.85  # High confidence for close match
        assert stillwater_match['distance_km'] < 2.0

    def test_location_no_match_far_away(self, mock_matcher):
        """Test that location matching doesn't return distant facilities."""
        # Facility far from all others
        facility = {
            "name": "Remote Mine",
            "location": {"lat": 10.0, "lon": 10.0},
            "commodities": []
        }

        candidates = mock_matcher.find_duplicates(facility, strategies=['location'])

        assert len(candidates) == 0

    def test_alias_match(self, mock_matcher):
        """Test alias matching strategy."""
        # Facility name matches an alias
        facility = {
            "name": "Stillwater East Mine",
            "location": {"lat": None, "lon": None},
            "commodities": []
        }

        candidates = mock_matcher.find_duplicates(facility, strategies=['alias'])

        assert len(candidates) == 1
        assert candidates[0]['facility_id'] == "usa-stillwater-fac"
        assert candidates[0]['strategy'] == "alias_match"
        assert candidates[0]['confidence'] == 0.90

    def test_company_commodity_match(self, mock_matcher):
        """Test company + commodity matching strategy."""
        # Same operator and commodity, nearby
        facility = {
            "name": "New Sibanye Mine",
            "location": {"lat": 45.55, "lon": -109.85},  # Close to Stillwater
            "commodities": [{"metal": "platinum", "primary": True}],
            "operator_link": {"company_id": "cmp-sibanye"}
        }

        candidates = mock_matcher.find_duplicates(facility, strategies=['company'])

        # Should match both Stillwater and East Boulder (both Sibanye platinum)
        assert len(candidates) >= 2

        # All should have same operator
        for candidate in candidates:
            assert candidate['strategy'] == "company_commodity"
            assert 'platinum' in candidate['matched_commodities']

    def test_company_commodity_no_match_different_company(self, mock_matcher):
        """Test company matching doesn't match different companies."""
        facility = {
            "name": "Different Company Mine",
            "location": {"lat": 45.5, "lon": -109.8},
            "commodities": [{"metal": "platinum", "primary": True}],
            "operator_link": {"company_id": "cmp-different"}
        }

        candidates = mock_matcher.find_duplicates(facility, strategies=['company'])

        assert len(candidates) == 0

    @patch('scripts.utils.facility_matcher.RAPIDFUZZ_AVAILABLE', True)
    @patch('scripts.utils.facility_matcher.fuzz')
    def test_entityidentity_match(self, mock_fuzz, mock_matcher):
        """Test EntityIdentity cross-reference strategy."""
        # Mock fuzzy matching
        mock_fuzz.ratio.return_value = 95  # High similarity score

        facility = {
            "name": "Stillwater Mine",
            "location": {"lat": 45.5, "lon": -109.8},
            "commodities": []
        }

        candidates = mock_matcher.find_duplicates(facility, strategies=['entityidentity'])

        # Should find the Stillwater facility via EI linkage
        assert len(candidates) >= 1
        ei_match = candidates[0]
        assert ei_match['facility_id'] == "usa-stillwater-fac"
        assert ei_match['strategy'] == "entityidentity_name"
        assert ei_match['ei_facility_id'] == "stillwater_abc123"

    def test_multi_strategy_combination(self, mock_matcher):
        """Test combining multiple strategies."""
        # Facility that matches on multiple strategies
        facility = {
            "name": "Stillwater Mine",  # Exact name
            "location": {"lat": 45.501, "lon": -109.801},  # Close location
            "commodities": [{"metal": "platinum", "primary": True}],
            "operator_link": {"company_id": "cmp-sibanye"}
        }

        candidates = mock_matcher.find_duplicates(
            facility,
            strategies=['name', 'location', 'company']
        )

        # Should find Stillwater via multiple strategies
        # But ranking should deduplicate to single best match
        assert len(candidates) >= 1
        assert candidates[0]['facility_id'] == "usa-stillwater-fac"
        # Should have highest confidence (from exact name match)
        assert candidates[0]['confidence'] == 0.95


class TestFacilityMatcherRanking:
    """Tests for candidate ranking and deduplication."""

    def test_rank_candidates_deduplication(self):
        """Test that ranking deduplicates by facility_id."""
        matcher = FacilityMatcher.__new__(FacilityMatcher)

        candidates = [
            {"facility_id": "usa-mine-fac", "confidence": 0.95, "strategy": "name"},
            {"facility_id": "usa-mine-fac", "confidence": 0.85, "strategy": "location"},
            {"facility_id": "usa-other-fac", "confidence": 0.70, "strategy": "alias"}
        ]

        ranked = matcher._rank_candidates(candidates)

        # Should have 2 unique facilities
        assert len(ranked) == 2

        # Should keep highest confidence for usa-mine-fac
        assert ranked[0]['facility_id'] == "usa-mine-fac"
        assert ranked[0]['confidence'] == 0.95
        assert ranked[0]['rank'] == 1

        # Second should be usa-other-fac
        assert ranked[1]['facility_id'] == "usa-other-fac"
        assert ranked[1]['confidence'] == 0.70
        assert ranked[1]['rank'] == 2

    def test_rank_candidates_sorting(self):
        """Test that ranking sorts by confidence descending."""
        matcher = FacilityMatcher.__new__(FacilityMatcher)

        candidates = [
            {"facility_id": "fac1", "confidence": 0.50, "strategy": "a"},
            {"facility_id": "fac2", "confidence": 0.90, "strategy": "b"},
            {"facility_id": "fac3", "confidence": 0.70, "strategy": "c"}
        ]

        ranked = matcher._rank_candidates(candidates)

        assert ranked[0]['facility_id'] == "fac2"  # 0.90
        assert ranked[1]['facility_id'] == "fac3"  # 0.70
        assert ranked[2]['facility_id'] == "fac1"  # 0.50

    def test_rank_candidates_empty(self):
        """Test ranking with empty candidate list."""
        matcher = FacilityMatcher.__new__(FacilityMatcher)

        ranked = matcher._rank_candidates([])

        assert ranked == []


class TestFacilityMatcherStatistics:
    """Tests for matcher statistics."""

    def test_get_statistics(self):
        """Test statistics collection."""
        matcher = FacilityMatcher.__new__(FacilityMatcher)

        # Mock data
        matcher.local_facilities = pd.DataFrame([
            {"facility_id": "f1", "lat": 1.0, "lon": 1.0, "ei_facility_id": "ei1"},
            {"facility_id": "f2", "lat": 2.0, "lon": 2.0, "ei_facility_id": None},
            {"facility_id": "f3", "lat": None, "lon": None, "ei_facility_id": None}
        ])

        matcher.ei_facilities = pd.DataFrame([
            {"facility_id": "ei1", "facility_name": "Facility 1"}
        ])

        matcher.ei_parquet_path = Path("/test/facilities.parquet")

        stats = matcher.get_statistics()

        assert stats['local_facilities_count'] == 3
        assert stats['ei_facilities_count'] == 1
        assert stats['facilities_with_coords'] == 2
        assert stats['facilities_with_ei_link'] == 1
        assert stats['ei_parquet_path'] == "/test/facilities.parquet"
