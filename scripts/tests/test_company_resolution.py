"""Unit tests for company resolution module.

Tests the FacilityCompanyResolver class including operator resolution,
ownership parsing, caching, and proximity boost logic.
"""

import sys
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

# Add scripts to path
SCRIPTS_PATH = Path(__file__).parent.parent
if str(SCRIPTS_PATH) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_PATH))

from utils.company_resolver import FacilityCompanyResolver, haversine_distance


class TestHaversineDistance:
    """Test the haversine distance calculation."""

    def test_distance_same_point(self):
        """Distance between same point should be 0."""
        coord = (40.7128, -74.0060)  # NYC
        distance = haversine_distance(coord, coord)
        assert distance == 0.0

    def test_distance_nyc_london(self):
        """Test known distance between NYC and London."""
        nyc = (40.7128, -74.0060)
        london = (51.5074, -0.1278)
        distance = haversine_distance(nyc, london)

        # Expected distance ~5570 km (allow 1% margin)
        assert 5500 < distance < 5650

    def test_distance_short(self):
        """Test short distance calculation."""
        # Two points ~10km apart
        point1 = (40.7128, -74.0060)
        point2 = (40.8000, -74.0060)  # ~8km north
        distance = haversine_distance(point1, point2)

        # Should be ~8-10 km
        assert 7 < distance < 11

    def test_distance_symmetry(self):
        """Distance should be symmetric."""
        point1 = (40.7128, -74.0060)
        point2 = (51.5074, -0.1278)

        dist1 = haversine_distance(point1, point2)
        dist2 = haversine_distance(point2, point1)

        assert abs(dist1 - dist2) < 0.01  # Should be equal


class TestFacilityCompanyResolver:
    """Test the FacilityCompanyResolver class."""

    @pytest.fixture
    def mock_matcher(self):
        """Create a mock EnhancedCompanyMatcher."""
        mock = Mock()
        mock.df = Mock()
        mock.df.__len__ = Mock(return_value=50000)
        return mock

    @pytest.fixture
    def resolver_with_mock(self, mock_matcher):
        """Create resolver with mocked matcher."""
        with patch('utils.company_resolver.EnhancedCompanyMatcher',
                   return_value=mock_matcher):
            resolver = FacilityCompanyResolver()
            return resolver

    def test_initialization(self, resolver_with_mock):
        """Test resolver initializes properly."""
        assert resolver_with_mock.matcher is not None
        assert isinstance(resolver_with_mock._cache, dict)
        assert len(resolver_with_mock._cache) == 0

    def test_resolve_operator_empty_name(self, resolver_with_mock):
        """Empty operator name should return None."""
        result = resolver_with_mock.resolve_operator("")
        assert result is None

        result = resolver_with_mock.resolve_operator("   ")
        assert result is None

    def test_resolve_operator_no_match(self, resolver_with_mock):
        """No match should return None."""
        resolver_with_mock.matcher.match_best.return_value = []

        result = resolver_with_mock.resolve_operator("NonexistentCompany123")
        assert result is None

    def test_resolve_operator_basic_match(self, resolver_with_mock):
        """Test basic operator resolution."""
        # Mock match result
        resolver_with_mock.matcher.match_best.return_value = [{
            'company_id': 'LEI_ABC123',
            'name': 'BHP Billiton Limited',
            'score': 95,
        }]

        result = resolver_with_mock.resolve_operator("BHP Billiton")

        assert result is not None
        assert result['company_id'] == 'cmp-LEI_ABC123'
        assert result['company_name'] == 'BHP Billiton Limited'
        assert 0.9 <= result['confidence'] <= 1.0
        assert 'match_explanation' in result

    def test_resolve_operator_score_conversion(self, resolver_with_mock):
        """Test score conversion from 0-100 to 0-1."""
        resolver_with_mock.matcher.match_best.return_value = [{
            'company_id': 'LEI_TEST',
            'name': 'Test Company',
            'score': 85,  # 85% score
        }]

        result = resolver_with_mock.resolve_operator("Test Co")

        assert result['confidence'] == 0.85

    def test_resolve_operator_with_cmp_prefix(self, resolver_with_mock):
        """Test handling of company_id that already has cmp- prefix."""
        resolver_with_mock.matcher.match_best.return_value = [{
            'company_id': 'cmp-already-has-prefix',
            'name': 'Test Company',
            'score': 90,
        }]

        result = resolver_with_mock.resolve_operator("Test")

        # Should not double-prefix
        assert result['company_id'] == 'cmp-already-has-prefix'
        assert not result['company_id'].startswith('cmp-cmp-')

    def test_resolve_operator_proximity_boost_close(self, resolver_with_mock):
        """Test proximity boost for HQ within 10km."""
        facility_coords = (40.7128, -74.0060)  # NYC
        hq_coords = (40.7200, -74.0100)  # ~1km away

        resolver_with_mock.matcher.match_best.return_value = [{
            'company_id': 'LEI_NYC',
            'name': 'NYC Company',
            'score': 80,
            'Entity.HeadquartersAddress.latitude': hq_coords[0],
            'Entity.HeadquartersAddress.longitude': hq_coords[1]
        }]

        result = resolver_with_mock.resolve_operator(
            "NYC Co",
            facility_coords=facility_coords
        )

        # Should get +0.10 boost: 0.80 + 0.10 = 0.90
        assert result['confidence'] == 0.90
        assert 'proximity boost' in result['match_explanation']

    def test_resolve_operator_proximity_boost_medium(self, resolver_with_mock):
        """Test proximity boost for HQ within 100km."""
        facility_coords = (40.7128, -74.0060)  # NYC
        hq_coords = (40.7128, -73.0060)  # ~85km away

        resolver_with_mock.matcher.match_best.return_value = [{
            'company_id': 'LEI_NEARBY',
            'name': 'Nearby Company',
            'score': 75,
            'Entity.HeadquartersAddress.latitude': hq_coords[0],
            'Entity.HeadquartersAddress.longitude': hq_coords[1]
        }]

        result = resolver_with_mock.resolve_operator(
            "Nearby Co",
            facility_coords=facility_coords
        )

        # Should get +0.05 boost: 0.75 + 0.05 = 0.80
        assert result['confidence'] == 0.80

    def test_resolve_operator_proximity_no_boost(self, resolver_with_mock):
        """Test no proximity boost for HQ >100km away."""
        facility_coords = (40.7128, -74.0060)  # NYC
        hq_coords = (51.5074, -0.1278)  # London (5500+ km)

        resolver_with_mock.matcher.match_best.return_value = [{
            'company_id': 'LEI_LONDON',
            'name': 'London Company',
            'score': 85,
            'Entity.HeadquartersAddress.latitude': hq_coords[0],
            'Entity.HeadquartersAddress.longitude': hq_coords[1]
        }]

        result = resolver_with_mock.resolve_operator(
            "London Co",
            facility_coords=facility_coords
        )

        # No boost: stays at 0.85
        assert result['confidence'] == 0.85

    def test_resolve_operator_confidence_capped(self, resolver_with_mock):
        """Test confidence is capped at 1.0."""
        facility_coords = (40.7128, -74.0060)
        hq_coords = (40.7200, -74.0100)  # Very close

        resolver_with_mock.matcher.match_best.return_value = [{
            'company_id': 'LEI_PERFECT',
            'name': 'Perfect Match',
            'score': 98,  # 0.98 before boost
            'Entity.HeadquartersAddress.latitude': hq_coords[0],
            'Entity.HeadquartersAddress.longitude': hq_coords[1]
        }]

        result = resolver_with_mock.resolve_operator(
            "Perfect Match",
            facility_coords=facility_coords
        )

        # 0.98 + 0.10 = 1.08, but should be capped at 1.0
        assert result['confidence'] == 1.0

    def test_caching(self, resolver_with_mock):
        """Test that results are cached."""
        resolver_with_mock.matcher.match_best.return_value = [{
            'company_id': 'LEI_CACHED',
            'name': 'Cached Company',
            'score': 90,
        }]

        # First call
        result1 = resolver_with_mock.resolve_operator("Cached Co")
        assert result1 is not None

        # Second call - should use cache
        result2 = resolver_with_mock.resolve_operator("Cached Co")

        # Should only call matcher once
        assert resolver_with_mock.matcher.match_best.call_count == 1

        # Results should be identical
        assert result1 == result2

    def test_caching_case_insensitive(self, resolver_with_mock):
        """Test that cache is case-insensitive."""
        resolver_with_mock.matcher.match_best.return_value = [{
            'company_id': 'LEI_CASE',
            'name': 'Case Test',
            'score': 90,
        }]

        # Different case
        resolver_with_mock.resolve_operator("CASE TEST")
        resolver_with_mock.resolve_operator("case test")
        resolver_with_mock.resolve_operator("Case Test")

        # Should only call matcher once
        assert resolver_with_mock.matcher.match_best.call_count == 1

    def test_clear_cache(self, resolver_with_mock):
        """Test cache clearing."""
        resolver_with_mock.matcher.match_best.return_value = [{
            'company_id': 'LEI_TEST',
            'name': 'Test',
            'score': 90,
        }]

        # Populate cache
        resolver_with_mock.resolve_operator("Test")
        assert len(resolver_with_mock._cache) > 0

        # Clear cache
        resolver_with_mock.clear_cache()
        assert len(resolver_with_mock._cache) == 0

    def test_resolve_owners_empty(self, resolver_with_mock):
        """Empty owner text should return empty list."""
        assert resolver_with_mock.resolve_owners("") == []
        assert resolver_with_mock.resolve_owners("   ") == []

    def test_resolve_owners_single_no_percentage(self, resolver_with_mock):
        """Single owner without percentage."""
        resolver_with_mock.matcher.match_best.return_value = [{
            'company_id': 'LEI_OWNER',
            'name': 'Single Owner Corp',
            'score': 90,
        }]

        owners = resolver_with_mock.resolve_owners("Single Owner Corp")

        assert len(owners) == 1
        assert owners[0]['company_id'] == 'cmp-LEI_OWNER'
        assert owners[0]['role'] == 'owner'
        assert owners[0]['percentage'] is None
        assert owners[0]['confidence'] == 0.90

    def test_resolve_owners_format_parentheses(self, resolver_with_mock):
        """Test parsing "Company (XX%)" format."""
        def mock_match(query, **kwargs):
            if "BHP" in query:
                return [{'company_id': 'LEI_BHP', 'name': 'BHP', 'score': 95}]
            elif "Rio" in query:
                return [{'company_id': 'LEI_RIO', 'name': 'Rio Tinto', 'score': 92}]
            return []

        resolver_with_mock.matcher.match_best.side_effect = mock_match

        owners = resolver_with_mock.resolve_owners("BHP (60%), Rio Tinto (40%)")

        assert len(owners) == 2

        # Find BHP and Rio by company_id
        bhp = next(o for o in owners if o['company_id'] == 'cmp-LEI_BHP')
        rio = next(o for o in owners if o['company_id'] == 'cmp-LEI_RIO')

        # BHP should be owner (>50%)
        assert bhp['role'] == 'owner'
        assert bhp['percentage'] == 60.0

        # Rio should be minority_owner (<=50%)
        assert rio['role'] == 'minority_owner'
        assert rio['percentage'] == 40.0

    def test_resolve_owners_format_space_percent(self, resolver_with_mock):
        """Test parsing "Company XX%" format."""
        def mock_match(query, **kwargs):
            if "Anglo" in query:
                return [{'company_id': 'LEI_ANGLO', 'name': 'Anglo American', 'score': 93}]
            elif "Impala" in query:
                return [{'company_id': 'LEI_IMPALA', 'name': 'Impala Platinum', 'score': 91}]
            return []

        resolver_with_mock.matcher.match_best.side_effect = mock_match

        owners = resolver_with_mock.resolve_owners(
            "Anglo American 50%, Impala Platinum 50%"
        )

        assert len(owners) == 2
        assert all(o['percentage'] == 50.0 for o in owners)

    def test_resolve_owners_joint_venture_prefix(self, resolver_with_mock):
        """Test handling of 'Joint venture:' prefix."""
        resolver_with_mock.matcher.match_best.return_value = [{
            'company_id': 'LEI_JV',
            'name': 'JV Company',
            'score': 90,
        }]

        owners = resolver_with_mock.resolve_owners("Joint venture: JV Company (50%)")

        assert len(owners) == 1
        # Should strip "Joint venture:" prefix before matching
        assert resolver_with_mock.matcher.match_best.call_count == 1

    def test_resolve_owners_decimal_percentage(self, resolver_with_mock):
        """Test parsing decimal percentages."""
        resolver_with_mock.matcher.match_best.return_value = [{
            'company_id': 'LEI_DECIMAL',
            'name': 'Decimal Corp',
            'score': 90,
        }]

        owners = resolver_with_mock.resolve_owners("Decimal Corp (33.33%)")

        assert len(owners) == 1
        assert owners[0]['percentage'] == 33.33

    def test_resolve_owners_unresolved_skip(self, resolver_with_mock):
        """Test that unresolved owners are skipped."""
        def mock_match(query, **kwargs):
            if "Known" in query:
                return [{'company_id': 'LEI_KNOWN', 'name': 'Known Corp', 'score': 90}]
            else:
                return []  # Unknown company

        resolver_with_mock.matcher.match_best.side_effect = mock_match

        owners = resolver_with_mock.resolve_owners(
            "Known Corp (60%), Unknown Corp (40%)"
        )

        # Should only return the resolved one
        assert len(owners) == 1
        assert owners[0]['company_id'] == 'cmp-LEI_KNOWN'

    def test_get_cache_stats(self, resolver_with_mock):
        """Test cache statistics."""
        stats = resolver_with_mock.get_cache_stats()

        assert 'cache_size' in stats
        assert 'database_size' in stats
        assert stats['cache_size'] == 0  # Empty cache
        assert stats['database_size'] == 50000  # Mock database size


class TestIntegrationWithRealMatcher:
    """Integration tests using the real EnhancedCompanyMatcher.

    These tests require the entityidentity library and data files to be available.
    They will be skipped if the dependencies are not installed.
    """

    @pytest.fixture
    def real_resolver(self):
        """Create resolver with real matcher."""
        try:
            resolver = FacilityCompanyResolver()
            return resolver
        except Exception as e:
            pytest.skip(f"Could not initialize resolver: {e}")

    def test_resolve_known_company_bhp(self, real_resolver):
        """Test resolving a well-known company (BHP)."""
        # Note: "BHP Billiton" doesn't match well, but "BHP" does
        result = real_resolver.resolve_operator("BHP")

        assert result is not None
        assert result['company_id'].startswith('cmp-')
        assert 'BHP' in result['company_name']
        assert 0.7 <= result['confidence'] <= 1.0

    def test_resolve_known_company_sibanye(self, real_resolver):
        """Test resolving Sibanye-Stillwater."""
        result = real_resolver.resolve_operator("Sibanye-Stillwater")

        assert result is not None
        assert result['company_id'].startswith('cmp-')
        assert 0.7 <= result['confidence'] <= 1.0

    def test_resolve_with_country_hint(self, real_resolver):
        """Test resolution with country hint."""
        result = real_resolver.resolve_operator("AngloGold", country_hint="ZAF")

        assert result is not None
        assert result['company_id'].startswith('cmp-')

    def test_real_ownership_parsing(self, real_resolver):
        """Test real ownership parsing."""
        owners = real_resolver.resolve_owners(
            "Anglo American Platinum (50%), Impala Platinum (50%)",
            country_hint="ZAF"
        )

        # Should resolve at least one (ideally both)
        assert len(owners) >= 1
        for owner in owners:
            assert owner['company_id'].startswith('cmp-')
            assert owner['percentage'] == 50.0
            assert 0.7 <= owner['confidence'] <= 1.0
