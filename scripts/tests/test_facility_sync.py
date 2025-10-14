"""
Unit tests for facility_sync module.

Tests the bidirectional synchronization between facility JSONs and
entityidentity parquet format.
"""

import json
import pytest
from datetime import datetime
from pathlib import Path

import pandas as pd

from scripts.utils.facility_sync import (
    FacilitySyncManager,
    iso2_to_iso3,
    iso3_to_iso2
)


class TestCountryCodeConversion:
    """Test country code conversion utilities."""

    def test_iso2_to_iso3_valid(self):
        """Test valid ISO2 to ISO3 conversions."""
        assert iso2_to_iso3("DZ") == "DZA"
        assert iso2_to_iso3("US") == "USA"
        assert iso2_to_iso3("ZA") == "ZAF"
        assert iso2_to_iso3("GB") == "GBR"
        assert iso2_to_iso3("AF") == "AFG"

    def test_iso2_to_iso3_case_insensitive(self):
        """Test that ISO2 conversion is case-insensitive."""
        assert iso2_to_iso3("dz") == "DZA"
        assert iso2_to_iso3("us") == "USA"
        assert iso2_to_iso3("Us") == "USA"

    def test_iso2_to_iso3_invalid(self):
        """Test invalid ISO2 codes raise ValueError."""
        with pytest.raises(ValueError, match="Invalid ISO2 country code"):
            iso2_to_iso3("XX")
        with pytest.raises(ValueError, match="Invalid ISO2 country code"):
            iso2_to_iso3("ZZZ")

    def test_iso3_to_iso2_valid(self):
        """Test valid ISO3 to ISO2 conversions."""
        assert iso3_to_iso2("DZA") == "DZ"
        assert iso3_to_iso2("USA") == "US"
        assert iso3_to_iso2("ZAF") == "ZA"
        assert iso3_to_iso2("GBR") == "GB"
        assert iso3_to_iso2("AFG") == "AF"

    def test_iso3_to_iso2_case_insensitive(self):
        """Test that ISO3 conversion is case-insensitive."""
        assert iso3_to_iso2("dza") == "DZ"
        assert iso3_to_iso2("usa") == "US"
        assert iso3_to_iso2("UsA") == "US"

    def test_iso3_to_iso2_invalid(self):
        """Test invalid ISO3 codes raise ValueError."""
        with pytest.raises(ValueError, match="Invalid ISO3 country code"):
            iso3_to_iso2("XX")
        with pytest.raises(ValueError, match="Invalid ISO3 country code"):
            iso3_to_iso2("ZZZZ")

    def test_bidirectional_conversion(self):
        """Test round-trip conversion preserves values."""
        countries = ["DZ", "US", "ZA", "GB", "AF"]
        for iso2 in countries:
            iso3 = iso2_to_iso3(iso2)
            assert iso3_to_iso2(iso3) == iso2


class TestFacilitySyncManager:
    """Test FacilitySyncManager class."""

    @pytest.fixture
    def sample_facility(self):
        """Create a sample facility for testing."""
        return {
            "facility_id": "test-sample-mine-fac",
            "name": "Sample Mine",
            "aliases": ["Sample", "Test Mine"],
            "country_iso3": "USA",
            "location": {
                "lat": 40.7128,
                "lon": -74.0060,
                "precision": "site"
            },
            "types": ["mine", "concentrator"],
            "commodities": [
                {"metal": "copper", "primary": True},
                {"metal": "gold", "primary": False}
            ],
            "status": "operating",
            "owner_links": [
                {
                    "company_id": "cmp-test-company",
                    "role": "owner",
                    "percentage": 75.0,
                    "confidence": 0.9
                }
            ],
            "operator_link": {
                "company_id": "cmp-test-operator",
                "confidence": 0.85
            },
            "products": [
                {
                    "stream": "copper concentrate",
                    "capacity": 50000,
                    "unit": "tonnes/year",
                    "year": 2024
                }
            ],
            "sources": [
                {
                    "type": "web",
                    "url": "https://example.com/mine",
                    "date": "2025-10-14T12:00:00"
                }
            ],
            "verification": {
                "status": "human_verified",
                "confidence": 0.95,
                "last_checked": "2025-10-14T12:00:00",
                "checked_by": "test_user",
                "notes": "Verified facility"
            }
        }

    @pytest.fixture
    def temp_facilities_dir(self, tmp_path):
        """Create a temporary facilities directory structure."""
        facilities_dir = tmp_path / "facilities"
        facilities_dir.mkdir()

        # Create USA directory with a sample facility
        usa_dir = facilities_dir / "USA"
        usa_dir.mkdir()

        return facilities_dir

    @pytest.fixture
    def sync_manager(self, temp_facilities_dir):
        """Create a FacilitySyncManager with temp directory."""
        return FacilitySyncManager(facilities_dir=temp_facilities_dir)

    def test_init_with_default_dir(self):
        """Test initialization with default directory."""
        # This should work if facilities/ exists in project root
        try:
            manager = FacilitySyncManager()
            assert manager.facilities_dir.exists()
        except ValueError:
            # Expected if facilities/ doesn't exist
            pass

    def test_init_with_custom_dir(self, temp_facilities_dir):
        """Test initialization with custom directory."""
        manager = FacilitySyncManager(facilities_dir=temp_facilities_dir)
        assert manager.facilities_dir == temp_facilities_dir

    def test_init_with_nonexistent_dir(self, tmp_path):
        """Test initialization with non-existent directory raises ValueError."""
        nonexistent = tmp_path / "nonexistent"
        with pytest.raises(ValueError, match="Facilities directory not found"):
            FacilitySyncManager(facilities_dir=nonexistent)

    def test_facility_to_parquet_row(self, sync_manager, sample_facility):
        """Test conversion of facility JSON to parquet row."""
        row = sync_manager._facility_to_parquet_row(sample_facility)

        # Check basic fields
        assert row['facility_id'] == "test-sample-mine-fac"
        assert row['facility_name'] == "Sample Mine"
        assert row['alt_names'] == ["Sample", "Test Mine"]
        assert row['country_iso2'] == "US"
        assert row['facility_type'] == "mine"

        # Check location
        assert row['lat'] == 40.7128
        assert row['lon'] == -74.0060
        assert row['geo_precision'] == "site"

        # Check commodities (semi-colon separated)
        assert row['commodities'] == "copper;gold"

        # Check process stages
        assert row['process_stages'] == "mine;concentrator"

        # Check capacity
        assert row['capacity_value'] == 50000
        assert row['capacity_unit'] == "tonnes/year"
        assert row['capacity_asof'] == 2024

        # Check status
        assert row['operating_status'] == "operating"

        # Check verification
        assert row['is_verified'] is True
        assert row['confidence'] == 0.95
        assert row['verification_notes'] == "Verified facility"

        # Check company
        assert row['company_id'] == "cmp-test-operator"

        # Check evidence
        assert "https://example.com/mine" in row['evidence_urls']

    def test_parquet_row_to_facility(self, sync_manager):
        """Test conversion of parquet row to facility JSON."""
        # Create a sample parquet row
        row = pd.Series({
            'facility_id': 'test-sample-mine-fac',
            'company_id': 'cmp-test-company',
            'facility_name': 'Sample Mine',
            'alt_names': ['Sample', 'Test Mine'],
            'facility_type': 'mine',
            'country': 'United States',
            'country_iso2': 'US',
            'admin1': None,
            'city': None,
            'address': None,
            'lat': 40.7128,
            'lon': -74.0060,
            'geo_precision': 'site',
            'commodities': 'copper;gold',
            'process_stages': 'mine;concentrator',
            'capacity_value': 50000,
            'capacity_unit': 'tonnes/year',
            'capacity_asof': 2024,
            'operating_status': 'operating',
            'evidence_urls': 'https://example.com/mine',
            'evidence_titles': 'Test Source',
            'confidence': 0.85,
            'is_verified': True,
            'verification_notes': 'Verified',
            'first_seen_utc': '2025-10-14T12:00:00',
            'last_seen_utc': '2025-10-14T12:00:00',
            'source': 'web'
        })

        facility = sync_manager._parquet_row_to_facility(row)

        # Check basic fields
        assert facility['facility_id'] == 'test-sample-mine-fac'
        assert facility['name'] == 'Sample Mine'
        assert facility['aliases'] == ['Sample', 'Test Mine']
        assert facility['country_iso3'] == 'USA'

        # Check location
        assert facility['location']['lat'] == 40.7128
        assert facility['location']['lon'] == -74.0060
        assert facility['location']['precision'] == 'site'

        # Check types
        assert 'mine' in facility['types']
        assert 'concentrator' in facility['types']

        # Check commodities
        assert len(facility['commodities']) == 2
        assert facility['commodities'][0]['metal'] == 'copper'
        assert facility['commodities'][0]['primary'] is True
        assert facility['commodities'][1]['metal'] == 'gold'
        assert facility['commodities'][1]['primary'] is False

        # Check status
        assert facility['status'] == 'operating'

        # Check operator
        assert facility['operator_link'] is not None
        assert facility['operator_link']['company_id'] == 'cmp-test-company'

        # Check verification
        assert facility['verification']['status'] == 'human_verified'
        assert facility['verification']['confidence'] == 0.85

    def test_facility_exists(self, sync_manager, temp_facilities_dir, sample_facility):
        """Test facility existence check."""
        # Initially should not exist
        assert not sync_manager._facility_exists("test-sample-mine-fac")

        # Create the facility file
        usa_dir = temp_facilities_dir / "USA"
        usa_dir.mkdir(exist_ok=True)
        facility_file = usa_dir / "test-sample-mine-fac.json"
        with open(facility_file, 'w') as f:
            json.dump(sample_facility, f)

        # Now should exist
        assert sync_manager._facility_exists("test-sample-mine-fac")

    def test_export_to_entityidentity_format(
        self, sync_manager, temp_facilities_dir, sample_facility, tmp_path
    ):
        """Test export of facilities to parquet format."""
        # Create multiple facilities in temp directory
        usa_dir = temp_facilities_dir / "USA"
        usa_dir.mkdir(exist_ok=True)

        # Write sample facility
        with open(usa_dir / "test-sample-mine-fac.json", 'w') as f:
            json.dump(sample_facility, f)

        # Write another facility
        facility2 = sample_facility.copy()
        facility2['facility_id'] = "test-another-mine-fac"
        facility2['name'] = "Another Mine"
        with open(usa_dir / "test-another-mine-fac.json", 'w') as f:
            json.dump(facility2, f)

        # Export to parquet
        output_dir = tmp_path / "output"
        output_file = sync_manager.export_to_entityidentity_format(output_dir)

        # Verify file was created
        assert output_file.exists()
        assert output_file.suffix == '.parquet'
        assert output_file.name.startswith('facilities_')

        # Load and verify parquet
        df = pd.read_parquet(output_file)
        assert len(df) == 2
        assert 'facility_id' in df.columns
        assert 'facility_name' in df.columns
        assert 'country_iso2' in df.columns

        # Check data types
        assert df['lat'].dtype == 'float64'
        assert df['lon'].dtype == 'float64'
        assert df['confidence'].dtype == 'float64'
        assert df['is_verified'].dtype == 'bool'

        # Verify content
        assert 'test-sample-mine-fac' in df['facility_id'].values
        assert 'test-another-mine-fac' in df['facility_id'].values

    def test_export_empty_directory(self, sync_manager, tmp_path):
        """Test export with no facilities raises ValueError."""
        output_dir = tmp_path / "output"
        with pytest.raises(ValueError, match="No facilities found to export"):
            sync_manager.export_to_entityidentity_format(output_dir)

    def test_import_from_entityidentity(
        self, sync_manager, temp_facilities_dir, tmp_path
    ):
        """Test import from entityidentity parquet."""
        # Create a sample parquet file
        data = {
            'facility_id': ['test-import-mine-fac'],
            'company_id': ['cmp-test'],
            'facility_name': ['Import Test Mine'],
            'alt_names': [['Import', 'Test']],
            'facility_type': ['mine'],
            'country': ['United States'],
            'country_iso2': ['US'],
            'admin1': [None],
            'city': [None],
            'address': [None],
            'lat': [35.0],
            'lon': [-110.0],
            'geo_precision': ['site'],
            'commodities': ['copper'],
            'process_stages': ['mine'],
            'capacity_value': [10000],
            'capacity_unit': ['tonnes/year'],
            'capacity_asof': [2024],
            'operating_status': ['operating'],
            'evidence_urls': ['https://example.com'],
            'evidence_titles': ['Test'],
            'confidence': [0.8],
            'is_verified': [True],
            'verification_notes': ['Test note'],
            'first_seen_utc': ['2025-10-14T12:00:00'],
            'last_seen_utc': ['2025-10-14T12:00:00'],
            'source': ['web']
        }
        df = pd.DataFrame(data)

        # Save to parquet
        parquet_file = tmp_path / "test_facilities.parquet"
        df.to_parquet(parquet_file, index=False)

        # Import
        stats = sync_manager.import_from_entityidentity(parquet_file)

        # Verify stats
        assert stats['imported'] == 1
        assert stats['skipped'] == 0
        assert stats['failed'] == 0

        # Verify file was created
        usa_dir = temp_facilities_dir / "USA"
        facility_file = usa_dir / "test-import-mine-fac.json"
        assert facility_file.exists()

        # Verify content
        with open(facility_file, 'r') as f:
            facility = json.load(f)

        assert facility['facility_id'] == 'test-import-mine-fac'
        assert facility['name'] == 'Import Test Mine'
        assert facility['country_iso3'] == 'USA'

    def test_import_with_overwrite(
        self, sync_manager, temp_facilities_dir, tmp_path, sample_facility
    ):
        """Test import with overwrite flag."""
        # Create existing facility
        usa_dir = temp_facilities_dir / "USA"
        usa_dir.mkdir(exist_ok=True)
        facility_file = usa_dir / "test-import-mine-fac.json"
        with open(facility_file, 'w') as f:
            json.dump(sample_facility, f)

        # Create parquet with same facility_id
        data = {
            'facility_id': ['test-import-mine-fac'],
            'company_id': ['cmp-new'],
            'facility_name': ['Updated Mine'],
            'alt_names': [[]],
            'facility_type': ['mine'],
            'country': ['United States'],
            'country_iso2': ['US'],
            'admin1': [None],
            'city': [None],
            'address': [None],
            'lat': [35.0],
            'lon': [-110.0],
            'geo_precision': ['site'],
            'commodities': ['gold'],
            'process_stages': ['mine'],
            'capacity_value': [None],
            'capacity_unit': [None],
            'capacity_asof': [None],
            'operating_status': ['operating'],
            'evidence_urls': [''],
            'evidence_titles': [None],
            'confidence': [0.7],
            'is_verified': [False],
            'verification_notes': [None],
            'first_seen_utc': ['2025-10-14T12:00:00'],
            'last_seen_utc': ['2025-10-14T12:00:00'],
            'source': ['web']
        }
        df = pd.DataFrame(data)
        parquet_file = tmp_path / "test_facilities.parquet"
        df.to_parquet(parquet_file, index=False)

        # Import without overwrite - should skip
        stats = sync_manager.import_from_entityidentity(parquet_file, overwrite=False)
        assert stats['imported'] == 0
        assert stats['skipped'] == 1

        # Verify original file unchanged
        with open(facility_file, 'r') as f:
            facility = json.load(f)
        assert facility['name'] == 'Sample Mine'  # Original name

        # Import with overwrite - should update
        stats = sync_manager.import_from_entityidentity(parquet_file, overwrite=True)
        assert stats['imported'] == 1
        assert stats['skipped'] == 0

        # Verify file was updated
        with open(facility_file, 'r') as f:
            facility = json.load(f)
        assert facility['name'] == 'Updated Mine'  # New name

    def test_import_nonexistent_file(self, sync_manager, tmp_path):
        """Test import with non-existent file raises FileNotFoundError."""
        nonexistent = tmp_path / "nonexistent.parquet"
        with pytest.raises(FileNotFoundError, match="Parquet file not found"):
            sync_manager.import_from_entityidentity(nonexistent)

    def test_round_trip_conversion(
        self, sync_manager, temp_facilities_dir, sample_facility, tmp_path
    ):
        """Test that export->import preserves facility data."""
        # Write sample facility
        usa_dir = temp_facilities_dir / "USA"
        usa_dir.mkdir(exist_ok=True)
        original_file = usa_dir / "test-sample-mine-fac.json"
        with open(original_file, 'w') as f:
            json.dump(sample_facility, f)

        # Export to parquet
        output_dir = tmp_path / "output"
        parquet_file = sync_manager.export_to_entityidentity_format(output_dir)

        # Clear facilities directory
        original_file.unlink()

        # Import back
        stats = sync_manager.import_from_entityidentity(parquet_file)
        assert stats['imported'] == 1

        # Compare facilities
        with open(original_file, 'r') as f:
            imported_facility = json.load(f)

        # Check key fields preserved
        assert imported_facility['facility_id'] == sample_facility['facility_id']
        assert imported_facility['name'] == sample_facility['name']
        assert imported_facility['country_iso3'] == sample_facility['country_iso3']
        assert imported_facility['location']['lat'] == sample_facility['location']['lat']
        assert imported_facility['location']['lon'] == sample_facility['location']['lon']
        assert imported_facility['status'] == sample_facility['status']

        # Commodities should match (order matters for primary)
        assert len(imported_facility['commodities']) == len(sample_facility['commodities'])
        assert imported_facility['commodities'][0]['metal'] == sample_facility['commodities'][0]['metal']


class TestParquetSchemaCompatibility:
    """Test that exported parquet matches entityidentity schema."""

    def test_column_names_match(self, tmp_path):
        """Test that exported parquet has correct column names."""
        # Create a minimal test setup
        facilities_dir = tmp_path / "facilities"
        usa_dir = facilities_dir / "USA"
        usa_dir.mkdir(parents=True)

        # Create minimal facility
        facility = {
            "facility_id": "test-schema-mine-fac",
            "name": "Schema Test",
            "aliases": [],
            "country_iso3": "USA",
            "location": {"lat": None, "lon": None, "precision": "unknown"},
            "types": ["mine"],
            "commodities": [],
            "status": "unknown",
            "owner_links": [],
            "operator_link": None,
            "products": [],
            "sources": [{"type": "manual", "id": "test"}],
            "verification": {
                "status": "csv_imported",
                "confidence": 0.5,
                "last_checked": "2025-10-14T12:00:00",
                "checked_by": "test"
            }
        }

        with open(usa_dir / "test-schema-mine-fac.json", 'w') as f:
            json.dump(facility, f)

        # Export
        manager = FacilitySyncManager(facilities_dir=facilities_dir)
        output_dir = tmp_path / "output"
        parquet_file = manager.export_to_entityidentity_format(output_dir)

        # Load and check columns
        df = pd.read_parquet(parquet_file)

        # Expected columns from entityidentity schema
        expected_columns = [
            'facility_id', 'company_id', 'facility_name', 'alt_names', 'facility_type',
            'country', 'country_iso2', 'admin1', 'city', 'address',
            'lat', 'lon', 'geo_precision',
            'commodities', 'process_stages', 'capacity_value', 'capacity_unit', 'capacity_asof',
            'operating_status', 'evidence_urls', 'evidence_titles',
            'confidence', 'is_verified', 'verification_notes',
            'first_seen_utc', 'last_seen_utc', 'source'
        ]

        assert set(df.columns) == set(expected_columns)

    def test_data_types_match(self, tmp_path):
        """Test that exported parquet has correct data types."""
        # Create test setup
        facilities_dir = tmp_path / "facilities"
        usa_dir = facilities_dir / "USA"
        usa_dir.mkdir(parents=True)

        facility = {
            "facility_id": "test-types-mine-fac",
            "name": "Types Test",
            "aliases": ["Test"],
            "country_iso3": "USA",
            "location": {"lat": 40.0, "lon": -110.0, "precision": "site"},
            "types": ["mine"],
            "commodities": [{"metal": "copper", "primary": True}],
            "status": "operating",
            "owner_links": [],
            "operator_link": {"company_id": "cmp-test", "confidence": 0.8},
            "products": [{"stream": "copper", "capacity": 1000, "unit": "t/y", "year": 2024}],
            "sources": [{"type": "web", "url": "https://example.com"}],
            "verification": {
                "status": "human_verified",
                "confidence": 0.9,
                "last_checked": "2025-10-14T12:00:00",
                "checked_by": "test"
            }
        }

        with open(usa_dir / "test-types-mine-fac.json", 'w') as f:
            json.dump(facility, f)

        # Export
        manager = FacilitySyncManager(facilities_dir=facilities_dir)
        output_dir = tmp_path / "output"
        parquet_file = manager.export_to_entityidentity_format(output_dir)

        # Load and check types
        df = pd.read_parquet(parquet_file)

        assert df['lat'].dtype == 'float64'
        assert df['lon'].dtype == 'float64'
        assert df['confidence'].dtype == 'float64'
        assert df['is_verified'].dtype == 'bool'
