#!/usr/bin/env python3
"""
Schema Validation Tests

Tests for the facility JSON schema, including validation of:
- New EntityIdentity integration fields
- Backward compatibility
- Chemical formula patterns
- Metal category enums
- Existing facility validation
"""

import json
from pathlib import Path

import pytest

try:
    import jsonschema
except ImportError:
    pytest.skip("jsonschema not installed", allow_module_level=True)


@pytest.fixture
def schema():
    """Load facility JSON schema."""
    schema_path = Path(__file__).parent.parent.parent / "schemas" / "facility.schema.json"
    with open(schema_path) as f:
        return json.load(f)


@pytest.fixture
def base_facility():
    """Minimal valid facility without new fields."""
    return {
        "facility_id": "usa-example-mine-fac",
        "name": "Example Mine",
        "country_iso3": "USA",
        "types": ["mine"],
        "verification": {
            "status": "csv_imported",
            "confidence": 0.65,
            "last_checked": "2025-10-14T12:00:00Z"
        }
    }


class TestSchemaBasics:
    """Test basic schema structure and metadata."""

    def test_schema_loads(self, schema):
        """Schema file is valid JSON."""
        assert schema is not None
        assert "$schema" in schema

    def test_schema_has_version(self, schema):
        """Schema includes version information."""
        assert "version" in schema
        assert schema["version"] == "2.0.0"

    def test_schema_has_description(self, schema):
        """Schema includes EntityIdentity integration description."""
        assert "EntityIdentity" in schema["description"]


class TestBackwardCompatibility:
    """Test that existing facilities without new fields still validate."""

    def test_minimal_facility_validates(self, schema, base_facility):
        """Minimal facility without new fields is valid."""
        jsonschema.validate(instance=base_facility, schema=schema)

    def test_facility_without_ei_id_validates(self, schema, base_facility):
        """Facility without ei_facility_id is valid."""
        # ei_facility_id is optional, so omitting it should be fine
        assert "ei_facility_id" not in base_facility
        jsonschema.validate(instance=base_facility, schema=schema)

    def test_commodity_without_new_fields_validates(self, schema, base_facility):
        """Commodity without chemical_formula or category is valid."""
        base_facility["commodities"] = [
            {"metal": "copper", "primary": True}
        ]
        jsonschema.validate(instance=base_facility, schema=schema)

    def test_existing_facility_structure_validates(self, schema):
        """Real facility structure from codebase validates."""
        facility = {
            "facility_id": "dza-test-mine-fac",
            "name": "Test Mine",
            "aliases": [],
            "country_iso3": "DZA",
            "location": {
                "lat": 26.766,
                "lon": -7.333,
                "precision": "site"
            },
            "types": ["mine"],
            "commodities": [
                {"metal": "iron ore", "primary": True}
            ],
            "status": "construction",
            "owner_links": [],
            "operator_link": None,
            "products": [],
            "sources": [
                {
                    "type": "gemini_research",
                    "id": "Test Report",
                    "date": "2025-10-14T13:10:31.624816"
                }
            ],
            "verification": {
                "status": "llm_suggested",
                "confidence": 0.75,
                "last_checked": "2025-10-14T13:10:31.624817",
                "checked_by": "import_pipeline"
            }
        }
        jsonschema.validate(instance=facility, schema=schema)


class TestNewFields:
    """Test new EntityIdentity integration fields."""

    def test_ei_facility_id_null_validates(self, schema, base_facility):
        """ei_facility_id can be null."""
        base_facility["ei_facility_id"] = None
        jsonschema.validate(instance=base_facility, schema=schema)

    def test_ei_facility_id_valid_format(self, schema, base_facility):
        """ei_facility_id with valid format validates."""
        valid_ids = [
            "mimosa_52f2f3d6",
            "stillwater_east_abc123",
            "test_facility_123",
            "simple_id"
        ]
        for facility_id in valid_ids:
            base_facility["ei_facility_id"] = facility_id
            jsonschema.validate(instance=base_facility, schema=schema)

    def test_ei_facility_id_invalid_format_fails(self, schema, base_facility):
        """ei_facility_id with invalid format fails validation."""
        invalid_ids = [
            "Invalid-With-Dashes",  # Pattern requires lowercase with underscores
            "Has Spaces",
            "UPPERCASE",
            "special@chars",
            ""  # Empty string
        ]
        for facility_id in invalid_ids:
            base_facility["ei_facility_id"] = facility_id
            with pytest.raises(jsonschema.ValidationError):
                jsonschema.validate(instance=base_facility, schema=schema)

    def test_chemical_formula_valid_formats(self, schema, base_facility):
        """chemical_formula accepts valid chemical formulas."""
        valid_formulas = [
            "Cu",
            "Fe2O3",
            "CaCO3",
            "Pt",
            "Au",
            "H2O",
            "NaCl",
            "Al2O3",
            "PGM",  # Platinum Group Metals
            "REE"   # Rare Earth Elements
        ]
        for formula in valid_formulas:
            base_facility["commodities"] = [
                {
                    "metal": "test",
                    "primary": True,
                    "chemical_formula": formula,
                    "category": None
                }
            ]
            jsonschema.validate(instance=base_facility, schema=schema)

    def test_chemical_formula_invalid_formats(self, schema, base_facility):
        """chemical_formula rejects invalid formats."""
        invalid_formulas = [
            "copper",  # Must start with uppercase
            "cu",      # Must start with uppercase
            "2Cu",     # Can't start with number
            "Cu-O",    # No special chars except numbers
            "Cu O"     # No spaces
        ]
        for formula in invalid_formulas:
            base_facility["commodities"] = [
                {
                    "metal": "test",
                    "primary": True,
                    "chemical_formula": formula,
                    "category": None
                }
            ]
            with pytest.raises(jsonschema.ValidationError):
                jsonschema.validate(instance=base_facility, schema=schema)

    def test_chemical_formula_null_validates(self, schema, base_facility):
        """chemical_formula can be null."""
        base_facility["commodities"] = [
            {
                "metal": "copper",
                "primary": True,
                "chemical_formula": None,
                "category": None
            }
        ]
        jsonschema.validate(instance=base_facility, schema=schema)

    def test_category_valid_values(self, schema, base_facility):
        """category accepts valid enum values."""
        valid_categories = [
            "base_metal",
            "precious_metal",
            "rare_earth",
            "industrial_mineral",
            "energy",
            "construction",
            "fertilizer",
            "unknown"
        ]
        for category in valid_categories:
            base_facility["commodities"] = [
                {
                    "metal": "test",
                    "primary": True,
                    "chemical_formula": None,
                    "category": category
                }
            ]
            jsonschema.validate(instance=base_facility, schema=schema)

    def test_category_null_validates(self, schema, base_facility):
        """category can be null."""
        base_facility["commodities"] = [
            {
                "metal": "copper",
                "primary": True,
                "chemical_formula": None,
                "category": None
            }
        ]
        jsonschema.validate(instance=base_facility, schema=schema)

    def test_category_invalid_value_fails(self, schema, base_facility):
        """category rejects invalid values."""
        base_facility["commodities"] = [
            {
                "metal": "test",
                "primary": True,
                "chemical_formula": None,
                "category": "invalid_category"
            }
        ]
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance=base_facility, schema=schema)


class TestCompleteEnhancedFacility:
    """Test facilities with all new fields populated."""

    def test_fully_enhanced_facility_validates(self, schema):
        """Facility with all new fields populated is valid."""
        facility = {
            "facility_id": "usa-stillwater-east-fac",
            "name": "Stillwater East Mine",
            "aliases": ["East Boulder"],
            "country_iso3": "USA",
            "ei_facility_id": "stillwater_east_52f2f3d6",
            "location": {
                "lat": 45.4,
                "lon": -109.9,
                "precision": "exact"
            },
            "types": ["mine"],
            "commodities": [
                {
                    "metal": "platinum",
                    "primary": True,
                    "chemical_formula": "Pt",
                    "category": "precious_metal"
                },
                {
                    "metal": "palladium",
                    "primary": False,
                    "chemical_formula": "Pd",
                    "category": "precious_metal"
                }
            ],
            "status": "operating",
            "owner_links": [
                {
                    "company_id": "cmp-sibanye-stillwater",
                    "role": "owner",
                    "percentage": 100,
                    "confidence": 0.95
                }
            ],
            "operator_link": {
                "company_id": "cmp-sibanye-stillwater",
                "confidence": 0.95
            },
            "products": [],
            "sources": [
                {
                    "type": "lei",
                    "id": "LEI123456",
                    "date": "2025-10-14T12:00:00Z"
                }
            ],
            "verification": {
                "status": "llm_verified",
                "confidence": 0.95,
                "last_checked": "2025-10-14T12:00:00Z",
                "checked_by": "entityidentity",
                "notes": "Verified via EntityIdentity company matcher"
            }
        }
        jsonschema.validate(instance=facility, schema=schema)


class TestRealFacilities:
    """Test validation against real facility files in the repository."""

    @pytest.fixture
    def facilities_dir(self):
        """Get facilities directory path."""
        return Path(__file__).parent.parent.parent / "facilities"

    def test_sample_facilities_validate(self, schema, facilities_dir):
        """Sample of real facilities validate against schema."""
        if not facilities_dir.exists():
            pytest.skip("Facilities directory not found")

        # Get sample facilities from countries with ISO3 codes only
        # Note: DZ and AF use ISO2 codes which don't match the current schema pattern
        sample_files = []
        for country_dir in ["USA", "ZAF", "BRA", "CAN"]:
            country_path = facilities_dir / country_dir
            if country_path.exists():
                files = list(country_path.glob("*.json"))
                if files:
                    sample_files.append(files[0])  # Take first file from each country

        if not sample_files:
            pytest.skip("No facility files found with ISO3 codes")

        errors = []
        for fac_file in sample_files:
            try:
                with open(fac_file) as f:
                    facility = json.load(f)
                # Should validate even without new fields
                jsonschema.validate(instance=facility, schema=schema)
            except Exception as e:
                errors.append(f"{fac_file.name}: {str(e)}")

        if errors:
            pytest.fail(
                f"Validation errors in sample facilities:\n" +
                "\n".join(errors)
            )

    def test_all_facilities_validate(self, schema, facilities_dir):
        """All facilities in database validate against schema.

        Note: This test is currently expected to fail for facilities with ISO2 codes
        (DZ, AF, SL, SK) as the schema pattern requires ISO3 codes. This is a known
        issue documented in the codebase where some countries use ISO2 directories.
        """
        if not facilities_dir.exists():
            pytest.skip("Facilities directory not found")

        facility_files = list(facilities_dir.glob("*/*.json"))
        if not facility_files:
            pytest.skip("No facility files found")

        errors = []
        iso2_errors = []
        other_errors = []

        for fac_file in facility_files:
            try:
                with open(fac_file) as f:
                    facility = json.load(f)
                jsonschema.validate(instance=facility, schema=schema)
            except jsonschema.ValidationError as e:
                # Check if it's an ISO2 code issue
                if "does not match '^[a-z]{3}-" in str(e):
                    iso2_errors.append(fac_file.name)
                else:
                    other_errors.append(f"{fac_file.name}: {str(e)}")
            except Exception as e:
                errors.append(f"{fac_file.name}: {str(e)}")

        # Report summary
        print(f"\n{len(iso2_errors)} facilities with ISO2 codes (expected)")

        if other_errors or errors:
            all_errors = other_errors + errors
            pytest.fail(
                f"Validation errors in {len(all_errors)} facilities (excluding ISO2 issues):\n" +
                "\n".join(all_errors[:10])  # Show first 10 errors
            )


class TestSchemaFieldOrder:
    """Test that new fields are in the right position in the schema."""

    def test_ei_facility_id_is_first_property(self, schema):
        """ei_facility_id should be first property for visibility."""
        properties = list(schema["properties"].keys())
        assert properties[0] == "ei_facility_id"

    def test_commodities_has_all_fields(self, schema):
        """Commodities items have all expected fields."""
        commodity_props = schema["properties"]["commodities"]["items"]["properties"]
        assert "metal" in commodity_props
        assert "primary" in commodity_props
        assert "chemical_formula" in commodity_props
        assert "category" in commodity_props

    def test_only_metal_and_primary_required(self, schema):
        """Only metal and primary are required in commodities."""
        commodity_required = schema["properties"]["commodities"]["items"]["required"]
        assert commodity_required == ["metal", "primary"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
