import pytest
from dbt_column_lineage_extractor import DbtColumnLineageExtractor

def test_extractor_initialization():
    """Test that the extractor can be initialized with valid parameters."""
    extractor = DbtColumnLineageExtractor(
        manifest_path="tests/test_data/inputs/manifest.json",
        catalog_path="tests/test_data/inputs/catalog.json",
        selected_models=[],
        dialect="snowflake"
    )
    assert isinstance(extractor, DbtColumnLineageExtractor)
    assert extractor.dialect == "snowflake"
    # When selected_models is empty, it automatically selects all models from manifest
    assert len(extractor.selected_models) > 0
    assert all(model.startswith("model.") for model in extractor.selected_models)

def test_extractor_with_specific_models():
    """Test that the extractor can be initialized with specific models."""
    specific_models = ["model.jaffle_shop.customers"]
    extractor = DbtColumnLineageExtractor(
        manifest_path="tests/test_data/inputs/manifest.json",
        catalog_path="tests/test_data/inputs/catalog.json",
        selected_models=specific_models,
        dialect="snowflake"
    )
    assert extractor.selected_models == specific_models

def test_dialect_support():
    """Test that the extractor supports different dialects."""
    # Test with supported dialects
    for dialect in ["snowflake", "postgres", "bigquery", "mysql"]:
        extractor = DbtColumnLineageExtractor(
            manifest_path="tests/test_data/inputs/manifest.json",
            catalog_path="tests/test_data/inputs/catalog.json",
            selected_models=[],
            dialect=dialect
        )
        assert extractor.dialect == dialect
