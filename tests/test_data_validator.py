import pytest
from src.validator.data_validation import DataValidator


def test_data_validation(sample_csv_data):
    validator = DataValidator()

    # Convert to Polars DataFrame
    import polars as pl

    df = pl.from_pandas(sample_csv_data)

    schema = {"id": "BIGINT", "email": "TEXT", "age": "BIGINT"}

    # Test valid data
    assert validator.validate_data(df, schema) is True
    assert len(validator.validation_errors) == 0

    # Test invalid email
    df_invalid = df.clone()
    df_invalid = df_invalid.with_columns(pl.lit("invalid-email").alias("email"))
    assert validator.validate_data(df_invalid, schema) is False
    assert any(
        "invalid email" in error["message"].lower()
        for error in validator.validation_errors
    )
