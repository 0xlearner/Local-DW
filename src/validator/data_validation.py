import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import polars as pl


def setup_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger


class DataValidator:
    def __init__(self):
        self.validation_errors: List[Dict[str, Any]] = []
        self.logger = setup_logger("validator")

    def validate_data(self, df: pl.DataFrame, schema: Dict[str, Any]) -> bool:
        """
        Validates data against schema and business rules
        Returns True if validation passes, False otherwise
        """
        self.validation_errors = []

        # Check for required columns
        self._validate_required_columns(df, schema)

        # Validate data types and constraints for each column
        for col in df.columns:
            if col in schema:
                self._validate_column(df, col, schema[col])

        return len(self.validation_errors) == 0

    def _validate_required_columns(
        self, df: pl.DataFrame, schema: Dict[str, Any]
    ) -> None:
        """Check if all required columns are present"""
        for col in schema:
            if col not in df.columns:
                self.validation_errors.append(
                    {
                        "type": "MISSING_COLUMN",
                        "message": f"Required column '{col}' is missing",
                        "column": col,
                    }
                )

    def _validate_column(self, df: pl.DataFrame, col: str, pg_type: str) -> None:
        """Validate a single column based on its PostgreSQL type"""
        if "TIMESTAMP" in pg_type.upper():
            self._validate_timestamp_column(df, col)
        elif "JSONB" in pg_type.upper() or "JSON" in pg_type.upper():
            self._validate_json_column(df, col)
        elif "[]" in pg_type:
            self._validate_array_column(df, col)

    def _validate_timestamp_column(self, df: pl.DataFrame, col: str) -> None:
        """Validate timestamp column with multiple format attempts"""
        self.logger.debug(f"Validating timestamp column: {col}")
        self.logger.debug(f"Sample values: {df[col].head(5)}")

        try:
            df_with_parsed = self._try_parse_timestamp(df, col)
            if df_with_parsed is not None:
                self._validate_timestamps(df_with_parsed[f"{col}_parsed"], col)
            else:
                self.validation_errors.append(
                    {
                        "type": "INVALID_TIMESTAMP",
                        "message": f"Column '{col}' contains invalid timestamp format",
                        "column": col,
                    }
                )
        except Exception as e:
            self.logger.error(f"Error validating timestamp column {col}: {str(e)}")
            self.validation_errors.append(
                {
                    "type": "INVALID_TIMESTAMP",
                    "message": f"Column '{col}' validation error: {str(e)}",
                    "column": col,
                }
            )

    def _try_parse_timestamp(
        self, df: pl.DataFrame, col: str
    ) -> Optional[pl.DataFrame]:
        """Try parsing timestamp with different formats"""
        formats = [
            ("%Y-%m-%dT%H:%M:%S.%f", "ISO format"),
            ("%Y-%m-%d %H:%M:%S", "Standard format"),
            ("%Y-%m-%d", "Date only format"),
        ]

        for fmt, desc in formats:
            try:
                df_parsed = df.with_columns(
                    pl.col(col).str.strptime(pl.Datetime, fmt).alias(f"{col}_parsed")
                )
                self.logger.debug(f"Successfully parsed {col} using {desc}")
                return df_parsed
            except Exception as e:
                self.logger.debug(f"Failed to parse with {desc}: {str(e)}")
                continue

        return None

    def _validate_json_column(self, df: pl.DataFrame, col: str) -> None:
        """Validate JSON column"""
        try:
            df_with_json = df.with_columns(
                pl.col(col).map_elements(lambda x: self._validate_json(x, col))
            )
            if df_with_json[col].null_count() > 0:
                self.validation_errors.append(
                    {
                        "type": "INVALID_JSON",
                        "message": f"Column '{col}' contains invalid JSON",
                        "column": col,
                    }
                )
        except Exception as e:
            self.logger.error(f"Error validating JSON column {col}: {str(e)}")
            self.validation_errors.append(
                {
                    "type": "INVALID_JSON",
                    "message": f"Column '{col}' validation error: {str(e)}",
                    "column": col,
                }
            )

    def _validate_array_column(self, df: pl.DataFrame, col: str) -> None:
        """Validate array column"""
        try:
            df_with_array = df.with_columns(
                pl.col(col).map_elements(lambda x: self._validate_array(x, col))
            )
            if df_with_array[col].null_count() > 0:
                self.validation_errors.append(
                    {
                        "type": "INVALID_ARRAY",
                        "message": f"Column '{col}' contains invalid array format",
                        "column": col,
                    }
                )
        except Exception as e:
            self.logger.error(f"Error validating array column {col}: {str(e)}")
            self.validation_errors.append(
                {
                    "type": "INVALID_ARRAY",
                    "message": f"Column '{col}' validation error: {str(e)}",
                    "column": col,
                }
            )

    def _validate_timestamps(self, col_data: pl.Series, col: str):
        logger = setup_logger("validator")
        try:
            # Get current time as Polars datetime
            current_time = datetime.now()

            # Count future dates
            future_count = col_data.filter(col_data > current_time).len()

            if future_count > 0:
                logger.debug(f"Found {future_count} future dates in {col}")
                self.validation_errors.append(
                    {
                        "type": "FUTURE_DATE",
                        "message": f"Column '{col}' contains {future_count} future dates",
                        "column": col,
                    }
                )
        except Exception as e:
            logger.error(f"Error in timestamp validation for {col}: {str(e)}")
            raise

    def _validate_numbers(self, col_data: pl.Series, col_name: str):
        negative_values = col_data.filter(col_data < 0)
        if len(negative_values) > 0:
            self.validation_errors.append(
                {
                    "type": "NEGATIVE_VALUES",
                    "message": f"Column '{col_name}' contains {len(negative_values)} negative values",
                    "column": col_name,
                }
            )

    def _validate_emails(self, col_data: pl.Series, col_name: str):
        email_pattern = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
        invalid_emails = col_data.filter(~col_data.str.contains(email_pattern))
        if len(invalid_emails) > 0:
            self.validation_errors.append(
                {
                    "type": "INVALID_EMAIL",
                    "message": f"Column '{col_name}' contains {len(invalid_emails)} invalid email addresses",
                    "column": col_name,
                }
            )

    def _validate_json(self, value: str, col: str) -> str:
        """Validate and format JSON string"""
        import json

        try:
            if isinstance(value, str):
                # Parse and re-serialize to ensure valid JSON format
                return json.dumps(json.loads(value))
            return None
        except Exception:
            return None

    def _validate_array(self, value: str, col: str) -> str:
        """Validate and convert array notation from various formats"""
        try:
            if isinstance(value, str):
                # Handle Python list string format (e.g. "['email', 'phone']")
                if value.startswith("[") and value.endswith("]"):
                    # Convert Python list string to PostgreSQL array format
                    try:
                        # Safely evaluate the string as a Python literal
                        import ast

                        python_list = ast.literal_eval(value)
                        # Convert to PostgreSQL array format
                        pg_array = (
                            "{" + ",".join(str(item) for item in python_list) + "}"
                        )
                        return pg_array
                    except (ValueError, SyntaxError):
                        return None

                # Handle PostgreSQL array format (e.g. "{email,phone}")
                if value.startswith("{") and value.endswith("}"):
                    return value

                # Handle comma-separated string (e.g. "email,phone")
                if "," in value:
                    items = [item.strip() for item in value.split(",")]
                    return "{" + ",".join(items) + "}"

                # Handle single value
                if value.strip():
                    return "{" + value.strip() + "}"

            return None
        except Exception:
            return None
