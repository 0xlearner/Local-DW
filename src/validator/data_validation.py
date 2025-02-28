import logging
import re
from datetime import datetime
from typing import Any, Dict, List

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

    def validate_data(self, df: pl.DataFrame, schema: Dict[str, Any]) -> bool:
        """
        Validates data against schema and business rules
        Returns True if validation passes, False otherwise
        """
        self.validation_errors = []
        logger = setup_logger("validator")

        # Check for required columns
        for col, dtype in schema.items():
            if col not in df.columns:
                self.validation_errors.append(
                    {
                        "type": "MISSING_COLUMN",
                        "message": f"Required column '{col}' is missing",
                        "column": col,
                    }
                )

        # Validate data types and constraints
        for col in df.columns:
            if col in schema:
                pg_type = schema[col]

                # Type-specific validations
                if "TIMESTAMP" in pg_type.upper():
                    logger.debug(f"Validating timestamp column: {col}")
                    logger.debug(f"Sample values: {df[col].head(5)}")

                    try:
                        # Try parsing ISO format timestamps
                        df_with_parsed = df.with_columns(
                            pl.col(col)
                            .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S.%f")
                            .alias(f"{col}_parsed")
                        )
                        logger.debug(f"Successfully parsed {col} using ISO format")
                        self._validate_timestamps(df_with_parsed[f"{col}_parsed"], col)
                    except Exception as e1:
                        logger.debug(f"Failed to parse with ISO format: {str(e1)}")
                        try:
                            # Try other common formats as fallback
                            for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
                                try:
                                    df_with_parsed = df.with_columns(
                                        pl.col(col)
                                        .str.strptime(pl.Datetime, fmt)
                                        .alias(f"{col}_parsed")
                                    )
                                    logger.debug(
                                        f"Successfully parsed {col} using format: {fmt}"
                                    )
                                    self._validate_timestamps(
                                        df_with_parsed[f"{col}_parsed"], col
                                    )
                                    break
                                except Exception as e2:
                                    logger.debug(
                                        f"Failed to parse with format {fmt}: {str(e2)}"
                                    )
                                    continue
                            else:
                                self.validation_errors.append(
                                    {
                                        "type": "INVALID_TIMESTAMP",
                                        "message": f"Column '{col}' contains invalid timestamp format",
                                        "column": col,
                                    }
                                )
                        except Exception as e:
                            logger.error(
                                f"Error validating timestamp column {col}: {str(e)}"
                            )
                            self.validation_errors.append(
                                {
                                    "type": "INVALID_TIMESTAMP",
                                    "message": f"Column '{col}' validation error: {str(e)}",
                                    "column": col,
                                }
                            )
                elif "JSONB" in pg_type.upper() or "JSON" in pg_type.upper():
                    try:
                        # Ensure the string is valid JSON
                        df_with_json = df.with_columns(
                            pl.col(col).map_elements(
                                lambda x: self._validate_json(x, col)
                            )
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
                        logger.error(f"Error validating JSON column {col}: {str(e)}")
                        self.validation_errors.append(
                            {
                                "type": "INVALID_JSON",
                                "message": f"Column '{col}' validation error: {str(e)}",
                                "column": col,
                            }
                        )
                elif (
                    "[]" in pg_type
                ):  # Only validate as array if explicitly defined as array type
                    try:
                        # Convert set notation to array notation
                        df_with_array = df.with_columns(
                            pl.col(col).map_elements(
                                lambda x: self._validate_array(x, col)
                            )
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
                        logger.error(f"Error validating array column {col}: {str(e)}")
                        self.validation_errors.append(
                            {
                                "type": "INVALID_ARRAY",
                                "message": f"Column '{col}' validation error: {str(e)}",
                                "column": col,
                            }
                        )

        return len(self.validation_errors) == 0

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
