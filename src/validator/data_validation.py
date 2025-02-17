from typing import List, Dict, Any
import polars as pl
from datetime import datetime
import re


class DataValidator:
    def __init__(self):
        self.validation_errors: List[Dict[str, Any]] = []

    def validate_data(self, df: pl.DataFrame, schema: Dict[str, Any]) -> bool:
        """
        Validates data against schema and business rules
        Returns True if validation passes, False otherwise
        """
        self.validation_errors = []

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
                col_data = df[col]
                pg_type = schema[col]

                # Null check
                null_count = col_data.null_count()
                if null_count > 0:
                    self.validation_errors.append(
                        {
                            "type": "NULL_VALUES",
                            "message": f"Column '{col}' contains {null_count} null values",
                            "column": col,
                        }
                    )

                # Type-specific validations
                if "TIMESTAMP" in pg_type:
                    self._validate_timestamps(col_data, col)
                elif "NUMERIC" in pg_type or "INTEGER" in pg_type:
                    self._validate_numbers(col_data, col)
                elif "EMAIL" in col.lower():
                    self._validate_emails(col_data, col)

        return len(self.validation_errors) == 0

    def _validate_timestamps(self, col_data: pl.Series, col_name: str):
        future_dates = col_data.filter(col_data > datetime.now())
        if len(future_dates) > 0:
            self.validation_errors.append(
                {
                    "type": "FUTURE_DATE",
                    "message": f"Column '{col_name}' contains {len(future_dates)} future dates",
                    "column": col_name,
                }
            )

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
