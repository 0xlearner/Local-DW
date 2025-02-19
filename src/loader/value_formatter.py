from datetime import datetime
import json
from typing import Any, Dict
from src.logger import setup_logger


class ValueFormatter:
    def __init__(self):
        self.logger = setup_logger("value_formatter")

    def format_value(self, value: Any, col_name: str, schema: Dict[str, str]) -> Any:
        """Format value according to column type"""
        if value is None:
            return None

        pg_type = schema[col_name].upper() if col_name in schema else "TEXT"

        formatters = {
            "TIMESTAMP": self._format_timestamp,
            "DATE": self._format_timestamp,
            "TIME": self._format_timestamp,
            "JSON": self._format_json,
            "JSONB": self._format_json
        }

        for type_key, formatter in formatters.items():
            if type_key in pg_type:
                return formatter(value, col_name)

        return value

    def _format_timestamp(self, value: Any, col_name: str) -> datetime:
        if isinstance(value, datetime):
            return value

        if isinstance(value, str):
            try:
                if value.endswith("Z"):
                    value = value[:-1] + "+00:00"

                try:
                    return datetime.fromisoformat(value)
                except ValueError:
                    pass

                formats = [
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d",
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S.%f",
                ]

                for fmt in formats:
                    try:
                        return datetime.strptime(value, fmt)
                    except ValueError:
                        continue

                if "+" in value:
                    parts = value.split("+")
                    base_dt = parts[0]
                    return datetime.fromisoformat(base_dt)

                raise ValueError(f"Could not parse timestamp: {value}")

            except ValueError as e:
                self.logger.error(f"Error parsing timestamp for {
                                  col_name}: {value} - {str(e)}")
                raise

        raise ValueError(f"Unexpected type for timestamp value: {type(value)}")

    def _format_json(self, value: Any, _: str) -> str:
        if isinstance(value, str):
            try:
                json.loads(value)
                return value
            except json.JSONDecodeError:
                return json.dumps(value)
        return json.dumps(value)

    def get_type_cast(self, col: str, schema: Dict[str, str]) -> str:
        """Get PostgreSQL type cast for a column"""
        if col not in schema:
            return ""

        pg_type = schema[col].upper()
        type_casts = {
            "TIMESTAMP": "::timestamp",
            "JSON": "::jsonb",
            "JSONB": "::jsonb",
            "ARRAY": "::text[]",
            "NUMERIC": "::numeric",
            "INTEGER": "::bigint",
            "BIGINT": "::bigint",
            "BOOLEAN": "::boolean"
        }

        for type_key, cast in type_casts.items():
            if type_key in pg_type:
                return cast

        return ""  # Default to no type cast for TEXT and other types
