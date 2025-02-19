from datetime import datetime
from typing import Dict, Any


class RecordSerializer:
    @staticmethod
    def serialize_record(record: Dict[str, Any]) -> Dict[str, Any]:
        """Convert record values to JSON-serializable format"""
        if not record:
            return {}

        serialized = {}
        for key, value in record.items():
            if isinstance(value, datetime):
                serialized[key] = value.isoformat()
            else:
                serialized[key] = value
        return serialized
