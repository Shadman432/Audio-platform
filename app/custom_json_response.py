# app/custom_json_response.py
import json
from decimal import Decimal
from fastapi.responses import JSONResponse
from typing import Any
import uuid
from datetime import datetime, date

class CustomJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        return json.dumps(
            content,
            ensure_ascii=False,
            allow_nan=False,
            indent=None,
            separators=(",", ":"),
            default=self.default_serializer,
        ).encode("utf-8")

    def default_serializer(self, obj: Any) -> Any:
        """Handle non-JSON serializable objects"""
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, uuid.UUID):
            return str(obj)
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        if hasattr(obj, '__dict__'):
            return obj.__dict__
        raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")