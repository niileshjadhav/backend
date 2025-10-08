"""
JSON Serialization Utilities
Handles proper serialization of datetime objects and other non-JSON-serializable types
"""

import json
from datetime import datetime, date
from typing import Any, Dict
import logging

logger = logging.getLogger(__name__)

def serialize_for_json(data: Any) -> Any:
    """
    Recursively serialize data structure to be JSON-compatible.
    Converts datetime objects to ISO format strings.
    
    Args:
        data: Any data structure that may contain datetime objects
        
    Returns:
        JSON-serializable version of the data
    """
    if data is None:
        return None
    
    if isinstance(data, datetime):
        return data.isoformat()
    
    if isinstance(data, date):
        return data.isoformat()
    
    if isinstance(data, dict):
        return {key: serialize_for_json(value) for key, value in data.items()}
    
    if isinstance(data, (list, tuple)):
        return [serialize_for_json(item) for item in data]
    
    if isinstance(data, (str, int, float, bool)):
        return data
    
    # For other types, try to convert to string
    try:
        return str(data)
    except Exception as e:
        logger.warning(f"Could not serialize object of type {type(data)}: {e}")
        return f"<non-serializable: {type(data).__name__}>"

def safe_json_serialize(data: Any) -> str:
    """
    Safely serialize data to JSON string with datetime handling.
    
    Args:
        data: Data to serialize
        
    Returns:
        JSON string representation
    """
    try:
        serializable_data = serialize_for_json(data)
        return json.dumps(serializable_data, indent=2)
    except Exception as e:
        logger.error(f"JSON serialization failed: {e}")
        return json.dumps({"error": "Serialization failed", "type": str(type(data))})

def prepare_filters_for_storage(filters: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prepare filters dictionary for storage in JSON column by serializing datetime objects.
    
    Args:
        filters: Dictionary that may contain datetime objects
        
    Returns:
        Dictionary with datetime objects converted to strings
    """
    if not filters:
        return {}
    
    return serialize_for_json(filters)