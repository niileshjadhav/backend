"""
Simplified MCP Server Module
Provides database operations without full MCP protocol overhead
"""

from .server import (
    query_logs,
    archive_records,
    delete_archived_records,
    get_table_stats,
    health_check,
    activities_schema,
    transaction_schema
)

__all__ = [
    "query_logs",
    "archive_records", 
    "delete_archived_records",
    "get_table_stats",
    "health_check",
    "activities_schema",
    "transaction_schema"
]