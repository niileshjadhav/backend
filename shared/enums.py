"""Enum definitions for consistent type checking"""
from enum import Enum


class TableName(Enum):
    """Supported table names"""
    DSI_ACTIVITIES = "dsiactivities"
    DSI_TRANSACTION_LOG = "dsitransactionlog"
    DSI_ACTIVITIES_ARCHIVE = "dsiactivitiesarchive"
    DSI_TRANSACTION_LOG_ARCHIVE = "dsitransactionlogarchive"
    
    @classmethod
    def get_valid_names(cls) -> list[str]:
        """Get list of valid table names"""
        return [table.value for table in cls]
    
    @classmethod
    def is_valid(cls, name: str) -> bool:
        """Check if table name is valid"""
        return name in cls.get_valid_names()