"""
Direct Database Service - Alternative to MCP
This service provides direct database operations without MCP protocol overhead.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func

from models.activities import ArchiveDSIActivities, DSIActivities
from models.transactions import ArchiveDSITransactionLog, DSITransactionLog
from .crud_service import CRUDService

logger = logging.getLogger(__name__)

def setup_database_logging(name: str = __name__) -> logging.Logger:
    """Setup consistent logging for database operations"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s:%(name)s:%(message)s'
    )
    return logging.getLogger(name)

# Common table definitions
REQUIRED_TABLES = [
    'dsiactivities',
    'dsiactivities_archive', 
    'dsitransactionlog',
    'dsitransactionlog_archive'
]

class DatabaseService:
    """Direct database operations service - replaces MCP tools"""
    
    def __init__(self, db_session: Session):
        self.db = db_session
        self.crud_service = CRUDService(db_session)
        
        # Configuration constants
        self.MAX_RECORDS_PER_QUERY = 10000
        self.DEFAULT_QUERY_LIMIT = None  # No default limit
        self.VALID_TABLE_NAMES = ["dsiactivities", "dsitransactionlog", "dsiactivities_archive", "dsitransactionlog_archive"]
        self.ARCHIVE_TABLE_NAMES = ["dsiactivities_archive", "dsitransactionlog_archive"]
    
    def validate_table_name(self, table_name: str) -> bool:
        """Validate if table name is supported"""
        return table_name in self.VALID_TABLE_NAMES
    
    async def query_logs(
        self,
        table_name: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = None,
        offset: int = 0
    ) -> Dict[str, Any]:
        """Query log records with optional filters - replaces MCP query_logs tool"""
        try:
            # Validate inputs
            if not self.validate_table_name(table_name):
                return {
                    "success": False,
                    "error": f"Invalid table name: {table_name}",
                    "records": []
                }
            
            # Set default limit
            if limit is None:
                limit = self.DEFAULT_QUERY_LIMIT
            
            # Enforce max limit
            if limit > self.MAX_RECORDS_PER_QUERY:
                logger.warning(f"Limiting query to {self.MAX_RECORDS_PER_QUERY} records")
                limit = self.MAX_RECORDS_PER_QUERY
            
            # Execute query
            result = self.crud_service.select_records(
                db=self.db,
                table_name=table_name,
                filters=filters or {},
                limit=limit,
                offset=offset
            )
            
            return {
                "success": True,
                "table": table_name,
                "records": result.get("records", []),
                "total_records": result.get("total_records", 0),
                "filters_applied": filters or {},
                "limit": limit,
                "offset": offset,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error querying logs: {e}")
            return {
                "success": False,
                "error": f"Database operation failed: {str(e)}",
                "records": []
            }
    
    async def get_table_stats(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """Get table statistics and counts - replaces MCP get_table_stats tool"""
        try:
            stats = {}
            
            tables = self.VALID_TABLE_NAMES if table_name is None else [table_name]
            
            for table in tables:
                if not self.validate_table_name(table):
                    logger.warning(f"Skipping invalid table: {table}")
                    continue
                
                try:
                    if table == 'dsiactivities':
                        # Get main table count
                        try:
                            main_count = self.db.query(func.count(DSIActivities.SequenceID)).scalar()
                        except Exception as e:
                            logger.warning(f"Main table {table} error: {e}")
                            main_count = 0
                        
                        # Get archive table count with graceful fallback
                        try:
                            archive_count = self.db.query(func.count(ArchiveDSIActivities.SequenceID)).scalar()
                        except Exception as e:
                            logger.warning(f"Archive table {table}_archive not found or error: {e}")
                            archive_count = 0
                            
                    elif table == 'dsitransactionlog':
                        # Get main table count
                        try:
                            main_count = self.db.query(func.count(DSITransactionLog.RecordID)).scalar()
                        except Exception as e:
                            logger.warning(f"Main table {table} error: {e}")
                            main_count = 0
                        
                        # Get archive table count with graceful fallback
                        try:
                            archive_count = self.db.query(func.count(ArchiveDSITransactionLog.RecordID)).scalar()
                        except Exception as e:
                            logger.warning(f"Archive table {table}_archive not found or error: {e}")
                            archive_count = 0
                    else:
                        continue
                    
                    stats[table] = {
                        "main_table_count": main_count or 0,
                        "archive_table_count": archive_count or 0,
                        "total_count": (main_count or 0) + (archive_count or 0),
                        "archive_table_exists": archive_count is not None
                    }
                    
                except Exception as e:
                    logger.error(f"Error getting stats for table {table}: {e}")
                    stats[table] = {
                        "main_table_count": 0,
                        "archive_table_count": 0,
                        "total_count": 0,
                        "error": f"Failed to get stats: {str(e)}",
                        "archive_table_exists": False
                    }
            
            return {
                "success": True,
                "stats": stats,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting table stats: {e}")
            return {
                "success": False,
                "error": f"Statistics operation failed: {str(e)}",
                "stats": {}
            }

    async def get_detailed_table_stats(self) -> Dict[str, Any]:
        """Get detailed table statistics with age-based counts for all 4 tables"""
        try:
            from sqlalchemy import text
            from datetime import datetime, timedelta
            
            # Calculate cutoff dates
            seven_days_ago = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d%H%M%S")
            thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d%H%M%S")
            
            detailed_stats = {}
            
            # Main tables statistics
            main_tables = [
                {
                    'name': 'dsiactivities',
                    'display_name': 'DSI Activities',
                    'model': DSIActivities,
                    'time_column': 'PostedTime'
                },
                {
                    'name': 'dsitransactionlog', 
                    'display_name': 'DSI Transaction Log',
                    'model': DSITransactionLog,
                    'time_column': 'WhenReceived'
                }
            ]
            
            # Archive tables statistics
            archive_tables = [
                {
                    'name': 'dsiactivities_archive',
                    'display_name': 'DSI Activities Archive',
                    'model': ArchiveDSIActivities,
                    'time_column': 'PostedTime'
                },
                {
                    'name': 'dsitransactionlog_archive',
                    'display_name': 'DSI Transaction Log Archive', 
                    'model': ArchiveDSITransactionLog,
                    'time_column': 'WhenReceived'
                }
            ]
            
            # Process main tables
            for table_info in main_tables:
                try:
                    table_name = table_info['name']
                    model = table_info['model']
                    time_column = table_info['time_column']
                    
                    # Total count - use .count() to count all rows including those with NULL IDs
                    total_count = self.db.query(model).count()
                    
                    # Count older than 7 days
                    older_than_7_days = 0
                    if total_count > 0:
                        try:
                            time_col = getattr(model, time_column)
                            # String date columns - use string comparison
                            older_than_7_days = self.db.query(func.count(time_col)).filter(
                                time_col < seven_days_ago
                            ).scalar() or 0
                        except Exception as e:
                            logger.warning(f"Could not calculate age-based count for {table_name}: {e}")
                            older_than_7_days = 0
                    
                    detailed_stats[table_name] = {
                        'display_name': table_info['display_name'],
                        'type': 'main',
                        'total_count': total_count,
                        'older_than_days': 7,
                        'older_count': older_than_7_days
                    }
                    
                except Exception as e:
                    logger.error(f"Error getting stats for main table {table_info['name']}: {e}")
                    detailed_stats[table_info['name']] = {
                        'display_name': table_info['display_name'],
                        'type': 'main',
                        'total_count': 0,
                        'older_than_days': 7,
                        'older_count': 0,
                        'error': str(e)
                    }
            
            # Process archive tables
            for table_info in archive_tables:
                try:
                    table_name = table_info['name']
                    model = table_info['model']
                    time_column = table_info['time_column']
                    
                    # Total count - use .count() to count all rows including those with NULL IDs
                    total_count = self.db.query(model).count()
                    
                    # Count older than 30 days
                    older_than_30_days = 0
                    if total_count > 0:
                        try:
                            time_col = getattr(model, time_column)
                            # Archive tables use string date format
                            older_than_30_days = self.db.query(func.count(time_col)).filter(
                                time_col < thirty_days_ago
                            ).scalar() or 0
                        except Exception as e:
                            logger.warning(f"Could not calculate age-based count for {table_name}: {e}")
                            older_than_30_days = 0
                    
                    detailed_stats[table_name] = {
                        'display_name': table_info['display_name'],
                        'type': 'archive',
                        'total_count': total_count,
                        'older_than_days': 30,
                        'older_count': older_than_30_days
                    }
                    
                except Exception as e:
                    logger.error(f"Error getting stats for archive table {table_info['name']}: {e}")
                    detailed_stats[table_info['name']] = {
                        'display_name': table_info['display_name'],
                        'type': 'archive',
                        'total_count': 0,
                        'older_than_days': 30,
                        'older_count': 0,
                        'error': str(e)
                    }
            
            return {
                "success": True,
                "detailed_stats": detailed_stats,
                "generated_at": datetime.now().isoformat(),
                "cutoff_dates": {
                    "seven_days_ago": seven_days_ago,
                    "thirty_days_ago": thirty_days_ago
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting detailed table stats: {e}")
            return {
                "success": False,
                "error": f"Detailed statistics operation failed: {str(e)}",
                "detailed_stats": {}
            }
    
    async def archive_records(
        self,
        table_name: str,
        filters: Dict[str, Any],
        dry_run: bool = True
    ) -> Dict[str, Any]:
        """Archive records from main table to archive table - replaces MCP archive_records tool"""
        try:
            # Validate inputs
            if not self.validate_table_name(table_name):
                return {
                    "success": False,
                    "error": f"Invalid table name: {table_name}",
                    "records_processed": 0
                }
            
            # SAFETY RULE: Apply default 7-day filter if no date filters provided
            if not filters:
                from datetime import datetime, timedelta
                cutoff_date = datetime.now() - timedelta(days=7)
                filters = {
                    "date_end": cutoff_date.strftime("%Y%m%d%H%M%S"),
                    "date_comparison": "older_than"
                }
            
            if dry_run:
                # Count records that would be archived
                result = self.crud_service.select_records(
                    db=self.db,
                    table_name=table_name,
                    filters=filters,
                    limit=None  # Count all matching records
                )
                
                return {
                    "success": True,
                    "operation": "archive_preview",
                    "table": table_name,
                    "records_to_archive": result.get("total_records", 0),
                    "filters_applied": filters,
                    "dry_run": True,
                    "timestamp": datetime.now().isoformat()
                }
            else:
                # Perform actual archiving
                result = self.crud_service.archive_records(
                    db=self.db,
                    table_name=table_name,
                    filters=filters
                )
                
                return {
                    "success": result.get("success", False),
                    "operation": "archive",
                    "table": table_name,
                    "records_processed": result.get("records_processed", 0),
                    "filters_applied": filters,
                    "dry_run": False,
                    "timestamp": datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"Error archiving records: {e}")
            return {
                "success": False,
                "error": f"Archive operation failed: {str(e)}",
                "records_processed": 0
            }
    
    async def delete_archived_records(
        self,
        table_name: str,
        filters: Dict[str, Any],
        dry_run: bool = True,
        safety_checks: bool = True
    ) -> Dict[str, Any]:
        """Delete records from archive table - replaces MCP delete_archived_records tool"""
        try:
            # Validate inputs
            if not self.validate_table_name(table_name):
                return {
                    "success": False,
                    "error": f"Invalid table name: {table_name}",
                    "records_processed": 0
                }
            
            # SAFETY RULE: Apply default 30-day filter if no date filters provided
            if not filters:
                from datetime import datetime, timedelta
                cutoff_date = datetime.now() - timedelta(days=30)
                filters = {
                    "date_end": cutoff_date.strftime("%Y%m%d%H%M%S"),
                    "date_comparison": "older_than"
                }
            
            # Safety check for same-day deletion
            if safety_checks:
                today = datetime.now().strftime("%Y%m%d")
                for key, value in filters.items():
                    if "time" in key.lower() and isinstance(value, str):
                        if value.startswith(today):
                            return {
                                "success": False,
                                "error": "Safety check failed: Cannot delete records from today",
                                "records_processed": 0
                            }
                
                # Safety check for 30-day minimum age for delete operations
                min_delete_date = datetime.now() - timedelta(days=30)
                for key, value in filters.items():
                    if "time" in key.lower() and isinstance(value, str) and len(value) >= 8:
                        try:
                            filter_date = datetime.strptime(value[:8], "%Y%m%d")
                            if filter_date > min_delete_date:
                                return {
                                    "success": False,
                                    "error": f"Safety check failed: Can only delete archived records older than 30 days. Current cutoff date {filter_date.strftime('%Y-%m-%d')} is too recent. Minimum allowed date: {min_delete_date.strftime('%Y-%m-%d')}",
                                    "records_processed": 0
                                }
                        except ValueError:
                            pass  # Invalid date format, skip validation
            
            # Get archive table name
            archive_table = f"{table_name}_archive"
            
            if dry_run:
                # Count records that would be deleted
                result = self.crud_service.select_records(
                    db=self.db,
                    table_name=archive_table,
                    filters=filters,
                    limit=None
                )
                
                return {
                    "success": True,
                    "operation": "delete_preview",
                    "table": archive_table,
                    "records_to_delete": result.get("total_records", 0),
                    "filters_applied": filters,
                    "dry_run": True,
                    "timestamp": datetime.now().isoformat()
                }
            else:
                # Perform actual deletion
                result = self.crud_service.delete_records(
                    db=self.db,
                    table_name=archive_table,
                    filters=filters
                )
                
                return {
                    "success": result.get("success", False),
                    "operation": "delete",
                    "table": archive_table,
                    "records_processed": result.get("records_processed", 0),
                    "filters_applied": filters,
                    "dry_run": False,
                    "timestamp": datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"Error deleting archived records: {e}")
            return {
                "success": False,
                "error": f"Delete operation failed: {str(e)}",
                "records_processed": 0
            }
    
    def validate_filters(self, table_name: str, filters: Dict[str, Any]) -> Dict[str, Any]:
        """Validate filters for a specific table - replaces MCP validate_filters tool"""
        try:
            if not self.validate_table_name(table_name):
                return {
                    "success": False,
                    "error": f"Invalid table name: {table_name}",
                    "valid_filters": {},
                    "invalid_filters": filters
                }
            
            validation_result = {
                "success": True,
                "table": table_name,
                "valid_filters": {},
                "invalid_filters": {},
                "warnings": [],
                "timestamp": datetime.now().isoformat()
            }
            
            # Define valid columns for each table
            valid_columns = {
                'dsiactivities': [
                    'SequenceID', 'ActivityID', 'ActivityType', 'TrackingID', 
                    'SecondaryTrackingID', 'AgentName', 'ThreadID', 'Description',
                    'PostedTime', 'PostedTimeUTC', 'LineNumber', 'FileName',
                    'MethodName', 'ServerName', 'InstanceID', 'IdenticalAlertCount',
                    'AlertLevel', 'DismissedBy', 'DismissedDateTime',
                    'LastIdenticalAlertDateTime', 'EventID', 'DefaultDescription',
                    'ExceptionMessage'
                ],
                'dsitransactionlog': [
                    'RecordID', 'RecordStatus', 'ProcessMethod', 'TransactionType',
                    'ServerName', 'DeviceID', 'UserID', 'DeviceLocalTime',
                    'DeviceUTCTime', 'DeviceSequenceID', 'WhenReceived',
                    'WhenProcessed', 'WhenExtracted', 'ElapsedTime', 'AppID',
                    'AppVersion', 'AppItemID', 'WorldHostID', 'ConnectorID',
                    'FunctionDefVersion', 'FunctionCallID'
                ]
            }
            
            table_columns = valid_columns.get(table_name, [])
            
            for filter_key, filter_value in filters.items():
                # Handle range filters (e.g., posted_time_start, posted_time_end)
                base_key = filter_key.replace('_start', '').replace('_end', '')
                base_key = base_key.replace('_from', '').replace('_to', '')
                
                # Convert to proper case (e.g., posted_time -> PostedTime)
                proper_key = ''.join(word.capitalize() for word in base_key.split('_'))
                
                if proper_key in table_columns:
                    validation_result["valid_filters"][filter_key] = filter_value
                    
                    # Add warnings for date formats
                    if "time" in filter_key.lower() and isinstance(filter_value, str):
                        if len(filter_value) != 14 or not filter_value.isdigit():
                            validation_result["warnings"].append(
                                f"Date filter '{filter_key}' should be in YYYYMMDDHHMMSS format"
                            )
                else:
                    validation_result["invalid_filters"][filter_key] = filter_value
            
            if validation_result["invalid_filters"]:
                validation_result["success"] = False
            
            return validation_result
            
        except Exception as e:
            logger.error(f"Error validating filters: {e}")
            return {
                "success": False,
                "error": f"Validation failed: {str(e)}",
                "valid_filters": {},
                "invalid_filters": filters
            }
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get table schema information - replaces MCP resources"""
        schemas = {
            'dsiactivities': {
                "table_name": "DSI Activities",
                "description": "Activity logs and events",
                "columns": [
                    {"name": "SequenceID", "type": "Integer", "description": "Primary Key, Auto-increment"},
                    {"name": "ActivityID", "type": "String(50)", "description": "Activity identifier"},
                    {"name": "ActivityType", "type": "String(50)", "description": "Type of activity"},
                    {"name": "TrackingID", "type": "String(50)", "description": "Tracking identifier"},
                    {"name": "AgentName", "type": "String(50)", "description": "Agent performing activity"},
                    {"name": "PostedTime", "type": "String(14)", "description": "Posted time (YYYYMMDDHHMMSS)"},
                    {"name": "Description", "type": "String(2000)", "description": "Activity description"},
                    {"name": "ServerName", "type": "String(64)", "description": "Server name"}
                ]
            },
            'dsitransactionlog': {
                "table_name": "DSI Transaction Log",
                "description": "Transaction processing logs",
                "columns": [
                    {"name": "RecordID", "type": "Integer", "description": "Primary Key, Auto-increment"},
                    {"name": "ServerName", "type": "String(63)", "description": "Server name"},
                    {"name": "DeviceID", "type": "String(64)", "description": "Device identifier"},
                    {"name": "UserID", "type": "String(64)", "description": "User identifier"},
                    {"name": "WhenReceived", "type": "String(14)", "description": "Received time (YYYYMMDDHHMMSS)"},
                    {"name": "WhenProcessed", "type": "String(14)", "description": "Processed time (YYYYMMDDHHMMSS)"}
                ]
            }
        }
        
        if table_name in schemas:
            return {
                "success": True,
                "schema": schemas[table_name],
                "timestamp": datetime.now().isoformat()
            }
        else:
            return {
                "success": False,
                "error": f"Schema not found for table: {table_name}",
                "available_tables": list(schemas.keys())
            }