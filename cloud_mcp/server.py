"""
MCP server implementation using fastmcp
Provides database operation tools via the Model Context Protocol
"""

import logging
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text, func
from fastmcp import FastMCP
from database import get_db
from services.crud_service import CRUDService
from models.activities import DSIActivities, ArchiveDSIActivities
from models.transactions import DSITransactionLog, ArchiveDSITransactionLog
from datetime import datetime

def format_database_date(date_str: str) -> str:
    """Convert database date string (YYYYMMDDHHMMSS) to readable format"""
    if not date_str:
        return None
    
    try:
        # Handle different string formats
        date_str = str(date_str).strip()
        
        # If it's already a datetime object, convert to string first
        if hasattr(date_str, 'strftime'):
            date_str = date_str.strftime('%Y%m%d%H%M%S')
        
        # Parse YYYYMMDDHHMMSS format
        if len(date_str) >= 14:
            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])
            hour = int(date_str[8:10])
            minute = int(date_str[10:12])
            second = int(date_str[12:14])
            
            dt = datetime(year, month, day, hour, minute, second)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Handle YYYYMMDD format (date only)
        elif len(date_str) >= 8:
            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])
            
            dt = datetime(year, month, day)
            return dt.strftime('%Y-%m-%d')
        
        # Return as-is if we can't parse it
        return str(date_str)
        
    except (ValueError, TypeError, IndexError):
        return str(date_str) if date_str else None

logger = logging.getLogger(__name__)

# Initialize MCP server
mcp = FastMCP("Cloud Inventory Database Server")

# Define the actual implementation functions
async def _query_logs(
    table_name: str,
    filters: Optional[Dict[str, Any]] = None,
    limit: Optional[int] = None,
    offset: int = 0
) -> Dict[str, Any]:
    """Query database logs with filters"""
    try:
        from datetime import datetime, timedelta
        
        # Get database session
        db_gen = get_db()
        db = next(db_gen)
        
        try:
            # Map table names to models
            model_map = {
                "dsiactivities": DSIActivities,
                "dsitransactionlog": DSITransactionLog,
                "dsiactivities_archive": ArchiveDSIActivities,
                "dsitransactionlog_archive": ArchiveDSITransactionLog
            }
            
            if table_name not in model_map:
                return {"success": False, "error": f"Unknown table: {table_name}"}
            
            model = model_map[table_name]
            query = db.query(model)
            
            # Apply date filters if specified
            filter_description = None
            if filters and "date_filter" in filters:
                current_date = datetime.now()
                date_filter = filters["date_filter"]
                # Parse date filter and calculate cutoff date
                cutoff_date = None
                if "older_than_" in date_filter:
                    # Parse "older_than_X_months", "older_than_X_days", etc.
                    parts = date_filter.replace("older_than_", "").split("_")
                    if len(parts) >= 2:
                        try:
                            number = int(parts[0])
                            unit = parts[1]
                            
                            if unit.startswith("month"):
                                cutoff_date = current_date - timedelta(days=number * 30)
                                filter_description = f"older than {number} months"
                            elif unit.startswith("day"):
                                cutoff_date = current_date - timedelta(days=number)
                                filter_description = f"older than {number} days"
                            elif unit.startswith("year"):
                                cutoff_date = current_date - timedelta(days=number * 365)
                                filter_description = f"older than {number} years"
                        except ValueError:
                            pass  # Skip invalid date filter
                
                elif date_filter == "yesterday":
                    cutoff_date = current_date - timedelta(days=1)
                    filter_description = "from yesterday"
                elif date_filter == "recent":
                    cutoff_date = current_date - timedelta(days=7)
                    filter_description = "from last 7 days"
                
                # Apply the date filter to the query
                if cutoff_date:
                    cutoff_string = cutoff_date.strftime("%Y%m%d%H%M%S")
                    
                    # For "recent" filter, we want records NEWER than cutoff (greater than)
                    # For other filters like "older_than_X", we want records OLDER than cutoff (less than)
                    if filters and filters.get("date_filter") == "recent":
                        # Recent records: PostedTime >= cutoff_string
                        if hasattr(model, 'EndDateTime'):
                            query = query.filter(model.EndDateTime >= cutoff_string)
                        elif hasattr(model, 'TransactionDateTime'):
                            query = query.filter(model.TransactionDateTime >= cutoff_string)
                        elif hasattr(model, 'PostedTime'):
                            # PostedTime is stored as YYYYMMDDHHMMSS string
                            query = query.filter(model.PostedTime >= cutoff_string)
                    else:
                        # Older records: PostedTime < cutoff_string
                        if hasattr(model, 'EndDateTime'):
                            query = query.filter(model.EndDateTime < cutoff_string)
                        elif hasattr(model, 'TransactionDateTime'):
                            query = query.filter(model.TransactionDateTime < cutoff_string)
                        elif hasattr(model, 'PostedTime'):
                            # PostedTime is stored as YYYYMMDDHHMMSS string
                            query = query.filter(model.PostedTime < cutoff_string)
            
            # Apply offset and limit
            query = query.offset(offset)
            if limit is not None:
                query = query.limit(limit)
            
            # Execute query
            records = query.all()
            
            # Convert to dict format with formatted dates
            records_list = []
            date_columns = ['PostedTime', 'EndDateTime', 'TransactionDateTime', 'StartDateTime']
            
            for record in records:
                record_dict = {}
                for column in record.__table__.columns:
                    value = getattr(record, column.name)
                    # Format date columns for better readability
                    if column.name in date_columns and value:
                        record_dict[column.name] = format_database_date(value)
                    else:
                        record_dict[column.name] = value
                records_list.append(record_dict)
            
            return {
                "success": True,
                "records": records_list,
                "total_records": len(records_list),
                "filter_applied": filters.get("date_filter", "") if filters else "",
                "filter_description": filter_description
            }
            
        finally:
            db.close()
        
    except Exception as e:
        logger.error(f"Error in query_logs: {e}")
        return {
            "success": False,
            "error": str(e)
        }

async def _archive_records(
    table_name: str,
    filters: Dict[str, Any],
    user_id: str
) -> Dict[str, Any]:
    """Archive records from main table to archive table"""
    try:
        from datetime import datetime, timedelta
        
        # Convert date_filter to date_end for CRUD service compatibility
        processed_filters = filters.copy()
        
        # Check if this is a confirmed operation
        is_confirmed = processed_filters.pop("confirmed", False)
        
        # SAFETY RULE: Apply default 7-day filter for archive operations if no date filter provided
        if "date_filter" not in processed_filters and "date_end" not in processed_filters:
            processed_filters["date_filter"] = "older_than_7_days"
        
        if "date_filter" in processed_filters:
            date_filter = processed_filters.pop("date_filter")  # Remove date_filter
            current_date = datetime.now()
            
            # Parse date filter and calculate cutoff date
            cutoff_date = None
            is_older_than = False
            
            if "older_than_" in date_filter:
                # Parse "older_than_X_months", "older_than_X_days", etc.
                parts = date_filter.replace("older_than_", "").split("_")
                is_older_than = True  # Set flag for older than operations
                if len(parts) >= 2:
                    try:
                        number = int(parts[0])
                        unit = parts[1]
                        
                        # SAFETY CHECK: Enforce minimum 7-day archive age
                        if unit.startswith("day") and number < 7:
                            return {
                                "success": False,
                                "error": f"Safety rule violation: Cannot archive records less than 7 days old. Requested: {number} days, minimum required: 7 days"
                            }
                        
                        if unit.startswith("month"):
                            cutoff_date = current_date - timedelta(days=number * 30)
                        elif unit.startswith("day"):
                            cutoff_date = current_date - timedelta(days=number)
                        elif unit.startswith("year"):
                            cutoff_date = current_date - timedelta(days=number * 365)
                    except ValueError:
                        pass  # Skip invalid date filter
            
            elif date_filter == "yesterday":
                # SAFETY CHECK: Yesterday is less than 7 days old 
                return {
                    "success": False,
                    "error": "Safety rule violation: Cannot archive records from yesterday. Records must be at least 7 days old before archiving."
                }
            elif date_filter == "recent":
                # SAFETY CHECK: Recent (7 days) doesn't meet minimum age requirement
                return {
                    "success": False,  
                    "error": "Safety rule violation: Cannot archive 'recent' records (last 7 days). Records must be older than 7 days before archiving."
                }
            
            # Convert cutoff_date to date_end format for CRUD service
            if cutoff_date:
                cutoff_string = cutoff_date.strftime("%Y%m%d%H%M%S")
                processed_filters["date_end"] = cutoff_string
                # CRITICAL FIX: Set the date_comparison flag for proper < vs <= handling
                if is_older_than:
                    processed_filters["date_comparison"] = "older_than"
        
        db_gen = get_db()
        db = next(db_gen)
        
        try:
            # Create CRUD service with database session
            crud_service = CRUDService(db)
            
            # Create a mock ParsedOperation for the CRUDService
            from services.prompt_parser import ParsedOperation
            
            mock_operation = ParsedOperation(
                action="ARCHIVE",
                table=table_name,
                filters=processed_filters,
                confidence=1.0,
                original_prompt=f"Archive {table_name} (confirmed={is_confirmed})",
                validation_errors=[],
                is_archive_target=False
            )
            
            result = await crud_service.execute_archive_operation(
                operation=mock_operation,
                user_id=user_id,
                reason="MCP archive request" + (" - CONFIRMED" if is_confirmed else " - PREVIEW"),
                user_role="Admin",
                confirmed=is_confirmed  # Use the confirmed flag from filters
            )
            
            if result.get("success"):
                # Handle both preview and actual archive results
                archived_count = result.get("records_archived", 0) or result.get("preview_count", 0)
                return {
                    "success": True,
                    "archived_count": archived_count,
                    "message": result.get("message", "Records archived successfully"),
                    "requires_confirmation": result.get("requires_confirmation", False),
                    "filters": filters  # Return original filters for reference
                }
            else:
                return {
                    "success": False,
                    "error": result.get("error", "Archive failed"),
                    "filters": filters
                }
                
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Error in archive_records: {e}")
        return {
            "success": False,
            "error": str(e)
        }

async def _delete_archived_records(
    table_name: str,
    filters: Dict[str, Any],
    user_id: str
) -> Dict[str, Any]:
    """Delete records from archive tables"""
    try:
        from datetime import datetime, timedelta
        
        # Convert date_filter to date_end for CRUD service compatibility
        processed_filters = filters.copy()
        
        # Check if this is a confirmed operation
        is_confirmed = processed_filters.pop("confirmed", False)
        
        # SAFETY RULE: Apply default 30-day filter for delete operations if no date filter provided
        if "date_filter" not in processed_filters and "date_end" not in processed_filters:
            processed_filters["date_filter"] = "older_than_30_days"
        
        if "date_filter" in processed_filters:
            date_filter = processed_filters.pop("date_filter")  # Remove date_filter
            current_date = datetime.now()
            
            # Parse date filter and calculate cutoff date
            cutoff_date = None
            is_older_than = False
            
            if "older_than_" in date_filter:
                # Parse "older_than_X_months", "older_than_X_days", etc.
                parts = date_filter.replace("older_than_", "").split("_")
                is_older_than = True  # Set flag for older than operations
                if len(parts) >= 2:
                    try:
                        number = int(parts[0])
                        unit = parts[1]
                        
                        # SAFETY CHECK: Enforce minimum 30-day age for delete operations
                        if unit.startswith("day") and number < 30:
                            return {
                                "success": False,
                                "error": f"Safety rule violation: Cannot delete archived records less than 30 days old. Requested: {number} days, minimum required: 30 days"
                            }
                        
                        if unit.startswith("month"):
                            cutoff_date = current_date - timedelta(days=number * 30)
                        elif unit.startswith("day"):
                            cutoff_date = current_date - timedelta(days=number)
                        elif unit.startswith("year"):
                            cutoff_date = current_date - timedelta(days=number * 365)
                    except ValueError:
                        pass  # Skip invalid date filter
            
            elif date_filter == "yesterday":
                # SAFETY CHECK: Yesterday is much less than 30 days old 
                return {
                    "success": False,
                    "error": "Safety rule violation: Cannot delete records from yesterday. Archived records must be at least 30 days old before deletion."
                }
            elif date_filter == "recent":
                # SAFETY CHECK: Recent (7 days) doesn't meet minimum age requirement
                return {
                    "success": False,  
                    "error": "Safety rule violation: Cannot delete 'recent' archived records (last 7 days). Archived records must be older than 30 days before deletion."
                }
            
            # Convert cutoff_date to date_end format for CRUD service
            if cutoff_date:
                cutoff_string = cutoff_date.strftime("%Y%m%d%H%M%S")
                processed_filters["date_end"] = cutoff_string
                # CRITICAL FIX: Set the date_comparison flag for proper < vs <= handling
                if is_older_than:
                    processed_filters["date_comparison"] = "older_than"
        
        db_gen = get_db()
        db = next(db_gen)
        
        try:
            # Create CRUD service with database session
            crud_service = CRUDService(db)
            
            # Create a mock ParsedOperation for the CRUDService
            from services.prompt_parser import ParsedOperation
            
            # For delete operations, we target archive tables
            archive_table_name = f"{table_name}_archive" if not table_name.endswith("_archive") else table_name
            
            mock_operation = ParsedOperation(
                action="DELETE",
                table=archive_table_name,
                filters=processed_filters,
                confidence=1.0,
                original_prompt=f"Delete from {archive_table_name} (confirmed={is_confirmed})",
                validation_errors=[],
                is_archive_target=True
            )
            
            result = await crud_service.execute_delete_operation(
                operation=mock_operation,
                user_id=user_id,
                reason="MCP delete request" + (" - CONFIRMED" if is_confirmed else " - PREVIEW"),
                user_role="Admin",
                confirmed=is_confirmed  # Use the confirmed flag from filters
            )
            
            if result.get("success"):
                deleted_count = result.get("records_deleted", 0) or result.get("preview_count", 0)
                return {
                    "success": True,
                    "deleted_count": deleted_count,
                    "message": result.get("message", "Archived records deleted successfully"),
                    "requires_confirmation": result.get("requires_confirmation", False),
                    "filters": filters  # Return original filters for reference
                }
            else:
                return {
                    "success": False,
                    "error": result.get("error", "Delete failed")
                }
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Error in delete_archived_records: {e}")
        return {
            "success": False,
            "error": str(e)
        }

async def _get_table_stats(
    table_name: str, 
    filters: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Get statistics for a table, optionally with date filters"""
    try:
        from datetime import datetime, timedelta
        
        db_gen = get_db()
        db = next(db_gen)
        
        try:
            # Map table names to models
            model_map = {
                "dsiactivities": DSIActivities,
                "dsitransactionlog": DSITransactionLog,
                "dsiactivities_archive": ArchiveDSIActivities,
                "dsitransactionlog_archive": ArchiveDSITransactionLog
            }
            
            if table_name not in model_map:
                return {
                    "success": False,
                    "error": f"Unknown table: {table_name}"
                }
            
            model = model_map[table_name]
            base_query = db.query(model)
            
            # # Get total count (unfiltered) first
            # if hasattr(model, 'SequenceID'):
            #     total_count = base_query.with_entities(func.count(model.SequenceID)).scalar()
            # elif hasattr(model, 'RecordID'):
            #     total_count = base_query.with_entities(func.count(model.RecordID)).scalar()
            # else:
            total_count = base_query.count()
            
            # Start with the base query for filtering
            query = db.query(model)
            filtered_count = total_count  # Default to total if no filters
            
            # Apply date filters if provided
            filter_description = None
            if filters and "date_filter" in filters:
                date_filter = filters["date_filter"]
                current_date = datetime.now()
                
                # Parse date filter and calculate cutoff date
                cutoff_date = None
                if "older_than_" in date_filter:
                    # Parse "older_than_X_months", "older_than_X_days", etc.
                    parts = date_filter.replace("older_than_", "").split("_")
                    if len(parts) >= 2:
                        try:
                            number = int(parts[0])
                            unit = parts[1]
                            
                            if unit.startswith("month"):
                                cutoff_date = current_date - timedelta(days=number * 30)
                                filter_description = f"older than {number} months"
                            elif unit.startswith("day"):
                                cutoff_date = current_date - timedelta(days=number)
                                filter_description = f"older than {number} days"
                            elif unit.startswith("year"):
                                cutoff_date = current_date - timedelta(days=number * 365)
                                filter_description = f"older than {number} years"
                        except ValueError:
                            pass  # Skip invalid date filter
                
                elif date_filter == "yesterday":
                    cutoff_date = current_date - timedelta(days=1)
                    filter_description = "from yesterday"
                elif date_filter == "recent":
                    cutoff_date = current_date - timedelta(days=7)
                    filter_description = "from last 7 days"
                
                # Apply the date filter to the query
                if cutoff_date:
                    cutoff_string = cutoff_date.strftime("%Y%m%d%H%M%S")
                    
                    # For "recent" filter, we want records NEWER than cutoff (greater than)
                    # For other filters like "older_than_X", we want records OLDER than cutoff (less than)
                    if filters and filters.get("date_filter") == "recent":
                        # Recent records: PostedTime >= cutoff_string
                        if hasattr(model, 'EndDateTime'):
                            query = query.filter(model.EndDateTime >= cutoff_string)
                        elif hasattr(model, 'TransactionDateTime'):
                            query = query.filter(model.TransactionDateTime >= cutoff_string)
                        elif hasattr(model, 'PostedTime'):
                            # PostedTime is stored as YYYYMMDDHHMMSS string
                            query = query.filter(model.PostedTime >= cutoff_string)
                    else:
                        # Older records: PostedTime < cutoff_string
                        if hasattr(model, 'EndDateTime'):
                            query = query.filter(model.EndDateTime < cutoff_string)
                        elif hasattr(model, 'TransactionDateTime'):
                            query = query.filter(model.TransactionDateTime < cutoff_string)
                        elif hasattr(model, 'PostedTime'):
                            # PostedTime is stored as YYYYMMDDHHMMSS string
                            query = query.filter(model.PostedTime < cutoff_string)
                    
                    # Get filtered count after applying the filter
                    if hasattr(model, 'SequenceID'):
                        filtered_count = query.with_entities(func.count(model.SequenceID)).scalar()
                    elif hasattr(model, 'RecordID'):
                        filtered_count = query.with_entities(func.count(model.RecordID)).scalar()
                    else:
                        filtered_count = query.count()
                    

            
            # Use filtered count as the main count (this is what the user is asking for)
            count = filtered_count
            
            # Get date range - use appropriate query based on whether we have filters
            latest_date = earliest_date = None
            date_query = query if filters and "date_filter" in filters else base_query
            
            if hasattr(model, 'EndDateTime'):
                latest_date = date_query.with_entities(func.max(model.EndDateTime)).scalar()
                earliest_date = date_query.with_entities(func.min(model.EndDateTime)).scalar()
            elif hasattr(model, 'TransactionDateTime'):
                latest_date = date_query.with_entities(func.max(model.TransactionDateTime)).scalar()
                earliest_date = date_query.with_entities(func.min(model.TransactionDateTime)).scalar()
            elif hasattr(model, 'PostedTime'):
                latest_date = date_query.with_entities(func.max(model.PostedTime)).scalar()
                earliest_date = date_query.with_entities(func.min(model.PostedTime)).scalar()
            
            # Build response based on whether filters were applied
            if filters and "date_filter" in filters:
                # When filters are applied, return the filtered count as primary result
                response = {
                    "success": True,
                    "table_name": table_name,
                    "record_count": filtered_count,  # This is what the user asked for (e.g., recent count)
                    "total_records": total_count,    # Total unfiltered records in the table
                    "earliest_date": format_database_date(earliest_date),  # From filtered results
                    "latest_date": format_database_date(latest_date),      # From filtered results
                    "filter_applied": filters.get("date_filter", ""),
                    "filter_description": filter_description
                }
            else:
                # No filters - return total count
                response = {
                    "success": True,
                    "table_name": table_name,
                    "record_count": total_count,    # Same as total_records when no filter
                    "total_records": total_count,   # Total records in the table
                    "earliest_date": format_database_date(earliest_date),
                    "latest_date": format_database_date(latest_date)
                }
                
            return response
            
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Error in get_table_stats: {e}")
        return {
            "success": False,
            "error": str(e)
        }

async def _health_check() -> Dict[str, Any]:
    """Health check for the MCP server"""
    try:
        db_gen = get_db()
        db = next(db_gen)
        
        try:
            # Simple query to check database connectivity
            result = db.execute(text("SELECT 1")).scalar()
            return {
                "success": True,
                "status": "healthy",
                "database": "connected" if result == 1 else "disconnected",
                "timestamp": datetime.now().isoformat()
            }
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Error in health_check: {e}")
        return {
            "success": False,
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

async def _execute_confirmed_archive(
    table_name: str,
    filters: Dict[str, Any],
    user_id: str
) -> Dict[str, Any]:
    """Execute confirmed archive operation without preview"""
    try:
        from datetime import datetime, timedelta
        
        # Convert date_filter to date_end for CRUD service compatibility
        processed_filters = filters.copy()
        
        if "date_filter" in processed_filters:
            date_filter = processed_filters.pop("date_filter")  # Remove date_filter
            current_date = datetime.now()
            
            # Parse date filter and calculate cutoff date
            cutoff_date = None
            is_older_than = False
            
            if "older_than_" in date_filter:
                # Parse "older_than_X_months", "older_than_X_days", etc.
                parts = date_filter.replace("older_than_", "").split("_")
                is_older_than = True  # Set flag for older than operations
                if len(parts) >= 2:
                    try:
                        number = int(parts[0])
                        unit = parts[1]
                        
                        # SAFETY CHECK: Enforce minimum 7-day archive age
                        if unit.startswith("day") and number < 7:
                            return {
                                "success": False,
                                "error": f"Safety rule violation: Cannot archive records less than 7 days old. Requested: {number} days, minimum required: 7 days"
                            }
                        
                        if unit.startswith("month"):
                            cutoff_date = current_date - timedelta(days=number * 30)
                        elif unit.startswith("day"):
                            cutoff_date = current_date - timedelta(days=number)
                        elif unit.startswith("year"):
                            cutoff_date = current_date - timedelta(days=number * 365)
                    except ValueError:
                        pass  # Skip invalid date filter
            
            elif date_filter == "yesterday":
                # SAFETY CHECK: Yesterday is less than 7 days old 
                return {
                    "success": False,
                    "error": "Safety rule violation: Cannot archive records from yesterday. Records must be at least 7 days old before archiving."
                }
            elif date_filter == "recent":
                # SAFETY CHECK: Recent (7 days) doesn't meet minimum age requirement
                return {
                    "success": False,  
                    "error": "Safety rule violation: Cannot archive 'recent' records (last 7 days). Records must be older than 7 days before archiving."
                }
            
            # Convert cutoff_date to date_end format for CRUD service
            if cutoff_date:
                cutoff_string = cutoff_date.strftime("%Y%m%d%H%M%S")
                processed_filters["date_end"] = cutoff_string
                # CRITICAL FIX: Set the date_comparison flag for proper < vs <= handling
                if is_older_than:
                    processed_filters["date_comparison"] = "older_than"
        
        db_gen = get_db()
        db = next(db_gen)
        
        try:
            # Create CRUD service with database session
            crud_service = CRUDService(db)
            
            # Create a mock ParsedOperation for the CRUDService
            from services.prompt_parser import ParsedOperation
            
            mock_operation = ParsedOperation(
                action="ARCHIVE",
                table=table_name,
                filters=processed_filters,
                confidence=1.0,
                original_prompt=f"Confirmed archive {table_name}",
                validation_errors=[],
                is_archive_target=False
            )
            
            result = await crud_service.execute_archive_operation(
                operation=mock_operation,
                user_id=user_id,
                reason="User confirmed archive operation",
                user_role="Admin",
                confirmed=True  # Skip preview, execute directly
            )
            
            if result.get("success"):
                return {
                    "success": True,
                    "archived_count": result.get("records_archived", 0),
                    "message": result.get("message", "Records archived successfully")
                }
            else:
                return {
                    "success": False,
                    "error": result.get("error", "Archive failed")
                }
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Error in execute_confirmed_archive: {e}")
        return {
            "success": False,
            "error": str(e)
        }

async def _execute_confirmed_delete(
    table_name: str,
    filters: Dict[str, Any],
    user_id: str
) -> Dict[str, Any]:
    """Execute confirmed delete operation without preview"""
    try:
        from datetime import datetime, timedelta
        
        # Convert date_filter to date_end for CRUD service compatibility
        processed_filters = filters.copy()
        
        if "date_filter" in processed_filters:
            date_filter = processed_filters.pop("date_filter")  # Remove date_filter
            current_date = datetime.now()
            
            # Parse date filter and calculate cutoff date
            cutoff_date = None
            is_older_than = False
            
            if "older_than_" in date_filter:
                # Parse "older_than_X_months", "older_than_X_days", etc.
                parts = date_filter.replace("older_than_", "").split("_")
                is_older_than = True  # Set flag for older than operations
                if len(parts) >= 2:
                    try:
                        number = int(parts[0])
                        unit = parts[1]
                        
                        # SAFETY CHECK: Enforce minimum 30-day age for delete operations
                        if unit.startswith("day") and number < 30:
                            return {
                                "success": False,
                                "error": f"Safety rule violation: Cannot delete archived records less than 30 days old. Requested: {number} days, minimum required: 30 days"
                            }
                        
                        if unit.startswith("month"):
                            cutoff_date = current_date - timedelta(days=number * 30)
                        elif unit.startswith("day"):
                            cutoff_date = current_date - timedelta(days=number)
                        elif unit.startswith("year"):
                            cutoff_date = current_date - timedelta(days=number * 365)
                    except ValueError:
                        pass  # Skip invalid date filter
            
            elif date_filter == "yesterday":
                # SAFETY CHECK: Yesterday is much less than 30 days old 
                return {
                    "success": False,
                    "error": "Safety rule violation: Cannot delete records from yesterday. Archived records must be at least 30 days old before deletion."
                }
            elif date_filter == "recent":
                # SAFETY CHECK: Recent (7 days) doesn't meet minimum age requirement
                return {
                    "success": False,  
                    "error": "Safety rule violation: Cannot delete 'recent' archived records (last 7 days). Archived records must be older than 30 days before deletion."
                }
            
            # Convert cutoff_date to date_end format for CRUD service
            if cutoff_date:
                cutoff_string = cutoff_date.strftime("%Y%m%d%H%M%S")
                processed_filters["date_end"] = cutoff_string
                # CRITICAL FIX: Set the date_comparison flag for proper < vs <= handling
                if is_older_than:
                    processed_filters["date_comparison"] = "older_than"
        
        db_gen = get_db()
        db = next(db_gen)
        
        try:
            # Create CRUD service with database session
            crud_service = CRUDService(db)
            
            # Create a mock ParsedOperation for the CRUDService
            from services.prompt_parser import ParsedOperation
            
            # For delete operations, we target archive tables
            archive_table_name = f"{table_name}_archive" if not table_name.endswith("_archive") else table_name
            
            mock_operation = ParsedOperation(
                action="DELETE",
                table=archive_table_name,
                filters=processed_filters,
                confidence=1.0,
                original_prompt=f"Confirmed delete from {archive_table_name}",
                validation_errors=[],
                is_archive_target=True
            )
            
            result = await crud_service.execute_delete_operation(
                operation=mock_operation,
                user_id=user_id,
                reason="User confirmed delete operation",
                user_role="Admin",
                confirmed=True  # Skip preview, execute directly
            )
            
            if result.get("success"):
                return {
                    "success": True,
                    "deleted_count": result.get("records_deleted", 0),
                    "message": result.get("message", "Archived records deleted successfully")
                }
            else:
                return {
                    "success": False,
                    "error": result.get("error", "Delete failed")
                }
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Error in execute_confirmed_delete: {e}")
        return {
            "success": False,
            "error": str(e)
        }

# Register MCP tools that wrap the internal functions
@mcp.tool(name="query_logs")
async def mcp_query_logs(
    table_name: str,
    filters: Optional[Dict[str, Any]] = None,
    limit: Optional[int] = None,
    offset: int = 0
) -> Dict[str, Any]:
    """Query database logs with filters"""
    return await _query_logs(table_name, filters, limit, offset)

@mcp.tool(name="archive_records")
async def mcp_archive_records(
    table_name: str,
    filters: Dict[str, Any],
    user_id: str
) -> Dict[str, Any]:
    """Archive records from main table to archive table"""
    return await _archive_records(table_name, filters, user_id)

@mcp.tool(name="delete_archived_records")
async def mcp_delete_archived_records(
    table_name: str,
    filters: Dict[str, Any],
    user_id: str
) -> Dict[str, Any]:
    """Delete records from archive tables"""
    return await _delete_archived_records(table_name, filters, user_id)

@mcp.tool(name="get_table_stats")
async def mcp_get_table_stats(
    table_name: str, 
    filters: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Get statistics for a table, optionally with date filters"""
    return await _get_table_stats(table_name, filters)

@mcp.tool(name="health_check")
async def mcp_health_check() -> Dict[str, Any]:
    """Health check for the MCP server"""
    return await _health_check()

# Export the internal functions for backward compatibility with existing code
query_logs = _query_logs
archive_records = _archive_records  
delete_archived_records = _delete_archived_records
get_table_stats = _get_table_stats
health_check = _health_check
execute_confirmed_archive = _execute_confirmed_archive
execute_confirmed_delete = _execute_confirmed_delete

# Schema information for different tables
activities_schema = {
    "table": "dsiactivities",
    "columns": [
        {"name": "id", "type": "int", "description": "Unique identifier"},
        {"name": "UserName", "type": "string", "description": "User who performed activity"},
        {"name": "ActivityType", "type": "string", "description": "Type of activity"},
        {"name": "StartDateTime", "type": "datetime", "description": "When activity started"},
        {"name": "EndDateTime", "type": "datetime", "description": "When activity ended"},
        {"name": "ServiceName", "type": "string", "description": "Service involved"},
        {"name": "Region", "type": "string", "description": "Geographic region"}
    ],
    "sample_filters": {
        "UserName": "john.doe",
        "ActivityType": "compute",
        "ServiceName": "EC2",
        "Region": "us-east-1"
    }
}

transaction_schema = {
    "table": "dsitransactionlog", 
    "columns": [
        {"name": "id", "type": "int", "description": "Unique identifier"},
        {"name": "UserName", "type": "string", "description": "User who made transaction"},
        {"name": "TransactionType", "type": "string", "description": "Type of transaction"},
        {"name": "TransactionDateTime", "type": "datetime", "description": "When transaction occurred"},
        {"name": "Amount", "type": "decimal", "description": "Transaction amount"},
        {"name": "ServiceName", "type": "string", "description": "Service involved"},
        {"name": "Region", "type": "string", "description": "Geographic region"}
    ],
    "sample_filters": {
        "UserName": "jane.smith",
        "TransactionType": "billing",
        "ServiceName": "S3",
        "Region": "us-west-2"
    }
}

# Add resource definitions for MCP
@mcp.resource("database://activities")
async def get_activities_resource() -> str:
    """Get activities table schema information"""
    return str(activities_schema)

@mcp.resource("database://transactions") 
async def get_transactions_resource() -> str:
    """Get transactions table schema information"""
    return str(transaction_schema)


def main():
    """Main entry point for MCP server"""
    import asyncio
    import sys
    
    try:
        # Run the MCP server
        asyncio.run(mcp.run())
    except KeyboardInterrupt:
        logger.info("MCP Server stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"MCP Server failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
