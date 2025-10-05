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
from models.job_logs import JobLogs
from services.job_logs_service import JobLogsService
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
            from schemas import ParsedOperation
            
            # CRITICAL FIX: Ensure the confirmed flag is preserved in filters for proper execution
            if is_confirmed and "confirmed" not in processed_filters:
                processed_filters["confirmed"] = True
            
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
                # For previews, use preview_count; for actual operations, use records_archived
                if result.get("requires_confirmation", False):
                    # This is a preview - use preview_count
                    archived_count = result.get("preview_count", 0)
                else:
                    # This is actual execution - use records_archived
                    archived_count = result.get("records_archived", 0)
                
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
            from schemas import ParsedOperation
            
            # For delete operations, we target archive tables
            # Map main table names to archive table names using new naming convention
            if table_name == "dsiactivities":
                archive_table_name = "dsiactivitiesarchive"
            elif table_name == "dsitransactionlog":
                archive_table_name = "dsitransactionlogarchive"
            elif table_name in ["dsiactivitiesarchive", "dsitransactionlogarchive"]:
                archive_table_name = table_name  # Already an archive table
            else:
                archive_table_name = f"{table_name}archive"  # Fallback for other tables
            
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
                # Handle both preview and actual delete results
                # For previews, use preview_count; for actual operations, use records_deleted
                if result.get("requires_confirmation", False):
                    # This is a preview - use preview_count
                    deleted_count = result.get("preview_count", 0)
                else:
                    # This is actual execution - use records_deleted
                    deleted_count = result.get("records_deleted", 0)
                
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
                "dsiactivitiesarchive": ArchiveDSIActivities,
                "dsitransactionlogarchive": ArchiveDSITransactionLog
            }
            
            if table_name not in model_map:
                return {
                    "success": False,
                    "error": f"Unknown table: {table_name}"
                }
            
            model = model_map[table_name]
            base_query = db.query(model)
        
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
                        # Recent records: date field >= cutoff_string
                        if hasattr(model, 'PostedTime'):
                            # Activities tables use PostedTime
                            query = query.filter(model.PostedTime >= cutoff_string)
                        elif hasattr(model, 'WhenReceived'):
                            # Transaction tables use WhenReceived
                            query = query.filter(model.WhenReceived >= cutoff_string)
                    else:
                        # Older records: date field < cutoff_string
                        if hasattr(model, 'PostedTime'):
                            # Activities tables use PostedTime
                            query = query.filter(model.PostedTime < cutoff_string)
                        elif hasattr(model, 'WhenReceived'):
                            # Transaction tables use WhenReceived
                            query = query.filter(model.WhenReceived < cutoff_string)
                    
                    # Get filtered count after applying the filter - use .count() to include all rows
                    filtered_count = query.count()
                    

            
            # Use filtered count as the main count (this is what the user is asking for)
            count = filtered_count
            
            # Get date range - use appropriate query based on whether we have filters
            latest_date = earliest_date = None
            date_query = query if filters and "date_filter" in filters else base_query
            
            if hasattr(model, 'PostedTime'):
                # Activities table uses PostedTime
                latest_date = date_query.with_entities(func.max(model.PostedTime)).scalar()
                earliest_date = date_query.with_entities(func.min(model.PostedTime)).scalar()
            elif hasattr(model, 'WhenReceived'):
                # Transaction table uses WhenReceived
                latest_date = date_query.with_entities(func.max(model.WhenReceived)).scalar()
                earliest_date = date_query.with_entities(func.min(model.WhenReceived)).scalar()
            
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

async def _region_status() -> Dict[str, Any]:
    """Get region connection status and current region information"""
    try:
        from services.region_service import get_region_service
        
        region_service = get_region_service()
        
        # Get current region
        current_region = region_service.get_current_region()
        
        # Get all available regions
        available_regions = region_service.get_available_regions()
        
        # Get connection status for all regions
        connection_status = region_service.get_connection_status()
        
        # Find connected regions
        connected_regions = [region for region, is_connected in connection_status.items() if is_connected]
        
        # Get default region
        default_region = region_service.get_default_region()
        
        return {
            "success": True,
            "current_region": current_region,
            "default_region": default_region,
            "available_regions": available_regions,
            "connection_status": connection_status,
            "connected_regions": connected_regions,
            "total_regions": len(available_regions),
            "connected_count": len(connected_regions),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in region_status: {e}")
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
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

async def _query_job_logs(
    filters: Optional[Dict[str, Any]] = None,
    limit: int = 5,
    offset: int = 0,
    order_by: str = "started_at",
    order_direction: str = "desc"
) -> Dict[str, Any]:
    """Query job logs with various filters and return structured content"""
    try:
        from services.region_service import get_region_service
        
        # Extract query parameters from filters if they exist there
        if filters:
            # Extract limit, offset, order_by, order_direction from filters and use as parameters
            # Note: format parameter is preserved in filters for later use in table formatting
            limit = filters.pop("limit", limit)
            offset = filters.pop("offset", offset) 
            order_by = filters.pop("order_by", order_by)
            order_direction = filters.pop("order_direction", order_direction)
            # Keep format parameter in filters for table formatting logic
        
        db_gen = get_db()
        db = next(db_gen)
        
        try:
            job_logs_service = JobLogsService(db)
            result = job_logs_service.query_job_logs(
                filters=filters,
                limit=limit,
                offset=offset,
                order_by=order_by,
                order_direction=order_direction
            )
            
            if not result.get('success'):
                return {
                    "type": "error_card",
                    "title": "Job Logs Query Error",
                    "region": get_region_service().get_current_region() or "Unknown",
                    "error_message": result.get('error', 'Unknown error occurred'),
                    "suggestions": [
                        "Check your filters and try again",
                        "Show me all job logs",
                        "Get job summary statistics"
                    ]
                }
            
            # Format records for display
            records = result.get('records', [])
            total_count = result.get('total_count', 0)
            
            # Create structured content for job logs table
            if not records:
                # Format filter message more clearly
                filter_msg = "None"
                if filters:
                    filter_parts = [f"{k} = {v}" for k, v in filters.items()]
                    filter_msg = ", ".join(filter_parts)
                
                return {
                    "type": "conversational_card",
                    "title": "Job Logs",
                    "region": get_region_service().get_current_region() or "Unknown",
                    "user_role": "Admin",
                    "content": f"No job logs found matching criteria.\n\nTotal records in database: {total_count}",
                    "suggestions": [
                        "Show job statistics",
                        "Show all jobs",
                        "Show recent jobs"
                    ]
                }
            
            # Check if reason-only format is requested
            if filters and filters.get('format') == 'reason_only':
                if records:
                    reason = records[0].get('reason', 'No reason provided')
                    job_info = records[0]
                    
                    return {
                        "type": "conversational_card",
                        "title": "Job Status",
                        "region": get_region_service().get_current_region() or "Unknown",
                        "user_role": "Admin",
                        "content": f"{reason}\n\nTable: {job_info.get('table_name', 'Unknown')}",
                        "suggestions": []
                    }
                else:
                    return {
                        "type": "conversational_card",
                        "title": "No Jobs Found",
                        "region": get_region_service().get_current_region() or "Unknown", 
                        "user_role": "Admin",
                        "content": "No jobs found in the system.",
                        "suggestions": []
                    }
            
            # Check if detailed table format is requested
            show_table = (
                (filters and filters.get('format') == 'table') or 
                any(keyword in str(filters).lower() for keyword in ['detail', 'table', 'full']) if filters else False
            )
            
            # Always use table format if explicitly requested via format parameter or by default
            if filters and filters.get('format') == 'table':
                show_table = True
            
            # Default to table format for job logs unless explicitly requesting list format
            if not filters or filters.get('format') != 'list':
                show_table = True
            
            # Final safeguard: if format is not explicitly set to 'list', use table format
            if not show_table and (not filters or 'format' not in filters):
                show_table = True
            
            if show_table:  # Use table format when requested or for smaller result sets
                # Calculate summary stats
                summary_stats = {
                    'successful': len([r for r in records if r.get('status') == 'SUCCESS']),
                    'failed': len([r for r in records if r.get('status') == 'FAILED']),
                    'in_progress': len([r for r in records if r.get('status') == 'IN_PROGRESS'])
                }
                
                return {
                    "type": "job_logs_table",
                    "title": "Job Logs",
                    "region": get_region_service().get_current_region() or "Unknown",
                    "records": records,
                    "total_count": total_count,
                    "filters_applied": filters or {},
                    "suggestions": []
                }
            
            # Create job logs summary content for larger result sets
            if len(records) > 1:
                table_content = f"Found {len(records)} job logs (showing {offset + 1}-{offset + len(records)} of {total_count} total):\n\n"
            else:
                table_content = f"Found {len(records)} job logs:\n\n"
            
            for i, record in enumerate(records[:10]):  # Limit to first 10 for display
                started_time = record.get('started_at', '')
                if started_time:
                    try:
                        from datetime import datetime
                        dt = datetime.fromisoformat(started_time.replace('Z', '+00:00'))
                        started_time = dt.strftime('%m/%d %H:%M')
                    except:
                        started_time = started_time[:16] if len(started_time) > 16 else started_time
                
                duration = record.get('duration_seconds', 0)
                duration_str = f"{duration:.1f}s" if duration and duration > 0 else "-"
                
                table_content += f"{i+1:2d}. [{record.get('status', 'UNKNOWN')}] [{record.get('job_type', 'UNKNOWN')}] [{record.get('id', '?')}] {record.get('table_name', 'Unknown')}\n"
                table_content += f"    Records: {record.get('records_affected', 0):,} | Duration: {duration_str} | Started: {started_time}\n"
                
                reason = record.get('reason', '')
                if reason:
                    reason_short = reason[:80] + '...' if len(reason) > 80 else reason
                    table_content += f"    Reason: {reason_short}\n"
                table_content += "\n"
            
            if len(records) > 10:
                table_content += f"... and {len(records) - 10} more records\n"
            
            # Add filter info if applied
            if filters:
                table_content += f"\nFilters applied: {', '.join([f'{k}={v}' for k, v in filters.items()])}\n"
            
            return {
                "type": "conversational_card",
                "title": "Job Logs",
                "region": get_region_service().get_current_region() or "Unknown",
                "user_role": "Admin",
                "content": table_content
            }
            
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Error in query_job_logs: {e}")
        return {
            "type": "error_card",
            "title": "Job Logs Query Error",
            "region": "Unknown",
            "error_message": str(e),
            "suggestions": [
                "Try a simpler query",
                "Show me recent job logs",
                "Get job summary statistics"
            ]
        }

async def _get_job_summary_stats(
    filters: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Get summary statistics for job logs and return structured content"""
    try:
        from services.region_service import get_region_service
        
        db_gen = get_db()
        db = next(db_gen)
        
        try:
            job_logs_service = JobLogsService(db)
            result = job_logs_service.get_job_summary_stats(filters=filters)
            
            if not result.get('success'):
                return {
                    "type": "error_card",
                    "title": "Job Summary Error",
                    "region": get_region_service().get_current_region() or "Unknown",
                    "error_message": result.get('error', 'Failed to get job statistics'),
                    "suggestions": [
                        "Show me recent job logs",
                    ]
                }
            
            summary = result.get('summary', {})
            job_types = result.get('job_types', [])
            tables = result.get('tables', [])
            records_stats = result.get('records_stats', {})
            
            # Check if count_only format is requested
            if filters and filters.get('format') == 'count_only':
                count_type = filters.get('count_type', 'total')
                date_range = filters.get('date_range', None)
                region = get_region_service().get_current_region() or "Unknown"
                
                # Create date suffix for title and label
                date_suffix = ""
                if date_range:
                    if date_range == "last_month":
                        date_suffix = " (Last Month)"
                    elif date_range == "today":
                        date_suffix = " (Today)"
                    elif date_range == "this_week":
                        date_suffix = " (This Week)"
                    elif date_range == "this_month":
                        date_suffix = " (This Month)"
                
                if count_type == 'successful':
                    count_value = summary.get('successful_jobs', 0)
                    title = f"Job Statistics"
                    label = f"Successful Jobs\n{date_suffix}"
                elif count_type == 'failed':
                    count_value = summary.get('failed_jobs', 0)
                    title = f"Job Statistics"
                    label = f"Failed Jobs\n{date_suffix}"
                else:  # total
                    count_value = summary.get('total_jobs', 0)
                    title = f"Job Statistics"
                    label = f"Total Jobs\n{date_suffix}"
                
                return {
                    "type": "stats_card",
                    "title": title,
                    "region": region,
                    "table_name": "",
                    "stats": [
                        {
                            "label": label,
                            "value": str(count_value),
                            "type": "number",
                            "highlight": True
                        }
                    ]
                }
            
            # Create stats for the stats card
            stats = [
                {
                    "label": "Total Jobs",
                    "value": str(summary.get('total_jobs', 0)),
                    "type": "number",
                    "highlight": True
                },
                {
                    "label": "Successful",
                    "value": str(summary.get('successful_jobs', 0)),
                    "type": "number",
                    "highlight": False
                },
                {
                    "label": "Failed",
                    "value": str(summary.get('failed_jobs', 0)),
                    "type": "number",
                    "highlight": summary.get('failed_jobs', 0) > 0
                },
                # {
                #     "label": "Success Rate",
                #     "value": f"{summary.get('success_rate', 0):.1f}%",
                #     "type": "percentage",
                #     "highlight": summary.get('success_rate', 0) < 95
                # }
            ]
            
            # Create additional details for job types and tables
            details = []
            
            if job_types:
                job_type_details = ", ".join([f"{jt['job_type']}: {jt['count']}" for jt in job_types[:3]])
                details.append(f"Job Types: {job_type_details}")
            
            if tables:
                table_details = ", ".join([f"{t['table_name']}: {t['count']}" for t in tables[:3]])
                details.append(f"Top Tables: {table_details}")
            
            if records_stats.get('max_records_in_job', 0) > 0:
                details.append(f"Largest Job: {records_stats['max_records_in_job']:,} records")
            
            filter_description = ""
            if filters:
                filter_parts = []
                for key, value in filters.items():
                    filter_parts.append(str(value))
                filter_description = f" {', '.join(filter_parts)}"
            
            return {
                "type": "stats_card",
                "title": f"Job Statistics",
                "region": get_region_service().get_current_region() or "Unknown",
                "table_name": "",
                "filter_description": filter_description,
                "stats": stats,
                "details": details
            }
            
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Error in get_job_summary_stats: {e}")
        return {
            "type": "error_card",
            "title": "Job Summary Error",
            "region": "Unknown",
            "error_message": str(e),
            "suggestions": [
                "Try again",
                "Show me recent job logs",
                "Check system status"
            ]
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
            from schemas import ParsedOperation
            
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
            from schemas import ParsedOperation
            
            # For delete operations, we target archive tables
            # Map main table names to archive table names using new naming convention
            if table_name == "dsiactivities":
                archive_table_name = "dsiactivitiesarchive"
            elif table_name == "dsitransactionlog":
                archive_table_name = "dsitransactionlogarchive"
            elif table_name in ["dsiactivitiesarchive", "dsitransactionlogarchive"]:
                archive_table_name = table_name  # Already an archive table
            else:
                archive_table_name = f"{table_name}archive"  # Fallback for other tables
            
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

@mcp.tool(name="region_status")
async def mcp_region_status() -> Dict[str, Any]:
    """Get region connection status and current region information"""
    return await _region_status()

@mcp.tool(name="health_check")
async def mcp_health_check() -> Dict[str, Any]:
    """Health check for the MCP server"""
    return await _health_check()

@mcp.tool(name="query_job_logs")
async def mcp_query_job_logs(
    filters: Optional[Dict[str, Any]] = None,
    limit: int = 5,
    offset: int = 0,
    order_by: str = "started_at",
    order_direction: str = "desc"
) -> Dict[str, Any]:
    """Query job logs with various filters
    
    Available filters:
    - status: str or list (IN_PROGRESS, SUCCESS, FAILED)
    - job_type: str or list (DELETE, ARCHIVE, OTHER)
    - table_name: str or list (table names)
    - schema_name: str or list (schema names)
    - id: int or list (job IDs)
    - min_records_affected: int
    - max_records_affected: int
    - started_after: ISO datetime string
    - started_before: ISO datetime string
    - finished_after: ISO datetime string
    - finished_before: ISO datetime string
    - date_range: str (today, yesterday, this_week, this_month, last_7_days, last_30_days)
    - reason_contains: str (text search in reason field)
    - failed_only: bool
    - successful_only: bool
    - in_progress_only: bool
    - zero_records_only: bool
    - has_records_only: bool
    
    Available order_by fields: id, job_type, table_name, status, records_affected, started_at, finished_at
    order_direction: 'asc' or 'desc'
    """
    return await _query_job_logs(filters, limit, offset, order_by, order_direction)

@mcp.tool(name="get_job_summary_stats")
async def mcp_get_job_summary_stats(
    filters: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Get summary statistics for job logs
    
    Returns statistics including:
    - Total jobs, successful, failed, in-progress counts
    - Success rate percentage
    - Job type breakdown
    - Table breakdown
    - Records affected statistics
    
    Accepts same filters as query_job_logs
    """
    return await _get_job_summary_stats(filters)

archive_records = _archive_records  
delete_archived_records = _delete_archived_records
get_table_stats = _get_table_stats
region_status = _region_status
health_check = _health_check
execute_confirmed_archive = _execute_confirmed_archive
execute_confirmed_delete = _execute_confirmed_delete
query_job_logs = _query_job_logs
get_job_summary_stats = _get_job_summary_stats

# Schema information for different tables
activities_schema = {
    "table": "dsiactivities",
    "columns": [
        {"name": "SequenceID", "type": "int", "description": "Unique identifier"},
        {"name": "ActivityID", "type": "string", "description": "Unique activity ID"},
        {"name": "ActivityType", "type": "string", "description": "Type of activity"},
        {"name": "AgentName", "type": "string", "description": "Name of the agent"},
        {"name": "PostedTime", "type": "datetime", "description": "When activity posted"},
        {"name": "PostedTimeUTC", "type": "datetime", "description": "When activity posted (UTC)"},
        {"name": "MethodName", "type": "string", "description": "Method invoked"},
        {"name": "ServerName", "type": "string", "description": "Server involved"},
        {"name": "InstanceID", "type": "string", "description": "Instance ID"},
        {"name": "EventID", "type": "string", "description": "Event ID"}
    ],
    "sample_filters": {
        "SequenceID": "1",
        "ActivityID": "95302abb-8e4c-45bf-9dc7-d579a534f34f",
        "ActivityType": "Event",
        "AgentName": "DSIManager",
        "PostedTime": "20250117131117",
        "MethodName": "GetLicenses",
        "ServerName": "USE2PLTFRMDV-D1",
        "InstanceID": "87522dda846f43aab3ba834891c77419",
        "EventID": "MGR150"
    }
}

transaction_schema = {
    "table": "dsitransactionlog", 
    "columns": [
        {"name": "RecordID", "type": "int", "description": "Unique identifier"},
        {"name": "RecordStatus", "type": "string", "description": "Record status"},
        {"name": "ProcessMethod", "type": "string", "description": "Process method"},
        {"name": "TransactionType", "type": "datetime", "description": "Transaction type"},
        {"name": "ServerName", "type": "datetime", "description": "Server name"},
        {"name": "DeviceID", "type": "string", "description": "Device identifier"},
        {"name": "UserID", "type": "string", "description": "User identifier"},
        {"name": "DeviceLocalTime", "type": "string", "description": "Device local time"},
        {"name": "DeviceUTCTime", "type": "string", "description": "Device UTC time"},
        {"name": "WhenReceived", "type": "string", "description": "When received"},
        {"name": "WhenProcessed", "type": "string", "description": "When processed"},
        {"name": "PromotionLevelID", "type": "string", "description": "Promotion level ID"},
        {"name": "EnvironmentID", "type": "string", "description": "Environment ID"},
    ],
    "sample_filters": {
        "RecordID": "14900",
        "RecordStatus": "1",
        "ProcessMethod": "S",
        "TransactionType": "A",
        "ServerName": "USE2PLTFRMDV-D2",
        "DeviceID": "H591e07610",
        "UserID": "DSI",
        "DeviceLocalTime": "20250909110721",
        "DeviceUTCTime": "20250909110721",
        "WhenReceived": "20250909110721",
        "WhenProcessed": "20250909110721",
        "PromotionLevelID": "Production",
        "EnvironmentID": "Prod"
    }
}

# Schema information for job_logs table
job_logs_schema = {
    "table": "job_logs",
    "columns": [
        {"name": "id", "type": "bigint", "description": "Unique identifier (auto-increment)"},
        {"name": "schema_name", "type": "string", "description": "Optional schema name for multi-schema jobs"},
        {"name": "job_type", "type": "string", "description": "Type of job (DELETE/ARCHIVE/OTHER)"},
        {"name": "table_name", "type": "string", "description": "Table affected by the job"},
        {"name": "status", "type": "string", "description": "Job status (IN_PROGRESS/SUCCESS/FAILED)"},
        {"name": "reason", "type": "text", "description": "Success message or failure reason"},
        {"name": "records_affected", "type": "int", "description": "Number of rows affected"},
        {"name": "started_at", "type": "datetime", "description": "When job started"},
        {"name": "finished_at", "type": "datetime", "description": "When job finished"}
    ],
    "sample_filters": {
        "id": "5",
        "schema_name": "log_management",
        "job_type": "ARCHIVE",
        "table_name": "dsitransactionlogarchive",
        "status": "SUCCESS",
        "reason": "Archived 333 records older than 7 days",
        "records_affected": "333",
        "started_at": "2025-10-02T21:26:21",
        "finished_at": "2025-10-02T21:26:21"
    },
    "available_filters": {
        "status": ["IN_PROGRESS", "SUCCESS", "FAILED"],
        "job_type": ["DELETE", "ARCHIVE", "OTHER"],
        "date_ranges": ["today", "yesterday", "this_week", "this_month", "last_7_days", "last_30_days"],
        "boolean_filters": ["failed_only", "successful_only", "in_progress_only", "zero_records_only", "has_records_only"]
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

@mcp.resource("database://job_logs")
async def get_job_logs_resource() -> str:
    """Get job_logs table schema information"""
    return str(job_logs_schema)


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
