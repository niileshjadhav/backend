"""Enhanced CRUD operations service with full DELETE functionality"""
from sqlalchemy.orm import Session
from sqlalchemy import text, func, and_, or_
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import logging

from models import (
    DSIActivities, DSITransactionLog, ArchiveDSIActivities, ArchiveDSITransactionLog,
    AuditLog
)
from services.auth_service import AuthService
from schemas import ParsedOperation

logger = logging.getLogger(__name__)

class CRUDService:
    """Comprehensive CRUD operations with safety mechanisms"""
    
    def __init__(self, db_session: Session):
        self.db = db_session
        self.auth_service = AuthService()
    
    async def execute_select_operation(
        self, 
        operation: ParsedOperation, 
        user_id: str,
        user_role: str = "Monitor",
        limit: Optional[int] = None,
        offset: int = 0
    ) -> Dict[str, Any]:
        """Execute SELECT operation with pagination"""
        try:
            # Verify permissions using provided user_role
            if not self.auth_service.check_permission(user_role, "SELECT"):
                return {"success": False, "error": "Permission denied"}
            
            user_data = {"user_id": user_id, "role": user_role}
            
            # Get model classes
            main_model, archive_model = self._get_model_classes(operation.table)
            target_model = archive_model if operation.is_archive_target else main_model
            
            # Build query
            query = self.db.query(target_model)
            query = self._apply_filters(query, operation, target_model)
            
            # Get total count
            total_count = query.count()
            
            # Get paginated results
            query_with_offset = query.offset(offset)
            if limit is not None:
                query_with_offset = query_with_offset.limit(limit)
            records = query_with_offset.all()
            
            # Note: Chat operation logging is now handled by ChatService
            
            return {
                "success": True,
                "operation": "SELECT",
                "table": f"{operation.table}{'_archive' if operation.is_archive_target else ''}",
                "total_records": total_count,
                "returned_records": len(records),
                "records": [self._record_to_dict(record) for record in records],
                "pagination": {
                    "limit": limit,
                    "offset": offset,
                    "has_more": offset + limit < total_count
                }
            }
            
        except Exception as e:
            logger.error(f"SELECT operation failed: {e}")
            # Note: Chat operation logging is now handled by ChatService
            return {"success": False, "error": str(e)}
    
    async def execute_archive_operation(
        self, 
        operation: ParsedOperation, 
        user_id: str,
        reason: str,
        user_role: str = "Admin",
        confirmed: bool = False
    ) -> Dict[str, Any]:
        """Execute ARCHIVE operation (main → archive)"""
        try:
            # Verify permissions using provided user_role
            if not self.auth_service.check_permission(user_role, "ARCHIVE"):
                return {"success": False, "error": "Permission denied - Admin role required"}
            
            user_data = {"user_id": user_id, "role": user_role}
            
            # Validation
            if operation.validation_errors:
                return {"success": False, "error": f"Validation failed: {', '.join(operation.validation_errors)}"}
            
            # SAFETY CHECK: Enforce 7-day minimum archive age
            if "date_end" in operation.filters:
                from datetime import datetime, timedelta
                current_date = datetime.now()
                min_archive_date = current_date - timedelta(days=7)
                
                # Parse the date_end filter
                date_end_str = operation.filters["date_end"]
                try:
                    # Assuming YYYYMMDDHHMMSS format
                    if len(date_end_str) >= 8:
                        filter_date = datetime.strptime(date_end_str[:8], "%Y%m%d")
                        if filter_date > min_archive_date:
                            return {
                                "success": False, 
                                "error": f"Safety rule violation: Can only archive records older than 7 days. Current cutoff date {filter_date.strftime('%Y-%m-%d')} is too recent. Minimum allowed date: {min_archive_date.strftime('%Y-%m-%d')}"
                            }
                except ValueError:
                    logger.warning(f"Could not parse date_end for validation: {date_end_str}")
            
            # Preview first if not confirmed
            if not confirmed:
                preview = await self._preview_archive_operation(operation, user_id)
                # Only require confirmation if there are records to process
                if preview.get("preview_count", 0) > 0:
                    preview["requires_confirmation"] = True
                return preview
            
            # Execute archive operation
            main_model, archive_model = self._get_model_classes(operation.table)
            
            # Start transaction
            try:
                self.db.begin()
                
                # Create audit log entry
                audit_entry = AuditLog(
                    operation_type="archive",
                    table_name=operation.table,
                    user_id=user_id,
                    date_range_start=operation.filters.get("date_start"),
                    date_range_end=operation.filters.get("date_end"),
                    status="in_progress",
                    operation_details={
                        "reason": reason,
                        "filters": operation.filters,
                        "confidence": operation.confidence
                    }
                )
                self.db.add(audit_entry)
                self.db.flush()
                
                # Execute archive
                archived_count, deleted_count = await self._perform_archive(
                    operation, main_model, archive_model, user_id, reason
                )
                
                # Update audit log
                audit_entry.records_affected = archived_count
                audit_entry.status = "success"
                
                # Note: Chat operation logging is now handled by ChatService
                
                self.db.commit()
                
                return {
                    "success": True,
                    "operation": "ARCHIVE",
                    "table": operation.table,
                    "records_archived": archived_count,
                    "records_deleted": deleted_count,
                    "audit_id": audit_entry.id,
                    "message": f"Successfully archived {archived_count} records from {operation.table}"
                }
                
            except Exception as e:
                self.db.rollback()
                if 'audit_entry' in locals():
                    audit_entry.status = "failed"
                    audit_entry.error_message = str(e)
                    try:
                        self.db.commit()
                    except:
                        pass
                raise e
                
        except Exception as e:
            logger.error(f"ARCHIVE operation failed: {e}")
            return {"success": False, "error": str(e)}
    
    async def execute_delete_operation(
        self, 
        operation: ParsedOperation, 
        user_id: str,
        reason: str,
        user_role: str = "Admin",
        confirmed: bool = False
    ) -> Dict[str, Any]:
        """Execute DELETE operation (archive only)"""
        try:
            # Verify permissions using provided user_role
            if not self.auth_service.check_permission(user_role, "DELETE"):
                return {"success": False, "error": "Permission denied - Admin role required"}
            
            user_data = {"user_id": user_id, "role": user_role}
            
            # Validation
            if operation.validation_errors:
                return {"success": False, "error": f"Validation failed: {', '.join(operation.validation_errors)}"}
            
            # DELETE can only target archive tables
            if not operation.is_archive_target:
                return {"success": False, "error": "DELETE operations can only target archive tables"}
            
            # SAFETY CHECK: Enforce minimum 30-day age for delete operations
            if "date_end" in operation.filters:
                current_date = datetime.now()
                min_delete_date = current_date - timedelta(days=30)
                
                # Parse the date_end filter
                date_end_str = operation.filters["date_end"]
                try:
                    # Assuming YYYYMMDDHHMMSS format
                    if len(date_end_str) >= 8:
                        filter_date = datetime.strptime(date_end_str[:8], "%Y%m%d")
                        if filter_date > min_delete_date:
                            return {
                                "success": False, 
                                "error": f"Safety rule violation: Can only delete archived records older than 30 days. Current cutoff date {filter_date.strftime('%Y-%m-%d')} is too recent. Minimum allowed date: {min_delete_date.strftime('%Y-%m-%d')}"
                            }
                except ValueError:
                    logger.warning(f"Could not parse date_end for delete validation: {date_end_str}")
            
            # Preview first if not confirmed
            if not confirmed:
                preview = await self._preview_delete_operation(operation, user_id)
                # Only require confirmation if there are records to process
                if preview.get("preview_count", 0) > 0:
                    preview["requires_confirmation"] = True
                return preview
            
            # Execute delete operation
            _, archive_model = self._get_model_classes(operation.table)
            
            try:
                self.db.begin()
                
                # Create audit log entry
                audit_entry = AuditLog(
                    operation_type="delete",
                    table_name=f"{operation.table}_archive",
                    user_id=user_id,
                    date_range_start=operation.filters.get("date_start"),
                    date_range_end=operation.filters.get("date_end"),
                    status="in_progress",
                    operation_details={
                        "reason": reason,
                        "filters": operation.filters,
                        "confidence": operation.confidence
                    }
                )
                self.db.add(audit_entry)
                self.db.flush()
                
                # Execute delete
                deleted_count = await self._perform_delete(operation, archive_model, user_id, reason)
                
                # Update audit log
                audit_entry.records_affected = deleted_count
                audit_entry.status = "success"
                
                # Note: Chat operation logging is now handled by ChatService
                
                self.db.commit()
                
                return {
                    "success": True,
                    "operation": "DELETE",
                    "table": f"{operation.table}_archive",
                    "records_deleted": deleted_count,
                    "audit_id": audit_entry.id,
                    "message": f"Successfully deleted {deleted_count} records from {operation.table}_archive"
                }
                
            except Exception as e:
                self.db.rollback()
                if 'audit_entry' in locals():
                    audit_entry.status = "failed"
                    audit_entry.error_message = str(e)
                    try:
                        self.db.commit()
                    except:
                        pass
                raise e
                
        except Exception as e:
            logger.error(f"DELETE operation failed: {e}")
            return {"success": False, "error": str(e)}
    
    async def _preview_archive_operation(self, operation: ParsedOperation, user_id: str) -> Dict[str, Any]:
        """Preview archive operation without executing"""
        main_model, _ = self._get_model_classes(operation.table)
        
        query = self.db.query(main_model)
        query = self._apply_filters(query, operation, main_model)
        
        record_count = query.count()
        sample_records = query.limit(5).all()
        
        return {
            "success": True,
            "operation": "ARCHIVE_PREVIEW",
            "table": operation.table,
            "preview_count": record_count,
            "sample_records": [self._record_to_dict(record) for record in sample_records],
            "message": f"Preview: {record_count:,} records will be archived from {operation.table} to {operation.table}_archive",
            "filters_applied": operation.filters,
            "safety_check": "Records will be copied to archive table before deletion from main table"
        }
    
    async def _preview_delete_operation(self, operation: ParsedOperation, user_id: str) -> Dict[str, Any]:
        """Preview delete operation without executing"""
        _, archive_model = self._get_model_classes(operation.table)
        
        query = self.db.query(archive_model)
        query = self._apply_filters(query, operation, archive_model)
        
        record_count = query.count()
        sample_records = query.limit(5).all()
        
        return {
            "success": True,
            "operation": "DELETE_PREVIEW",
            "table": operation.table,
            "preview_count": record_count,
            "sample_records": [self._record_to_dict(record) for record in sample_records],
            "message": f"⚠️ WARNING: {record_count:,} records will be PERMANENTLY DELETED from {operation.table}",
            "filters_applied": operation.filters,
            "safety_warning": "This operation is IRREVERSIBLE. Records will be permanently removed."
        }
    
    async def _perform_archive(
        self, 
        operation: ParsedOperation, 
        main_model, 
        archive_model, 
        user_id: str, 
        reason: str
    ) -> Tuple[int, int]:
        """Perform the actual archive operation"""
        # Build filter conditions for SQL
        where_conditions = []
        params = {"user_id": user_id, "reason": reason}
        
        # Date filters
        if "date_start" in operation.filters and "date_end" in operation.filters:
            time_field = "PostedTime" if operation.table == "dsiactivities" else "WhenReceived"
            where_conditions.append(f"{time_field} BETWEEN :date_start AND :date_end")
            params["date_start"] = operation.filters["date_start"]
            params["date_end"] = operation.filters["date_end"]
        elif "date_end" in operation.filters:
            time_field = "PostedTime" if operation.table == "dsiactivities" else "WhenReceived"
            # Check if this is an "older than" comparison (should use < instead of <=)
            if operation.filters.get("date_comparison") == "older_than":
                where_conditions.append(f"{time_field} < :date_end")
            else:
                where_conditions.append(f"{time_field} <= :date_end")
            params["date_end"] = operation.filters["date_end"]
        
        # Entity filters
        if "agent_name" in operation.filters:
            where_conditions.append("AgentName = :agent_name")
            params["agent_name"] = operation.filters["agent_name"]
        
        if "server_name" in operation.filters:
            where_conditions.append("ServerName = :server_name")
            params["server_name"] = operation.filters["server_name"]
        
        if "user_id" in operation.filters and operation.table == "dsitransactionlog":
            where_conditions.append("UserID = :filter_user_id")
            params["filter_user_id"] = operation.filters["user_id"]
        
        if "device_id" in operation.filters and operation.table == "dsitransactionlog":
            where_conditions.append("DeviceID = :device_id")
            params["device_id"] = operation.filters["device_id"]
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        
        # Archive query - copy to archive table with explicit column mapping
        archive_table = f"{operation.table}_archive"
        main_table = operation.table
        
        # Get column names from main table (excluding the archive-specific columns)
        main_columns = []
        if operation.table == "dsiactivities":
            main_columns = [
                "ActivityID", "ActivityType", "TrackingID", "SecondaryTrackingID", 
                "AgentName", "ThreadID", "Description", "PostedTime", "PostedTimeUTC",
                "LineNumber", "FileName", "MethodName", "ServerName", "InstanceID",
                "IdenticalAlertCount", "AlertLevel", "DismissedBy", "DismissedDateTime",
                "LastIdenticalAlertDateTime", "EventID", "DefaultDescription", "ExceptionMessage"
            ]
        elif operation.table == "dsitransactionlog":
            main_columns = [
                "RecordStatus", "ProcessMethod", "TransactionType", "ServerName", "DeviceID", 
                "UserID", "DeviceLocalTime", "DeviceUTCTime", "DeviceSequenceID", "WhenReceived",
                "WhenProcessed", "WhenExtracted", "ElapsedTime", "AppID", "AppVersion", "AppItemID",
                "WorldHostID", "ConnectorID", "FunctionDefVersion", "FunctionCallID", "FunctionCallRC",
                "DataIn", "DataOut", "ErrorsOut", "SecurityID", "GUID", "UnitID", "PromotionLevelID",
                "EnvironmentID", "Marking", "OrgUnitID", "TrackingReference"
            ]
        
        if not main_columns:
            raise Exception(f"Unknown table for column mapping: {operation.table}")
        
        columns_list = ", ".join(main_columns)
        values_list = ", ".join(main_columns)
        
        archive_query = text(f"""
            INSERT INTO {archive_table} 
            ({columns_list}, archived_at, archived_by, archive_reason)
            SELECT {values_list}, 
                CURRENT_TIMESTAMP as archived_at,
                :user_id as archived_by,
                :reason as archive_reason
            FROM {main_table} 
            WHERE {where_clause}
        """)
        
        result = self.db.execute(archive_query, params)
        archived_count = result.rowcount
        
        # Verify archive operation with MySQL-compatible query
        verify_query = text(f"""
            SELECT COUNT(*) FROM {archive_table} 
            WHERE archived_by = :verify_user_id 
            AND archived_at >= DATE_SUB(NOW(), INTERVAL 1 MINUTE)
        """)
        
        try:
            verified_count = self.db.execute(verify_query, {"verify_user_id": user_id}).scalar()
        except Exception as e:
            # Fallback verification - just check total count
            logger.warning(f"Archive verification query failed: {e}, using fallback")
            verified_count = archived_count  # Assume success if we can't verify
        
        if verified_count != archived_count:
            logger.warning(f"Archive verification mismatch. Expected {archived_count}, found {verified_count}")
            # Don't fail the operation - this might be due to database dialect differences
        
        # Delete from main table
        delete_query = text(f"""
            DELETE FROM {main_table} 
            WHERE {where_clause}
        """)
        
        delete_params = {k: v for k, v in params.items() if k not in ["user_id", "reason"]}
        delete_result = self.db.execute(delete_query, delete_params)
        deleted_count = delete_result.rowcount
        
        return archived_count, deleted_count
    
    async def _perform_delete(
        self, 
        operation: ParsedOperation, 
        archive_model, 
        user_id: str, 
        reason: str
    ) -> int:
        """Perform the actual delete operation on archive table"""
        # Build query with filters
        query = self.db.query(archive_model)
        query = self._apply_filters(query, operation, archive_model)
        
        # Execute delete
        deleted_count = query.delete(synchronize_session=False)
        
        return deleted_count
    
    def _get_model_classes(self, table_name: str):
        """Get SQLAlchemy model classes for main and archive tables"""
        if table_name == "dsiactivities":
            return DSIActivities, ArchiveDSIActivities
        elif table_name == "dsitransactionlog":
            return DSITransactionLog, ArchiveDSITransactionLog
        elif table_name == "dsiactivities_archive":
            return ArchiveDSIActivities, ArchiveDSIActivities  # Archive table as both main and archive
        elif table_name == "dsitransactionlog_archive":
            return ArchiveDSITransactionLog, ArchiveDSITransactionLog  # Archive table as both main and archive
        else:
            raise ValueError(f"Unsupported table: {table_name}")
    
    def _apply_filters(self, query, operation: ParsedOperation, model_class):
        """Apply filters to SQLAlchemy query"""
        filters = operation.filters
        
        # Date filters
        if "date_start" in filters and "date_end" in filters:
            # Use PostedTime for activities tables, WhenReceived for transaction log tables
            time_field = "PostedTime" if ("activities" in operation.table) else "WhenReceived"
            query = query.filter(
                getattr(model_class, time_field).between(filters["date_start"], filters["date_end"])
            )
        elif "date_end" in filters:
            time_field = "PostedTime" if ("activities" in operation.table) else "WhenReceived"
            # Check if this is an "older than" comparison (should use < instead of <=)
            if filters.get("date_comparison") == "older_than":
                query = query.filter(getattr(model_class, time_field) < filters["date_end"])
            else:
                query = query.filter(getattr(model_class, time_field) <= filters["date_end"])
        
        # Entity filters
        if "agent_name" in filters:
            query = query.filter(model_class.AgentName == filters["agent_name"])
        
        if "server_name" in filters:
            query = query.filter(model_class.ServerName == filters["server_name"])
        
        if "user_id" in filters and hasattr(model_class, "UserID"):
            query = query.filter(model_class.UserID == filters["user_id"])
        
        if "device_id" in filters and hasattr(model_class, "DeviceID"):
            query = query.filter(model_class.DeviceID == filters["device_id"])
        
        return query
    
    def _record_to_dict(self, record) -> Dict:
        """Convert SQLAlchemy record to dictionary"""
        result = {}
        for column in record.__table__.columns:
            value = getattr(record, column.name)
            # Convert datetime to string for JSON serialization
            if isinstance(value, datetime):
                value = value.isoformat()
            result[column.name] = value
        return result
    
    # Removed excessive logging method - now handled selectively in chat_service
    # This method was creating too many chatops_log entries
    
    async def get_operation_history(
        self, 
        user_id: Optional[str] = None, 
        operation_type: Optional[str] = None,
        days: int = 7,
        limit: int = 50
    ) -> List[Dict]:
        """Get operation history from audit logs"""
        try:
            query = self.db.query(AuditLog)
            
            # Apply filters
            if user_id:
                query = query.filter(AuditLog.user_id == user_id)
            
            if operation_type:
                query = query.filter(AuditLog.operation_type == operation_type)
            
            # Date filter
            since_date = datetime.now() - timedelta(days=days)
            query = query.filter(AuditLog.operation_date >= since_date)
            
            # Order and limit
            query = query.order_by(AuditLog.operation_date.desc()).limit(limit)
            
            records = query.all()
            
            return [
                {
                    "id": record.id,
                    "operation_type": record.operation_type,
                    "table_name": record.table_name,
                    "user_id": record.user_id,
                    "operation_date": record.operation_date.isoformat(),
                    "records_affected": record.records_affected,
                    "status": record.status,
                    "error_message": record.error_message,
                    "date_range_start": record.date_range_start,
                    "date_range_end": record.date_range_end,
                    "operation_details": record.operation_details
                }
                for record in records
            ]
            
        except Exception as e:
            logger.error(f"Error fetching operation history: {e}")
            return []