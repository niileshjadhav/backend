"""Job Logs service for querying job execution history"""
from sqlalchemy.orm import Session
from sqlalchemy import desc, asc, and_, or_, func
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import logging

from models.job_logs import JobLogs

logger = logging.getLogger(__name__)

class JobLogsService:
    """Service for querying job logs with various filters"""
    
    def __init__(self, db_session: Session):
        self.db = db_session
    
    def query_job_logs(
        self,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        order_by: str = "started_at",
        order_direction: str = "desc"
    ) -> Dict[str, Any]:
        """
        Query job logs with various filters
        
        Args:
            filters: Dict containing filter criteria
            limit: Maximum number of records to return
            offset: Number of records to skip
            order_by: Field to order by
            order_direction: 'asc' or 'desc'
            
        Returns:
            Dict containing query results and metadata
        """
        try:
            # Start with base query
            query = self.db.query(JobLogs)
            
            # Apply filters if provided
            if filters:
                query = self._apply_filters(query, filters)
            
            # Get total count before applying pagination
            total_count = query.count()
            
            # Apply ordering
            if hasattr(JobLogs, order_by):
                order_field = getattr(JobLogs, order_by)
                if order_direction.lower() == "desc":
                    query = query.order_by(desc(order_field))
                else:
                    query = query.order_by(asc(order_field))
            
            # Apply pagination
            query = query.offset(offset).limit(limit)
            
            # Execute query
            records = query.all()
            
            # Convert to dictionaries
            result_records = []
            for record in records:
                result_records.append({
                    "id": record.id,
                    "schema_name": record.schema_name,
                    "job_type": record.job_type,
                    "table_name": record.table_name,
                    "status": record.status,
                    "reason": record.reason,
                    "records_affected": record.records_affected,
                    "started_at": record.started_at.isoformat() if record.started_at else None,
                    "finished_at": record.finished_at.isoformat() if record.finished_at else None,
                    "duration_seconds": self._calculate_duration(record.started_at, record.finished_at)
                })
            
            return {
                "success": True,
                "records": result_records,
                "total_count": total_count,
                "returned_count": len(result_records),
                "offset": offset,
                "limit": limit,
                "filters_applied": filters or {},
                "order_by": order_by,
                "order_direction": order_direction
            }
            
        except Exception as e:
            logger.error(f"Error querying job logs: {e}")
            return {
                "success": False,
                "error": str(e),
                "records": [],
                "total_count": 0
            }
    
    def _apply_filters(self, query, filters: Dict[str, Any]):
        """Apply various filters to the query"""
        
        # Status filter
        if "status" in filters:
            status = filters["status"]
            if isinstance(status, list):
                query = query.filter(JobLogs.status.in_(status))
            else:
                query = query.filter(JobLogs.status == status)
        
        # Job type filter
        if "job_type" in filters:
            job_type = filters["job_type"]
            if isinstance(job_type, list):
                query = query.filter(JobLogs.job_type.in_(job_type))
            else:
                query = query.filter(JobLogs.job_type == job_type)
        
        # Table name filter
        if "table_name" in filters:
            table_name = filters["table_name"]
            if isinstance(table_name, list):
                query = query.filter(JobLogs.table_name.in_(table_name))
            else:
                query = query.filter(JobLogs.table_name == table_name)
        
        # Schema name filter
        if "schema_name" in filters:
            schema_name = filters["schema_name"]
            if isinstance(schema_name, list):
                query = query.filter(JobLogs.schema_name.in_(schema_name))
            else:
                query = query.filter(JobLogs.schema_name == schema_name)
        
        # ID filter
        if "id" in filters:
            job_id = filters["id"]
            if isinstance(job_id, list):
                query = query.filter(JobLogs.id.in_(job_id))
            else:
                query = query.filter(JobLogs.id == job_id)
        
        # Records affected range
        if "min_records_affected" in filters:
            query = query.filter(JobLogs.records_affected >= filters["min_records_affected"])
        
        if "max_records_affected" in filters:
            query = query.filter(JobLogs.records_affected <= filters["max_records_affected"])
        
        # Date range filters
        if "started_after" in filters:
            try:
                start_date = datetime.fromisoformat(filters["started_after"].replace('Z', '+00:00'))
                query = query.filter(JobLogs.started_at >= start_date)
            except ValueError:
                logger.warning(f"Invalid started_after date format: {filters['started_after']}")
        
        if "started_before" in filters:
            try:
                end_date = datetime.fromisoformat(filters["started_before"].replace('Z', '+00:00'))
                query = query.filter(JobLogs.started_at <= end_date)
            except ValueError:
                logger.warning(f"Invalid started_before date format: {filters['started_before']}")
        
        if "finished_after" in filters:
            try:
                start_date = datetime.fromisoformat(filters["finished_after"].replace('Z', '+00:00'))
                query = query.filter(JobLogs.finished_at >= start_date)
            except ValueError:
                logger.warning(f"Invalid finished_after date format: {filters['finished_after']}")
        
        if "finished_before" in filters:
            try:
                end_date = datetime.fromisoformat(filters["finished_before"].replace('Z', '+00:00'))
                query = query.filter(JobLogs.finished_at <= end_date)
            except ValueError:
                logger.warning(f"Invalid finished_before date format: {filters['finished_before']}")
        
        # Date range shortcuts
        if "date_range" in filters:
            date_range = filters["date_range"].lower()
            now = datetime.now()
            
            if date_range == "today":
                start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
                query = query.filter(JobLogs.started_at >= start_of_day)
            
            elif date_range == "yesterday":
                yesterday = now - timedelta(days=1)
                start_of_yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
                start_of_today = now.replace(hour=0, minute=0, second=0, microsecond=0)
                query = query.filter(
                    and_(
                        JobLogs.started_at >= start_of_yesterday,
                        JobLogs.started_at < start_of_today
                    )
                )
            
            elif date_range == "this_week":
                start_of_week = now - timedelta(days=now.weekday())
                start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
                query = query.filter(JobLogs.started_at >= start_of_week)
            
            elif date_range == "this_month":
                start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                query = query.filter(JobLogs.started_at >= start_of_month)
            
            elif date_range == "last_7_days":
                seven_days_ago = now - timedelta(days=7)
                query = query.filter(JobLogs.started_at >= seven_days_ago)
            
            elif date_range == "last_30_days":
                thirty_days_ago = now - timedelta(days=30)
                query = query.filter(JobLogs.started_at >= thirty_days_ago)
            
            elif date_range == "last_month":
                # Previous calendar month
                if now.month == 1:
                    # If current month is January, last month is December of previous year
                    start_of_last_month = now.replace(year=now.year-1, month=12, day=1, hour=0, minute=0, second=0, microsecond=0)
                    start_of_current_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                else:
                    start_of_last_month = now.replace(month=now.month-1, day=1, hour=0, minute=0, second=0, microsecond=0)
                    start_of_current_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                
                query = query.filter(
                    and_(
                        JobLogs.started_at >= start_of_last_month,
                        JobLogs.started_at < start_of_current_month
                    )
                )
            
            elif date_range.startswith("from_") and "_to_" in date_range:
                # Handle custom date ranges like "from_9/15/2025_to_9/30/2025"
                try:
                    # Extract start and end dates from the filter
                    parts = date_range.replace("from_", "").split("_to_")
                    if len(parts) == 2:
                        start_date_str = parts[0].replace("_", "/")  # Convert 9_15_2025 to 9/15/2025
                        end_date_str = parts[1].replace("_", "/")    # Convert 9_30_2025 to 9/30/2025
                        
                        # Parse the dates
                        start_date = datetime.strptime(start_date_str, "%m/%d/%Y")
                        end_date = datetime.strptime(end_date_str, "%m/%d/%Y")
                        
                        # Set time boundaries (start of start date to end of end date)
                        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
                        end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
                        
                        query = query.filter(
                            and_(
                                JobLogs.started_at >= start_date,
                                JobLogs.started_at <= end_date
                            )
                        )
                except (ValueError, IndexError) as e:
                    logger.warning(f"Failed to parse custom date range '{date_range}': {e}")
        
        # Text search in reason field
        if "reason_contains" in filters:
            search_term = filters["reason_contains"]
            query = query.filter(JobLogs.reason.ilike(f"%{search_term}%"))
        
        # Failed jobs only
        if filters.get("failed_only", False):
            query = query.filter(JobLogs.status == "FAILED")
        
        # Successful jobs only
        if filters.get("successful_only", False):
            query = query.filter(JobLogs.status == "SUCCESS")
        
        # In progress jobs only
        if filters.get("in_progress_only", False):
            query = query.filter(JobLogs.status == "IN_PROGRESS")
        
        # Jobs with zero records affected
        if filters.get("zero_records_only", False):
            query = query.filter(JobLogs.records_affected == 0)
        
        # Jobs with records affected
        if filters.get("has_records_only", False):
            query = query.filter(JobLogs.records_affected > 0)
        
        return query
    
    def _calculate_duration(self, started_at: datetime, finished_at: datetime) -> Optional[float]:
        """Calculate duration in seconds between start and finish times"""
        if not started_at or not finished_at:
            return None
        
        duration = finished_at - started_at
        return duration.total_seconds()
    
    def get_job_summary_stats(self, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get summary statistics for job logs"""
        try:
            # Start with base query
            query = self.db.query(JobLogs)
            
            # Apply filters if provided
            if filters:
                query = self._apply_filters(query, filters)
            
            # Get various counts
            total_jobs = query.count()
            successful_jobs = query.filter(JobLogs.status == "SUCCESS").count()
            failed_jobs = query.filter(JobLogs.status == "FAILED").count()
            in_progress_jobs = query.filter(JobLogs.status == "IN_PROGRESS").count()
            
            # Get job type breakdown - apply the same filters
            job_type_query = self.db.query(
                JobLogs.job_type,
                func.count(JobLogs.id).label('count')
            )
            if filters:
                job_type_query = self._apply_filters(job_type_query, filters)
            job_type_stats = job_type_query.group_by(JobLogs.job_type).all()
            
            # Get table breakdown - apply the same filters
            table_query = self.db.query(
                JobLogs.table_name,
                func.count(JobLogs.id).label('count')
            )
            if filters:
                table_query = self._apply_filters(table_query, filters)
            table_stats = table_query.group_by(JobLogs.table_name).all()
            
            # Get records affected stats
            records_stats = query.with_entities(
                func.sum(JobLogs.records_affected).label('total_records'),
                func.avg(JobLogs.records_affected).label('avg_records'),
                func.max(JobLogs.records_affected).label('max_records'),
                func.min(JobLogs.records_affected).label('min_records')
            ).first()
            
            return {
                "success": True,
                "summary": {
                    "total_jobs": total_jobs,
                    "successful_jobs": successful_jobs,
                    "failed_jobs": failed_jobs,
                    "in_progress_jobs": in_progress_jobs,
                    "success_rate": round((successful_jobs / total_jobs) * 100, 2) if total_jobs > 0 else 0
                },
                "job_types": [{"job_type": jt.job_type, "count": jt.count} for jt in job_type_stats],
                "tables": [{"table_name": ts.table_name, "count": ts.count} for ts in table_stats],
                "records_stats": {
                    "total_records_affected": int(records_stats.total_records or 0),
                    "average_records_per_job": round(float(records_stats.avg_records or 0), 2),
                    "max_records_in_job": int(records_stats.max_records or 0),
                    "min_records_in_job": int(records_stats.min_records or 0)
                },
                "filters_applied": filters or {}
            }
            
        except Exception as e:
            logger.error(f"Error getting job summary stats: {e}")
            return {
                "success": False,
                "error": str(e)
            }