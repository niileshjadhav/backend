"""Job Logs model for tracking database operations"""
from sqlalchemy import Column, Integer, String, Text, DateTime, BigInteger
from sqlalchemy.sql import func
from database import Base

class JobLogs(Base):
    __tablename__ = "job_logs"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    schema_name = Column(String(100), nullable=True)  # Optional, for multi-schema jobs
    job_type = Column(String(50), nullable=False)     # DELETE / ARCHIVE / OTHER
    table_name = Column(String(100), nullable=False)  # Table affected
    status = Column(String(20), nullable=False)       # IN_PROGRESS / SUCCESS / FAILED
    reason = Column(Text, nullable=True)              # Success message or failure reason
    records_affected = Column(Integer, default=0)     # Number of rows affected
    started_at = Column(DateTime, default=func.current_timestamp())  # When job started
    finished_at = Column(DateTime, nullable=True)     # When job finished
    
    def __repr__(self):
        return f"<JobLogs(id={self.id}, job_type='{self.job_type}', table_name='{self.table_name}', status='{self.status}')>"