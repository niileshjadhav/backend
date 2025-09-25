"""Audit model"""
from sqlalchemy import Column, Integer, String, TIMESTAMP, text, JSON
from database import Base

class AuditLog(Base):
    __tablename__ = "audit_log"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    operation_type = Column(String(50), nullable=False)
    table_name = Column(String(50), nullable=False)
    user_id = Column(String(64), nullable=False)
    operation_date = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))
    date_range_start = Column(String(14))
    date_range_end = Column(String(14))
    records_affected = Column(Integer)
    status = Column(String(20), nullable=False)  # 'success', 'failed', 'partial'
    error_message = Column(String(1000))
    operation_details = Column(JSON)  # Additional details as JSON
    session_id = Column(String(100))  # For tracking chat sessions