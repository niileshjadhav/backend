"""ChatOps model"""
from sqlalchemy import Column, Integer, String, TIMESTAMP, text, TEXT, JSON
from database import Base

class ChatOpsLog(Base):
    __tablename__ = "chatops_log"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(100))
    user_id = Column(String(64), nullable=False)
    user_role = Column(String(20), nullable=False)
    region = Column(String(10))  # Store region context (APAC, US, EU, MEA)
    message_type = Column(String(20), nullable=False)  # 'query', 'command', 'response'
    user_message = Column(TEXT)
    bot_response = Column(TEXT)
    operation_type = Column(String(50))  # 'SELECT', 'ARCHIVE', 'DELETE', null
    table_name = Column(String(50))  # dsiactivities, dsitransactionlog, null
    filters_applied = Column(JSON)  # Store filters as JSON
    records_affected = Column(Integer)
    operation_status = Column(String(20))  # 'success', 'failed', 'preview', 'confirmed'
    timestamp = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))
    error_message = Column(String(1000))