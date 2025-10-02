"""Activities model"""
from sqlalchemy import Column, Integer, String, TIMESTAMP, text
from sqlalchemy.ext.declarative import declarative_base
from database import Base

class DSIActivities(Base):
    __tablename__ = "dsiactivities"
    
    SequenceID = Column(Integer, primary_key=True, autoincrement=True)
    ActivityID = Column(String(50))
    ActivityType = Column(String(50))
    TrackingID = Column(String(50))
    SecondaryTrackingID = Column(String(50))
    AgentName = Column(String(50))
    ThreadID = Column(Integer)
    Description = Column(String(2000))
    PostedTime = Column(String(14))
    PostedTimeUTC = Column(String(14))
    LineNumber = Column(Integer)
    FileName = Column(String(50))
    MethodName = Column(String(250))
    ServerName = Column(String(64))
    InstanceID = Column(String(50))
    IdenticalAlertCount = Column(Integer)
    AlertLevel = Column(String(50))
    DismissedBy = Column(String(64))
    DismissedDateTime = Column(String(14))
    LastIdenticalAlertDateTime = Column(String(14))
    EventID = Column(String(50))
    DefaultDescription = Column(String(2000))
    ExceptionMessage = Column(String(2000))


class ArchiveDSIActivities(Base):
    __tablename__ = "dsiactivitiesarchive"
    
    SequenceID = Column(Integer, primary_key=True, autoincrement=True)
    ActivityID = Column(String(50))
    ActivityType = Column(String(50))
    TrackingID = Column(String(50))
    SecondaryTrackingID = Column(String(50))
    AgentName = Column(String(50))
    ThreadID = Column(Integer)
    Description = Column(String(2000))
    PostedTime = Column(String(14))
    PostedTimeUTC = Column(String(14))
    LineNumber = Column(Integer)
    FileName = Column(String(50))
    MethodName = Column(String(250))
    ServerName = Column(String(64))
    InstanceID = Column(String(50))
    IdenticalAlertCount = Column(Integer)
    AlertLevel = Column(String(50))
    DismissedBy = Column(String(64))
    DismissedDateTime = Column(String(14))
    LastIdenticalAlertDateTime = Column(String(14))
    EventID = Column(String(50))
    DefaultDescription = Column(String(2000))
    ExceptionMessage = Column(String(2000))