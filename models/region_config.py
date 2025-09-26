"""Region Configuration Model"""
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text
from sqlalchemy.sql import func
from database import Base


class RegionConfig(Base):
    """Model for storing region-based database connection configurations"""
    __tablename__ = "region_config"
    
    id = Column(Integer, primary_key=True, index=True)
    region = Column(String(10), unique=True, nullable=False, index=True)
    connection_string = Column(Text, nullable=False)  # Full database connection string
    is_active = Column(Boolean, default=True, nullable=False)
    is_connected = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    last_connected_at = Column(DateTime(timezone=True), nullable=True)
    connection_notes = Column(Text, nullable=True)  # For storing any additional connection info
    
    def __repr__(self):
        return f"<RegionConfig(region='{self.region}', is_active={self.is_active})>"
    
    def get_database_url(self) -> str:
        """Get database URL from stored connection string"""
        return self.connection_string
    
    def to_dict(self) -> dict:
        """Convert to dictionary for API responses (without sensitive data)"""
        return {
            "id": self.id,
            "region": self.region,
            "connection_string": self.connection_string,
            "is_active": self.is_active,
            "is_connected": self.is_connected,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "last_connected_at": self.last_connected_at.isoformat() if self.last_connected_at else None,
            "connection_notes": self.connection_notes
        }
    
    def to_dict_secure(self) -> dict:
        """Convert to dictionary without connection string for secure API responses"""
        result = self.to_dict()  
        result.pop("connection_string", None)  # Remove connection string from response
        return result