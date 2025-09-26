"""Region Configuration Service"""
import logging
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime
from models.region_config import RegionConfig
from database import get_db
from shared.enums import Region
import bcrypt

logger = logging.getLogger(__name__)


class RegionConfigService:
    """Service for managing region database configurations"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def _encrypt_password(self, password: str) -> str:
        """Encrypt password using bcrypt"""
        salt = bcrypt.gensalt()
        hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
        return hashed.decode('utf-8')
    
    def _verify_password(self, password: str, hashed: str) -> bool:
        """Verify password against hash"""
        return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))
    
    def create_region_config(
        self, 
        db: Session,
        region: str,
        host: str,
        port: int,
        username: str,
        password: str,
        database_name: str,
        connection_notes: Optional[str] = None
    ) -> RegionConfig:
        """Create a new region configuration"""
        try:
            # Validate region format (allow any non-empty alphanumeric string)
            if not region or not isinstance(region, str) or not region.strip():
                raise ValueError(f"Invalid region: region must be a non-empty string")
            
            # Normalize region name (uppercase, strip whitespace)
            region = region.strip().upper()
            
            # Check if region already exists
            existing = db.query(RegionConfig).filter(RegionConfig.region == region).first()
            if existing:
                raise ValueError(f"Region {region} already exists")
            
            # Store password in plain text for database connections
            # Note: These are internal database credentials, not user passwords
            
            # Create new config
            config = RegionConfig(
                region=region,
                host=host,
                port=port,
                username=username,
                password=password,  # Store as plain text for database connections
                database_name=database_name,
                connection_notes=connection_notes,
                is_active=True
            )
            
            db.add(config)
            db.commit()
            db.refresh(config)
            
            self.logger.info(f"Created region configuration for {region}")
            return config
            
        except Exception as e:
            db.rollback()
            self.logger.error(f"Failed to create region config for {region}: {e}")
            raise
    
    def update_region_config(
        self,
        db: Session,
        region: str,
        host: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        database_name: Optional[str] = None,
        is_active: Optional[bool] = None,
        connection_notes: Optional[str] = None
    ) -> Optional[RegionConfig]:
        """Update an existing region configuration"""
        try:
            config = db.query(RegionConfig).filter(RegionConfig.region == region).first()
            if not config:
                raise ValueError(f"Region {region} not found")
            
            # Update fields if provided
            if host is not None:
                config.host = host
            if port is not None:
                config.port = port
            if username is not None:
                config.username = username
            if password is not None:
                config.password = password  # Store as plain text for database connections
            if database_name is not None:
                config.database_name = database_name
            if is_active is not None:
                config.is_active = is_active
            if connection_notes is not None:
                config.connection_notes = connection_notes
            
            config.updated_at = datetime.utcnow()
            
            db.commit()
            db.refresh(config)
            
            self.logger.info(f"Updated region configuration for {region}")
            return config
            
        except Exception as e:
            db.rollback()
            self.logger.error(f"Failed to update region config for {region}: {e}")
            raise
    
    def get_region_config(self, db: Session, region: str) -> Optional[RegionConfig]:
        """Get configuration for a specific region"""
        try:
            return db.query(RegionConfig).filter(
                RegionConfig.region == region,
                RegionConfig.is_active == True
            ).first()
        except Exception as e:
            self.logger.error(f"Failed to get region config for {region}: {e}")
            return None
    
    def get_all_region_configs(self, db: Session, include_inactive: bool = False) -> List[RegionConfig]:
        """Get all region configurations"""
        try:
            query = db.query(RegionConfig)
            if not include_inactive:
                query = query.filter(RegionConfig.is_active == True)
            
            return query.order_by(RegionConfig.region).all()
        except Exception as e:
            self.logger.error(f"Failed to get region configs: {e}")
            return []
    
    def delete_region_config(self, db: Session, region: str) -> bool:
        """Delete a region configuration (soft delete by setting is_active=False)"""
        try:
            config = db.query(RegionConfig).filter(RegionConfig.region == region).first()
            if not config:
                return False
            
            config.is_active = False
            config.updated_at = datetime.utcnow()
            
            db.commit()
            
            self.logger.info(f"Deleted region configuration for {region}")
            return True
            
        except Exception as e:
            db.rollback()
            self.logger.error(f"Failed to delete region config for {region}: {e}")
            return False
    
    def get_database_url(self, db: Session, region: str) -> Optional[str]:
        """Get database URL for a region"""
        config = self.get_region_config(db, region)
        if not config:
            return None
        
        return config.get_database_url()
    
    def test_region_connection(self, db: Session, region: str) -> tuple[bool, str]:
        """Test database connection for a region"""
        try:
            config = self.get_region_config(db, region)
            if not config:
                return False, f"No configuration found for region {region}"
            
            # Create a test connection
            from sqlalchemy import create_engine
            
            database_url = config.get_database_url()
            engine = create_engine(database_url, pool_pre_ping=True)
            
            # Test the connection
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).fetchone()
                if result:
                    # Update last connected time
                    config.last_connected_at = datetime.utcnow()
                    config.is_connected = True
                    db.commit()
                    
                    engine.dispose()  # Clean up the test engine
                    return True, f"Connection to {region} successful"
                else:
                    config.is_connected = False
                    db.commit()
                    return False, f"Connection test failed for {region}"
                    
        except Exception as e:
            self.logger.error(f"Connection test failed for region {region}: {e}")
            if 'config' in locals():
                config.is_connected = False
                db.commit()
            return False, f"Connection test failed: {str(e)}"
    
    def update_connection_status(self, db: Session, region: str, is_connected: bool):
        """Update the connection status for a region"""
        try:
            config = self.get_region_config(db, region)
            if config:
                config.is_connected = is_connected
                if is_connected:
                    config.last_connected_at = datetime.utcnow()
                db.commit()
        except Exception as e:
            self.logger.error(f"Failed to update connection status for {region}: {e}")
    
    def get_available_regions(self, db: Session) -> List[str]:
        """Get list of available (configured and active) regions"""
        try:
            configs = self.get_all_region_configs(db)
            return [config.region for config in configs]
        except Exception as e:
            self.logger.error(f"Failed to get available regions: {e}")
            return []


# Global service instance
region_config_service = RegionConfigService()


def get_region_config_service() -> RegionConfigService:
    """Get the global region config service instance"""
    return region_config_service