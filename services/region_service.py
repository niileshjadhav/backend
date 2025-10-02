"""Region and database connection management service"""
import logging
import os
from typing import Dict, Optional, Tuple
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
import asyncio
from contextlib import asynccontextmanager
from shared.enums import TableName
from database import get_db
from services.region_config_service import get_region_config_service
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class RegionService:
    """Service for managing regional database connections"""
    
    def __init__(self):
        self.engines: Dict[str, Engine] = {}
        self.session_makers: Dict[str, sessionmaker] = {}
        self.connection_status: Dict[str, bool] = {}
        self.region_config_service = get_region_config_service()
    
    def _get_database_url_for_region(self, region: str) -> Optional[str]:
        """Get database URL for a region from database configuration"""
        try:
            # Get a database session to query region configs
            db = next(get_db())
            try:
                return self.region_config_service.get_database_url(db, region)
            finally:
                db.close()
        except Exception as e:
            logger.error(f"Failed to get database URL for region {region}: {e}")
            return None
    
    def get_available_regions(self) -> list[str]:
        """Get list of available regions from database configuration"""
        try:
            db = next(get_db())
            try:
                return self.region_config_service.get_available_regions(db)
            finally:
                db.close()
        except Exception as e:
            logger.error(f"Failed to get available regions: {e}")
            # Fallback to default regions if database is not available
            return ["US", "EU", "APAC", "MEA"]
    
    def is_region_valid(self, region: str) -> bool:
        """Check if a region is valid"""
        return region in self.get_available_regions()
    
    def get_valid_regions(self) -> list[str]:
        """Get list of valid regions (same as available)"""
        return self.get_available_regions()
    
    def get_default_region(self) -> str:
        """Get the default region"""
        # Return the first available region as default
        available = self.get_available_regions()
        return available[0] if available else "US"
    
    def set_current_region(self, region: str):
        """Set the current region (for logging/tracking purposes)"""
        if self.is_region_valid(region):
            self.current_region = region
        else:
            logger.warning(f"Attempted to set invalid region: {region}")
    
    def get_current_region(self) -> Optional[str]:
        """Get the current region"""
        return getattr(self, 'current_region', None)
    
    async def connect_to_region(self, region: str) -> Tuple[bool, str]:
        """Connect to a specific region database"""
        try:
            database_url = self._get_database_url_for_region(region)
            
            if not database_url:
                return False, f"Database URL not configured for region {region}. Please configure region settings first."
            
            # Create engine
            engine = create_engine(
                database_url,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False
            )
            
            # Test connection
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).fetchone()
                if result:
                    self.engines[region] = engine
                    self.session_makers[region] = sessionmaker(bind=engine)
                    self.connection_status[region] = True
                    
                    # Update connection status in database
                    db = next(get_db())
                    try:
                        self.region_config_service.update_connection_status(db, region, True)
                    finally:
                        db.close()
                    
                    return True, f"Connected to {region} region successfully"
                
        except Exception as e:
            logger.error(f"Failed to connect to region {region}: {e}")
            self.connection_status[region] = False
            
            # Update connection status in database
            try:
                db = next(get_db())
                try:
                    self.region_config_service.update_connection_status(db, region, False)
                finally:
                    db.close()
            except:
                pass  # Don't fail if we can't update status
                    
            return False, f"Failed to connect to {region}: {str(e)}"
    
    async def disconnect_from_region(self, region: str) -> Tuple[bool, str]:
        """Disconnect from a specific region database"""
        try:
            if region in self.engines:
                self.engines[region].dispose()
                del self.engines[region]
                del self.session_makers[region]
                
            self.connection_status[region] = False
            
            # Update connection status in database
            try:
                db = next(get_db())
                try:
                    self.region_config_service.update_connection_status(db, region, False)
                finally:
                    db.close()
            except:
                pass  # Don't fail if we can't update status
            
            return True, f"Disconnected from {region} region successfully"
            
        except Exception as e:
            logger.error(f"Failed to disconnect from region {region}: {e}")
            return False, f"Failed to disconnect from {region}: {str(e)}"
    
    def get_connection_status(self, region: str = None) -> Dict[str, bool]:
        """Get connection status for regions"""
        if region:
            return {region: self.connection_status.get(region, False)}
        
        # Get all available regions from the database configuration
        available_regions = self.get_available_regions()
        return {
            region: self.connection_status.get(region, False) 
            for region in available_regions
        }
    
    def get_session(self, region: str):
        """Get database session for a specific region"""
        try:
            if region not in self.session_makers:
                raise ValueError(f"Not connected to region: {region}")
            
            return self.session_makers[region]()
            
        except Exception as e:
            logger.error(f"Failed to get session for region {region}: {e}")
            raise
    
    def is_connected(self, region: str) -> bool:
        """Check if connected to a specific region"""
        return self.connection_status.get(region, False)
    
    async def test_connection(self, region: str) -> Tuple[bool, str, Dict]:
        """Test connection to a region and return detailed status"""
        try:
            if region not in self.engines:
                return False, f"Not connected to {region}", {}
            
            engine = self.engines[region]
            
            # Test query
            with engine.connect() as conn:
                # Test basic connectivity
                conn.execute(text("SELECT 1"))
                
                # Get table counts
                tables_info = {}
                table_names = ["dsiactivities", "dsitransactionlog"]
                for table in table_names:
                    try:
                        result = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()
                        tables_info[table] = result[0] if result else 0
                        
                        # Also check archive table (use correct naming convention)
                        if table == "dsiactivities":
                            archive_table = "dsiactivitiesarchive"
                        elif table == "dsitransactionlog":
                            archive_table = "dsitransactionlogarchive"
                        else:
                            archive_table = f"{table}archive"  # Fallback for other tables
                        try:
                            result = conn.execute(text(f"SELECT COUNT(*) FROM {archive_table}")).fetchone()
                            tables_info[archive_table] = result[0] if result else 0
                        except Exception as archive_error:
                            logger.warning(f"Archive table {archive_table} does not exist or cannot be queried: {archive_error}")
                            tables_info[archive_table] = 0
                        
                    except Exception as table_error:
                        logger.warning(f"Could not query table {table}: {table_error}")
                        tables_info[table] = "Error"
                
                return True, f"Connection to {region} is healthy", tables_info
                
        except Exception as e:
            logger.error(f"Connection test failed for region {region}: {e}")
            return False, f"Connection test failed: {str(e)}", {}

# Global region service instance
region_service = RegionService()

def get_region_service() -> RegionService:
    """Get the global region service instance"""
    return region_service