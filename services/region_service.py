"""Region and database connection management service"""
import logging
import os
from typing import Dict, Optional, Tuple
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
import asyncio
from contextlib import asynccontextmanager
from shared.enums import Region, TableName
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class RegionService:
    """Service for managing regional database connections"""
    
    def __init__(self):
        # Get region-specific database URLs from environment variables
        self.region_database_urls = {
            Region.APAC: os.getenv("DATABASE_URL_APAC"),
            Region.US: os.getenv("DATABASE_URL_US"),
            Region.EU: os.getenv("DATABASE_URL_EU"),
            Region.MEA: os.getenv("DATABASE_URL_MEA")
        }
        
        self.engines: Dict[Region, Engine] = {}
        self.session_makers: Dict[Region, sessionmaker] = {}
        self.connection_status: Dict[Region, bool] = {}
    
    def get_available_regions(self) -> list[str]:
        """Get list of available regions"""
        return [region.value for region in Region]
    
    def is_region_valid(self, region: str) -> bool:
        """Check if a region is valid"""
        try:
            Region(region)
            return True
        except ValueError:
            return False
    
    def get_valid_regions(self) -> list[str]:
        """Get list of valid regions (same as available)"""
        return self.get_available_regions()
    
    def get_default_region(self) -> str:
        """Get the default region"""
        # Return the first available region as default
        available = self.get_available_regions()
        return available[0] if available else Region.US.value
    
    def set_current_region(self, region: str):
        """Set the current region (for logging/tracking purposes)"""
        if self.is_region_valid(region):
            # Just log the current region, no need to store state
            logger.info(f"Current region context: {region}")
        else:
            logger.warning(f"Attempted to set invalid region: {region}")
    
    async def connect_to_region(self, region: str) -> Tuple[bool, str]:
        """Connect to a specific region database"""
        try:
            region_enum = Region(region)
            database_url = self.region_database_urls[region_enum]
            
            if not database_url:
                return False, f"Database URL not configured for region {region}"
            
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
                    self.engines[region_enum] = engine
                    self.session_makers[region_enum] = sessionmaker(bind=engine)
                    self.connection_status[region_enum] = True
                    
                    logger.info(f"Successfully connected to region: {region}")
                    return True, f"Connected to {region} region successfully"
                
        except Exception as e:
            logger.error(f"Failed to connect to region {region}: {e}")
            self.connection_status[Region(region)] = False
            return False, f"Failed to connect to {region}: {str(e)}"
    
    async def disconnect_from_region(self, region: str) -> Tuple[bool, str]:
        """Disconnect from a specific region database"""
        try:
            region_enum = Region(region)
            
            if region_enum in self.engines:
                self.engines[region_enum].dispose()
                del self.engines[region_enum]
                del self.session_makers[region_enum]
                
            self.connection_status[region_enum] = False
            
            logger.info(f"Disconnected from region: {region}")
            return True, f"Disconnected from {region} region successfully"
            
        except Exception as e:
            logger.error(f"Failed to disconnect from region {region}: {e}")
            return False, f"Failed to disconnect from {region}: {str(e)}"
    
    def get_connection_status(self, region: str = None) -> Dict[str, bool]:
        """Get connection status for regions"""
        if region:
            try:
                region_enum = Region(region)
                return {region: self.connection_status.get(region_enum, False)}
            except ValueError:
                return {region: False}
        
        return {
            region.value: self.connection_status.get(region, False) 
            for region in Region
        }
    
    def get_session(self, region: str):
        """Get database session for a specific region"""
        try:
            region_enum = Region(region)
            if region_enum not in self.session_makers:
                raise ValueError(f"Not connected to region: {region}")
            
            return self.session_makers[region_enum]()
            
        except Exception as e:
            logger.error(f"Failed to get session for region {region}: {e}")
            raise
    
    def is_connected(self, region: str) -> bool:
        """Check if connected to a specific region"""
        try:
            region_enum = Region(region)
            return self.connection_status.get(region_enum, False)
        except ValueError:
            return False
    
    async def test_connection(self, region: str) -> Tuple[bool, str, Dict]:
        """Test connection to a region and return detailed status"""
        try:
            region_enum = Region(region)
            
            if region_enum not in self.engines:
                return False, f"Not connected to {region}", {}
            
            engine = self.engines[region_enum]
            
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
                        
                        # Also check archive table
                        archive_table = f"archive_{table}"
                        result = conn.execute(text(f"SELECT COUNT(*) FROM {archive_table}")).fetchone()
                        tables_info[archive_table] = result[0] if result else 0
                        
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