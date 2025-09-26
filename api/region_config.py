"""Region configuration management API endpoints"""
from fastapi import APIRouter, HTTPException, Depends, Header
from sqlalchemy.orm import Session
from typing import Optional, List
from pydantic import BaseModel

from database import get_db
from services.region_config_service import get_region_config_service
from services.auth_service import AuthService

router = APIRouter(prefix="/region-config", tags=["region-config"])
auth_service = AuthService()

# Pydantic schemas for region configuration
class RegionConfigCreate(BaseModel):
    region: str
    host: str
    port: int
    username: str
    password: str
    database_name: str
    connection_notes: Optional[str] = None

class RegionConfigUpdate(BaseModel):
    host: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None
    database_name: Optional[str] = None
    is_active: Optional[bool] = None
    connection_notes: Optional[str] = None

class RegionConfigResponse(BaseModel):
    id: int
    region: str
    host: str
    port: int
    username: str
    database_name: str
    connection_notes: Optional[str]
    is_active: bool
    is_connected: bool
    last_connected_at: Optional[str]
    created_at: str
    updated_at: Optional[str]

    class Config:
        from_attributes = True

class ConnectionTestResponse(BaseModel):
    success: bool
    message: str

def require_admin_auth(authorization: Optional[str] = Header(None)):
    """Verify admin authentication"""
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header required")
    
    try:
        user_info = auth_service.verify_token(authorization.replace("Bearer ", ""))
        if user_info.get("role") != "Admin":
            raise HTTPException(status_code=403, detail="Admin access required")
        return user_info
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

@router.post("/", response_model=RegionConfigResponse)
async def create_region_config(
    config_data: RegionConfigCreate,
    db: Session = Depends(get_db),
    user_info: dict = Depends(require_admin_auth)
):
    """Create a new region configuration"""
    try:
        region_config_service = get_region_config_service()
        
        config = region_config_service.create_region_config(
            db=db,
            region=config_data.region,
            host=config_data.host,
            port=config_data.port,
            username=config_data.username,
            password=config_data.password,
            database_name=config_data.database_name,
            connection_notes=config_data.connection_notes
        )
        
        return config
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create region configuration: {str(e)}")

@router.get("/", response_model=List[RegionConfigResponse])
async def get_region_configs(
    include_inactive: bool = False,
    db: Session = Depends(get_db),
    user_info: dict = Depends(require_admin_auth)
):
    """Get all region configurations"""
    try:
        region_config_service = get_region_config_service()
        configs = region_config_service.get_all_region_configs(db, include_inactive)
        return configs
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get region configurations: {str(e)}")

@router.get("/{region}", response_model=RegionConfigResponse)
async def get_region_config(
    region: str,
    db: Session = Depends(get_db),
    user_info: dict = Depends(require_admin_auth)
):
    """Get configuration for a specific region"""
    try:
        region_config_service = get_region_config_service()
        config = region_config_service.get_region_config(db, region)
        
        if not config:
            raise HTTPException(status_code=404, detail=f"Region {region} not found")
        
        return config
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get region configuration: {str(e)}")

@router.put("/{region}", response_model=RegionConfigResponse)
async def update_region_config(
    region: str,
    config_data: RegionConfigUpdate,
    db: Session = Depends(get_db),
    user_info: dict = Depends(require_admin_auth)
):
    """Update an existing region configuration"""
    try:
        region_config_service = get_region_config_service()
        
        config = region_config_service.update_region_config(
            db=db,
            region=region,
            host=config_data.host,
            port=config_data.port,
            username=config_data.username,
            password=config_data.password,
            database_name=config_data.database_name,
            is_active=config_data.is_active,
            connection_notes=config_data.connection_notes
        )
        
        if not config:
            raise HTTPException(status_code=404, detail=f"Region {region} not found")
        
        return config
        
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update region configuration: {str(e)}")

@router.delete("/{region}")
async def delete_region_config(
    region: str,
    db: Session = Depends(get_db),
    user_info: dict = Depends(require_admin_auth)
):
    """Delete a region configuration (soft delete)"""
    try:
        region_config_service = get_region_config_service()
        success = region_config_service.delete_region_config(db, region)
        
        if not success:
            raise HTTPException(status_code=404, detail=f"Region {region} not found")
        
        return {"message": f"Region {region} configuration deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete region configuration: {str(e)}")

@router.post("/{region}/test", response_model=ConnectionTestResponse)
async def test_region_connection(
    region: str,
    db: Session = Depends(get_db),
    user_info: dict = Depends(require_admin_auth)
):
    """Test database connection for a region"""
    try:
        region_config_service = get_region_config_service()
        success, message = region_config_service.test_region_connection(db, region)
        
        return ConnectionTestResponse(success=success, message=message)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to test connection: {str(e)}")