"""Region configuration management API endpoints"""
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from typing import List, Dict

from database import get_db
from services.region_config_service import get_region_config_service
from security import get_admin_user
from schemas import (
    RegionConfigCreate,
    RegionConfigUpdate, 
    RegionConfigResponse,
    ConnectionTestResponse
)

router = APIRouter(prefix="/region-config", tags=["region-config"])

@router.post("/", response_model=RegionConfigResponse)
async def create_region_config(
    config_data: RegionConfigCreate,
    db: Session = Depends(get_db),
    admin_user: Dict = Depends(get_admin_user)
):
    """Create a new region configuration"""
    try:
        region_config_service = get_region_config_service()
        
        config = region_config_service.create_region_config(
            db=db,
            region=config_data.region,
            connection_string=config_data.connection_string,
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
    admin_user: Dict = Depends(get_admin_user)
):
    """Get all region configurations"""
    try:
        region_config_service = get_region_config_service()
        configs = region_config_service.get_all_region_configs(db, include_inactive)
        
        # Add is_connected field by checking region service
        from services.region_service import get_region_service
        region_service = get_region_service()
        
        result = []
        for config in configs:
            config_dict = {
                "id": config.id,
                "region": config.region,
                "connection_notes": config.connection_notes,
                "is_active": config.is_active,
                "is_connected": region_service.is_connected(config.region),
                "last_connected_at": config.last_connected_at.isoformat() if config.last_connected_at else None,
                "created_at": config.created_at.isoformat() if config.created_at else None,
                "updated_at": config.updated_at.isoformat() if config.updated_at else None,
            }
            result.append(config_dict)
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get region configurations: {str(e)}")

@router.get("/{region}", response_model=RegionConfigResponse)
async def get_region_config(
    region: str,
    db: Session = Depends(get_db),
    admin_user: Dict = Depends(get_admin_user)
):
    """Get configuration for a specific region"""
    try:
        region_config_service = get_region_config_service()
        config = region_config_service.get_region_config(db, region)
        
        if not config:
            raise HTTPException(status_code=404, detail=f"Region {region} not found")
        
        # Add is_connected field by checking region service
        from services.region_service import get_region_service
        region_service = get_region_service()
        
        result = {
            "id": config.id,
            "region": config.region,
            "connection_notes": config.connection_notes,
            "is_active": config.is_active,
            "is_connected": region_service.is_connected(config.region),
            "last_connected_at": config.last_connected_at.isoformat() if config.last_connected_at else None,
            "created_at": config.created_at.isoformat() if config.created_at else None,
            "updated_at": config.updated_at.isoformat() if config.updated_at else None,
        }
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get region configuration: {str(e)}")

@router.put("/{region}", response_model=RegionConfigResponse)
async def update_region_config(
    region: str,
    config_data: RegionConfigUpdate,
    db: Session = Depends(get_db),
    admin_user: Dict = Depends(get_admin_user)
):
    """Update an existing region configuration"""
    try:
        region_config_service = get_region_config_service()
        
        config = region_config_service.update_region_config(
            db=db,
            region=region,
            connection_string=config_data.connection_string,
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
    admin_user: Dict = Depends(get_admin_user)
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
    admin_user: Dict = Depends(get_admin_user)
):
    """Test database connection for a region"""
    try:
        region_config_service = get_region_config_service()
        success, message = region_config_service.test_region_connection(db, region)
        
        return ConnectionTestResponse(success=success, message=message)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to test connection: {str(e)}")