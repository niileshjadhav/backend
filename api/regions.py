"""Region management API endpoints"""
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from typing import Optional, Dict
from datetime import datetime

from database import get_db
from services.region_service import get_region_service
from security import get_current_user_optional, get_current_user_required
from schemas import (
    RegionConnectionRequest, 
    RegionConnectionResponse, 
    RegionStatusResponse
)

router = APIRouter(prefix="/regions", tags=["regions"])

@router.get("/status", response_model=RegionStatusResponse)
async def get_regions_status(
    current_user: Optional[Dict] = Depends(get_current_user_optional)
):
    """Get status of all regions and available options"""
    try:
        region_service = get_region_service()
        
        return RegionStatusResponse(
            regions=region_service.get_connection_status(),
            available_regions=region_service.get_available_regions()
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get region status: {str(e)}")

@router.post("/connect", response_model=RegionConnectionResponse)
async def connect_to_region(
    request: RegionConnectionRequest,
    current_user: Optional[Dict] = Depends(get_current_user_optional)
):
    """Connect to a specific region"""
    try:
        region_service = get_region_service()
        
        # Validate region
        if request.region not in region_service.get_available_regions():
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid region: {request.region}. Available regions: {region_service.get_available_regions()}"
            )
        
        # Attempt connection
        success, message = await region_service.connect_to_region(request.region)
        
        # Get basic tables info if connection successful
        tables_info = None
        if success:
            test_success, test_message, tables_info = await region_service.test_connection(request.region)
            if not test_success:
                message += f". Warning: {test_message}"
        
        return RegionConnectionResponse(
            success=success,
            region=request.region,
            message=message,
            tables_info=tables_info
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to connect to region: {str(e)}")

@router.post("/disconnect", response_model=RegionConnectionResponse)
async def disconnect_from_region(
    request: RegionConnectionRequest,
    current_user: Optional[Dict] = Depends(get_current_user_optional)
):
    """Disconnect from a specific region"""
    try:
        region_service = get_region_service()
        
        success, message = await region_service.disconnect_from_region(request.region)
        
        return RegionConnectionResponse(
            success=success,
            region=request.region,
            message=message
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to disconnect from region: {str(e)}")

@router.get("/")
async def get_available_options(
    db: Session = Depends(get_db),
    current_user: Dict = Depends(get_current_user_required)
):
    """Get available regions and connection status for UI dropdowns"""
    
    try:
        region_service = get_region_service()
        
        return {
            "regions": region_service.get_available_regions(),
            "connection_status": region_service.get_connection_status(),
            "count": len(region_service.get_available_regions())
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get options: {str(e)}")