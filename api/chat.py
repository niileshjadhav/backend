"""Chat API - No repetitive code"""
from fastapi import APIRouter, Depends, HTTPException, Header
from sqlalchemy.orm import Session
from database import get_db
from schemas import ChatMessage, ChatResponse, ConfirmationRequest
from services.chat_service import ChatService
from services.region_service import RegionService
from shared.enums import Region, TableName
from typing import Optional, Dict, Any
from datetime import datetime

router = APIRouter(prefix="/chat", tags=["chat"])

@router.post("", response_model=ChatResponse)
async def chat_with_agent(
    message: ChatMessage,
    db: Session = Depends(get_db),
    authorization: Optional[str] = Header(None)
):
    """Main chat endpoint with region and table support"""
    try:
        # Extract token from Authorization header
        token = None
        if authorization and authorization.startswith("Bearer "):
            token = authorization[7:]
        
        # Validate region if provided
        if message.region:
            from services.region_service import get_region_service
            region_service = get_region_service()
            
            if message.region not in region_service.get_available_regions():
                raise HTTPException(
                    status_code=400, 
                    detail=f"Invalid region: {message.region}. Available: {region_service.get_available_regions()}"
                )
            
            if not region_service.is_connected(message.region):
                raise HTTPException(
                    status_code=400,
                    detail=f"Not connected to region: {message.region}. Please connect first."
                )
        
        chat_service = ChatService()
        return await chat_service.process_chat(
            user_message=message.message,
            db=db,
            user_token=token,
            session_id=message.session_id,
            user_id=message.user_id,
            region=message.region
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Chat processing failed: {str(e)}")

@router.post("/confirm", response_model=ChatResponse)
async def confirm_operation(
    confirmation: ConfirmationRequest,
    db: Session = Depends(get_db),
    authorization: Optional[str] = Header(None)
):
    """Confirm archive or delete operations with buttons"""
    try:
        # Extract token from Authorization header
        token = None
        if authorization and authorization.startswith("Bearer "):
            token = authorization[7:]
        
        if not token:
            raise HTTPException(status_code=401, detail="Authentication required for confirmations")
        
        # Validate region connection
        from services.region_service import get_region_service
        region_service = get_region_service()
        
        if not region_service.is_connected(confirmation.region):
            raise HTTPException(
                status_code=400,
                detail=f"Not connected to region: {confirmation.region}"
            )
        
        # Import services
        from services.auth_service import AuthService
        from services.crud_service import CRUDService
        from services.prompt_parser import ParsedOperation
        
        auth_service = AuthService()
        user_info = auth_service.get_user_from_token(token)
        
        # Get regional database session
        region_db_session = region_service.get_session(confirmation.region)
        crud_service = CRUDService(region_db_session)
        
        # Create operation object
        operation = ParsedOperation(
            action=confirmation.operation,
            table=confirmation.table,
            filters=confirmation.filters,
            confidence=1.0,
            original_prompt=f"Confirmed {confirmation.operation.lower()} operation",
            validation_errors=[],
            is_archive_target=(confirmation.operation == "DELETE")
        )
        
        # Execute confirmed operation
        if confirmation.operation == "ARCHIVE" and confirmation.confirmed:
            result = await crud_service.execute_archive_operation(
                operation=operation,
                user_id=user_info["username"],
                reason="User confirmed via button",
                user_role=user_info["role"],
                confirmed=True
            )
            
            if result["success"]:
                response_text = f"✅ Archive Completed in {confirmation.region.upper()}\n\n{result['records_archived']:,} records successfully archived from {confirmation.table} to {confirmation.table}_archive."
                response_type = "operation_success"
            else:
                response_text = f"❌ Archive failed: {result.get('error', 'Unknown error')}"
                response_type = "error"
                
        elif confirmation.operation == "DELETE" and confirmation.confirmed:
            result = await crud_service.execute_delete_operation(
                operation=operation,
                user_id=user_info["username"],
                reason="User confirmed via button",
                user_role=user_info["role"],
                confirmed=True
            )
            
            if result["success"]:
                response_text = f"✅ Delete Completed in {confirmation.region.upper()}\n\n{result['records_deleted']:,} records permanently deleted from {confirmation.table}_archive."
                response_type = "operation_success"
            else:
                response_text = f"❌ Delete failed: {result.get('error', 'Unknown error')}"
                response_type = "error"
        
        elif not confirmation.confirmed:
            response_text = f"❌ Operation Cancelled\n\n{confirmation.operation} operation for {confirmation.table} in {confirmation.region.upper()} was cancelled by user."
            response_type = "operation_cancelled"
        
        else:
            response_text = f"❌ Unsupported Operation\n\nOperation '{confirmation.operation}' is not supported for confirmation."
            response_type = "error"
        
        # Cleanup session
        try:
            region_db_session.close()
        except:
            pass
        
        return ChatResponse(
            response=response_text,
            response_type=response_type,
            context={
                "operation": confirmation.operation,
                "table": confirmation.table,
                "region": confirmation.region,
                "confirmed": confirmation.confirmed,
                "timestamp": datetime.now().isoformat()
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Confirmation failed: {str(e)}")