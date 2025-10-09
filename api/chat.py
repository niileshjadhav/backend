"""Chat API - No repetitive code"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from database import get_db
from schemas import ChatMessage, ChatResponse, ConfirmationRequest
from services.chat_service import ChatService
from services.region_service import RegionService
from security import get_current_user_optional, get_current_user_required
from shared.enums import TableName
from typing import Optional, Dict, Any
from datetime import datetime

router = APIRouter(prefix="/chat", tags=["chat"])

def _get_archive_table_name(table_name: str) -> str:
    """Get the correct archive table name for a given main table name"""
    if table_name == "dsiactivities":
        return "dsiactivitiesarchive"
    elif table_name == "dsitransactionlog":
        return "dsitransactionlogarchive" 
    elif table_name in ["dsiactivitiesarchive", "dsitransactionlogarchive"]:
        return table_name  # Already an archive table
    else:
        return f"{table_name}archive"  # Fallback for other tables

@router.post("", response_model=ChatResponse)
async def chat_with_agent(
    message: ChatMessage,
    db: Session = Depends(get_db),
    current_user: Optional[Dict] = Depends(get_current_user_optional)
):
    """Main chat endpoint with region and table support"""
    try:
        # Extract token for chat service (legacy support)
        token = None
        if current_user:
            # Create a simple token representation for the chat service
            from services.auth_service import AuthService
            auth_service = AuthService()
            token = auth_service.create_access_token(current_user)
        
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
    current_user: Dict = Depends(get_current_user_required)
):
    """Confirm archive or delete operations with buttons"""
    try:
        
        # Validate region connection
        from services.region_service import get_region_service
        region_service = get_region_service()
        
        if not region_service.is_connected(confirmation.region):
            raise HTTPException(
                status_code=400,
                detail=f"Not connected to region: {confirmation.region}"
            )
        
        # Import services
        from services.crud_service import CRUDService
        from schemas import ParsedOperation
        
        user_info = current_user
        structured_content = None
        
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
                archived_count = result['records_archived']
                response_text = f"Archive Completed in {confirmation.region.upper()}\n\n{archived_count:,} records successfully archived from {confirmation.table} to {_get_archive_table_name(confirmation.table)}."
                response_type = "archive_completed"
                
                # Create structured content for success card
                structured_content = {
                    "type": "success_card",
                    "title": "Archive Completed",
                    "region": confirmation.region.upper(),
                    "details": [
                        f"Successfully archived {archived_count:,} records",
                        f"From: {confirmation.table}",
                        f"To: {_get_archive_table_name(confirmation.table)}",
                        f"Executed by: {user_info['username']}",
                        "Records have been moved from main table to archive table."
                    ]
                }
            else:
                response_text = f"Archive failed: {result.get('error', 'Unknown error')}"
                response_type = "error"
                structured_content = None
                
        elif confirmation.operation == "DELETE" and confirmation.confirmed:
            result = await crud_service.execute_delete_operation(
                operation=operation,
                user_id=user_info["username"],
                reason="User confirmed via button",
                user_role=user_info["role"],
                confirmed=True
            )
            
            if result["success"]:
                deleted_count = result['records_deleted']
                response_text = f"Delete Completed in {confirmation.region.upper()}\n\n{deleted_count:,} records permanently deleted from {_get_archive_table_name(confirmation.table)}."
                response_type = "delete_completed"
                
                # Create structured content for success card
                structured_content = {
                    "type": "success_card",
                    "title": "Delete Completed",
                    "region": confirmation.region.upper(),
                    "details": [
                        f"Successfully deleted {deleted_count:,} records",
                        f"From: {_get_archive_table_name(confirmation.table)}",
                        f"Executed by: {user_info['username']}",
                        "Records permanently removed from the archive table."
                    ]
                }
            else:
                response_text = f"Delete failed: {result.get('error', 'Unknown error')}"
                response_type = "error"
                structured_content = None
        
        elif not confirmation.confirmed:
            response_text = f"Operation Cancelled\n\n{confirmation.operation} operation for {confirmation.table} in {confirmation.region.upper()} was cancelled by user."
            response_type = "cancelled"
            
            # Create structured content for cancelled card
            structured_content = {
                "type": "cancelled_card",
                "title": f"{confirmation.operation.title()} Cancelled",
                "region": confirmation.region.upper(),
                "table": confirmation.table,
                "message": f"The {confirmation.operation.lower()} operation has been cancelled.",
                "details": [
                    f"Cancelled: {confirmation.operation} for {confirmation.table}",
                    "No changes have been made to the database"
                ]
            }
        
        else:
            response_text = f"Unsupported Operation\n\nOperation '{confirmation.operation}' is not supported for confirmation."
            response_type = "error"
            structured_content = None
        
        # Cleanup session
        try:
            region_db_session.close()
        except:
            pass
        
        return ChatResponse(
            response=response_text,
            response_type=response_type,
            structured_content=structured_content,
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