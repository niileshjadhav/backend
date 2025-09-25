"""API schemas"""
from pydantic import BaseModel
from typing import Dict, List, Optional, Any

class ChatMessage(BaseModel):
    message: str
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    region: Optional[str] = None

class ChatResponse(BaseModel):
    response: str  # Keep for backward compatibility
    response_type: Optional[str] = "conversation"
    suggestions: Optional[List[str]] = None
    requires_confirmation: bool = False
    operation_data: Optional[Dict] = None
    context: Optional[Dict[str, Any]] = None
    row_count: Optional[int] = None
    sample_data: Optional[List[Dict]] = None
    
    # New structured content fields
    structured_content: Optional[Dict[str, Any]] = None  # For rich content rendering

# Authentication schemas
class LoginRequest(BaseModel):
    username: str
    password: str

class LoginResponse(BaseModel):
    access_token: str
    token_type: str
    user_info: dict

class UserInfoResponse(BaseModel):
    username: str
    role: str
    permissions: list

# Operation schemas
class OperationRequest(BaseModel):
    operation: str
    table: str
    filters: Optional[Dict[str, Any]] = None
    
class OperationResponse(BaseModel):
    success: bool
    operation: str
    count: int
    data: Optional[List[Dict]] = None
    preview: bool = False

# MCP integration schemas
class MCPRequest(BaseModel):
    operation: str
    parameters: Dict[str, Any]
    user_context: Optional[Dict[str, str]] = None

class MCPResponse(BaseModel):
    success: bool
    result: Any
    error: Optional[str] = None
    suggestions: Optional[List[str]] = None

# Region and connection schemas
class RegionConnectionRequest(BaseModel):
    region: str

class RegionConnectionResponse(BaseModel):
    success: bool
    region: str
    message: str
    tables_info: Optional[Dict[str, Any]] = None

class RegionStatusResponse(BaseModel):
    regions: Dict[str, bool]
    available_regions: List[str]

class ConfirmationRequest(BaseModel):
    operation: str
    table: str
    region: str
    filters: Dict[str, Any]
    confirmed: bool = False