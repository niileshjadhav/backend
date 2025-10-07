"""API schemas"""
from pydantic import BaseModel, field_validator
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime

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
    
    # Tool input parameters for advanced operations
    tool_input: Optional[Dict[str, Any]] = None  # For tool-specific input parameters

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

# Region configuration schemas
class RegionConfigCreate(BaseModel):
    region: str
    connection_string: str
    connection_notes: Optional[str] = None

class RegionConfigUpdate(BaseModel):
    connection_string: Optional[str] = None
    is_active: Optional[bool] = None
    connection_notes: Optional[str] = None

class RegionConfigResponse(BaseModel):
    id: int
    region: str
    connection_notes: Optional[str]
    is_active: bool
    is_connected: bool
    last_connected_at: Optional[str]
    created_at: str
    updated_at: Optional[str]

    @field_validator('last_connected_at', 'created_at', 'updated_at', mode='before')
    @classmethod
    def convert_datetime_to_string(cls, v):
        if v is None:
            return None
        if isinstance(v, datetime):
            return v.isoformat()
        return v

    class Config:
        from_attributes = True

class ConnectionTestResponse(BaseModel):
    success: bool
    message: str

# Database operation data structures
@dataclass
class ParsedOperation:
    """Represents a parsed operation from user prompt"""
    action: str  # SELECT, ARCHIVE, DELETE
    table: str   # dsiactivities, dsitransactionlog, archivedsiactivities, archivedsitransactionlog
    filters: Dict[str, str]  # date_start, date_end, agent_name, server_name, etc.
    is_archive_target: bool  # True if operation targets archive table
    original_prompt: str
    confidence: float  # 0.0 to 1.0
    validation_errors: List[str]