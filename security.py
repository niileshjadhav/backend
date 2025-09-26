"""Security utilities for FastAPI authentication"""
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from services.auth_service import AuthService
from typing import Optional, Dict
import logging

logger = logging.getLogger(__name__)

# Create security scheme
security = HTTPBearer(auto_error=False)

def get_auth_service() -> AuthService:
    """Get authentication service instance"""
    return AuthService()

async def get_current_user_optional(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    auth_service: AuthService = Depends(get_auth_service)
) -> Optional[Dict]:
    """
    Get current user from token (optional - returns None if no token or invalid token)
    Use this for endpoints that work without authentication but provide enhanced features when authenticated
    """
    if not credentials:
        return None
    
    try:
        user_info = auth_service.get_user_from_token(credentials.credentials)
        return user_info
    except Exception as e:
        logger.warning(f"Optional authentication failed: {e}")
        return None

async def get_current_user_required(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    auth_service: AuthService = Depends(get_auth_service)
) -> Dict:
    """
    Get current user from token (required - raises HTTP 401 if no token or invalid token)
    Use this for endpoints that require authentication
    """
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    try:
        user_info = auth_service.get_user_from_token(credentials.credentials)
        if not user_info:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired token",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return user_info
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )

async def get_admin_user(
    current_user: Dict = Depends(get_current_user_required)
) -> Dict:
    """
    Get current user and verify admin role (raises HTTP 403 if not admin)
    Use this for endpoints that require admin privileges
    """
    if current_user.get("role") != "Admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    return current_user

def check_operation_permission(user_role: str, operation: str) -> bool:
    """Check if user role has permission for operation"""
    auth_service = AuthService()
    return auth_service.check_permission(user_role, operation)

async def require_operation_permission(
    operation: str,
    current_user: Dict = Depends(get_current_user_required)
) -> Dict:
    """
    Verify user has permission for specific operation
    Use this for endpoints that require specific permissions
    """
    if not check_operation_permission(current_user.get("role"), operation):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Insufficient permissions for {operation} operation"
        )
    return current_user