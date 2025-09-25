"""Authentication API"""
from fastapi import APIRouter, HTTPException, Depends, Header, Request
from sqlalchemy.orm import Session
from database import get_db
from services.auth_service import AuthService
from pydantic import BaseModel
from typing import Optional

router = APIRouter(prefix="/auth", tags=["authentication"])

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

@router.post("/login", response_model=LoginResponse)
async def login(
    request: LoginRequest,
    db: Session = Depends(get_db)
):
    try:
        auth_service = AuthService()
        
        # Authenticate user
        user_info = auth_service.authenticate_user(
            request.username, 
            request.password,
            db
        )
        
        if not user_info:
            raise HTTPException(
                status_code=401,
                detail="Invalid username or password"
            )
        
        # Generate JWT token
        token = auth_service.create_access_token(user_info)
        
        return LoginResponse(
            access_token=token,
            token_type="bearer",
            user_info=user_info
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Authentication error: {str(e)}"
        )

@router.get("/me", response_model=UserInfoResponse)
async def get_current_user(request: Request):
    try:
        authorization = request.headers.get("authorization")
        
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(
                status_code=401,
                detail="Authorization header required"
            )
        
        token = authorization[7:]
        auth_service = AuthService()
        
        user_info = auth_service.get_user_from_token(token)
        
        if not user_info:
            raise HTTPException(
                status_code=401,
                detail="Invalid or expired token"
            )
        
        role = user_info.get("role", "Monitor")
        permissions_dict = auth_service.get_role_permissions(role)
        
        # Convert permissions dictionary to list of granted permissions
        permissions = [perm for perm, granted in permissions_dict.items() if granted]
        
        return UserInfoResponse(
            username=user_info.get("username"),
            role=role,
            permissions=permissions
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Token validation error: {str(e)}"
        )

@router.post("/refresh", response_model=LoginResponse)
async def refresh_token(
    request: Request,
    db: Session = Depends(get_db)
):
    try:
        authorization = request.headers.get("authorization")
        
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(
                status_code=401,
                detail="Authorization header required"
            )
        
        token = authorization[7:]
        auth_service = AuthService()
        
        # Verify the current token
        user_info = auth_service.get_user_from_token(token)
        
        if not user_info:
            raise HTTPException(
                status_code=401,
                detail="Invalid or expired token"
            )
        
        # Create a new token with fresh expiration
        new_token = auth_service.create_access_token(user_info)
        
        return LoginResponse(
            access_token=new_token,
            token_type="bearer",
            user_info=user_info
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Token refresh error: {str(e)}"
        )