"""Authentication API"""
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from database import get_db
from services.auth_service import AuthService
from security import get_current_user_required, get_current_user_optional, get_admin_user
from pydantic import BaseModel
from typing import Optional, Literal, List, Dict

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

class SignupRequest(BaseModel):
    username: str
    password: str
    role: Literal["Admin", "Monitor"] = "Monitor"

class SignupResponse(BaseModel):
    success: bool
    message: str
    user_info: Optional[dict] = None

class UserListResponse(BaseModel):
    success: bool
    users: List[dict]
    total_count: int

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
async def get_current_user(
    current_user: Dict = Depends(get_current_user_required),
    auth_service: AuthService = Depends(lambda: AuthService())
):
    """Get current user information from JWT token"""
    try:
        role = current_user.get("role", "Monitor")
        permissions_dict = auth_service.get_role_permissions(role)
        
        # Convert permissions dictionary to list of granted permissions
        permissions = [perm for perm, granted in permissions_dict.items() if granted]
        
        return UserInfoResponse(
            username=current_user.get("username"),
            role=role,
            permissions=permissions
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Token validation error: {str(e)}"
        )

@router.post("/refresh", response_model=LoginResponse)
async def refresh_token(
    current_user: Dict = Depends(get_current_user_required),
    auth_service: AuthService = Depends(lambda: AuthService())
):
    """Refresh JWT token with extended expiration"""
    try:
        # Create a new token with fresh expiration
        new_token = auth_service.create_access_token(current_user)
        
        return LoginResponse(
            access_token=new_token,
            token_type="bearer",
            user_info=current_user
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Token refresh error: {str(e)}"
        )

@router.post("/signup", response_model=SignupResponse)
async def signup_user(
    signup_request: SignupRequest,
    db: Session = Depends(get_db),
    current_user: Optional[Dict] = Depends(get_current_user_optional),
    auth_service: AuthService = Depends(lambda: AuthService())
):
    """
    Create a new user account. 
    
    Admin users can create any role (Admin or Monitor).
    
    Security Rules:
    - Only Admin users can create other Admin and Monitor users
    - Username must be unique
    - Password must be at least 8 characters
    """
    try:
        # Validate input
        if not signup_request.username or len(signup_request.username.strip()) < 3:
            raise HTTPException(
                status_code=400,
                detail="Username must be at least 3 characters long"
            )
        
        if not signup_request.password or len(signup_request.password) < 8:
            raise HTTPException(
                status_code=400,
                detail="Password must be at least 8 characters long"
            )
        
        # Authorization logic for role creation
        requested_role = signup_request.role
        
        if requested_role in ["Admin", "Monitor"]:
            # Only Admin users can create other Admin and Monitor users
            if not current_user or current_user.get("role") != "Admin":
                raise HTTPException(
                    status_code=403,
                    detail="Only Admin users can create other Admin and Monitor accounts. Admin authentication required."
                )

        # Create the user
        result = auth_service.create_user(
            username=signup_request.username.strip(),
            password=signup_request.password,
            role=requested_role,
            db=db
        )
        
        if result["success"]:
            return SignupResponse(
                success=True,
                message=f"User '{signup_request.username}' created successfully with role '{requested_role}'",
                user_info={
                    "username": signup_request.username,
                    "role": requested_role,
                    "created_at": result.get("created_at", "now")
                }
            )
        else:
            raise HTTPException(
                status_code=400,
                detail=result["error"]
            )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"User creation error: {str(e)}"
        )

@router.get("/users", response_model=UserListResponse)
async def list_users(
    db: Session = Depends(get_db),
    admin_user: Dict = Depends(get_admin_user),
    auth_service: AuthService = Depends(lambda: AuthService())
):
    """
    List all users in the system.
    
    Only accessible to Admin users.
    Useful for user management and auditing.
    """
    try:
        # Get all users
        users = auth_service.get_all_users(db)
        
        return UserListResponse(
            success=True,
            users=users,
            total_count=len(users)
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error listing users: {str(e)}"
        )