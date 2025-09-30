"""Authentication service"""
from sqlalchemy.orm import Session
from sqlalchemy import and_
from models.users import User
from typing import Optional, Dict, List
import hashlib
import jwt
import os
from datetime import datetime, timedelta, timezone
import logging
from passlib.context import CryptContext

logger = logging.getLogger(__name__)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

class AuthService:
    def __init__(self):
        self.secret_key = os.getenv("SECRET_KEY", "your-secret-key-change-in-production-environment")
        self.algorithm = "HS256"
        self.token_expire_minutes = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))
    
    def hash_password(self, password: str) -> str:
        """Hash a password using bcrypt"""
        return pwd_context.hash(password)
    
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify a password against its hash"""
        return pwd_context.verify(plain_password, hashed_password)
    
    def authenticate_user(self, username: str, password: str, db: Session = None) -> Optional[Dict]:
        """Authenticate user and return user info with role"""
        try:
            if not db:
                from database import get_db
                db = next(get_db())
            
            # Password is required for authentication
            if not password:
                logger.warning(f"Authentication failed for {username}: No password provided")
                return None
                
            user = db.query(User).filter(User.username == username).first()
            
            if not user:
                logger.warning(f"Authentication failed for {username}: User not found")
                return None
            
            # User must have a password hash stored in database
            if not user.password_hash:
                logger.warning(f"Authentication failed for {username}: No password hash stored")
                return None
            
            # Verify password against stored hash
            if not self.verify_password(password, user.password_hash):
                logger.warning(f"Authentication failed for {username}: Invalid password")
                return None
            
            logger.info(f"User {username} authenticated successfully")
            
            return {
                "user_id": user.username,
                "username": user.username,
                "role": user.role,
                "permissions": self.get_role_permissions(user.role),
                "active": True
            }
            
        except Exception as e:
            logger.error(f"Authentication error for {username}: {e}")
            return None
    
    def get_role_permissions(self, role: str) -> Dict[str, bool]:
        """Get permissions for a role"""
        if role == "Admin":
            return {
                "select": True,
                "archive": True,
                "delete_archive": True,
                "confirm_operations": True
            }
        elif role == "Monitor":
            return {
                "select": True,
                "archive": False,
                "delete_archive": False,
                "confirm_operations": False
            }
        else:
            return {
                "select": False,
                "archive": False,
                "delete_archive": False,
                "confirm_operations": False
            }
    
    def check_permission(self, user_role: str, operation: str) -> bool:
        """Check if user role has permission for operation"""
        permissions = self.get_role_permissions(user_role)
        
        operation_map = {
            "SELECT": "select",
            "ARCHIVE": "archive",
            "DELETE": "delete_archive",
            "CONFIRM": "confirm_operations"
        }
        
        permission_key = operation_map.get(operation.upper())
        return permissions.get(permission_key, False)
    
    def create_access_token(self, user_data: dict) -> str:
        """Create JWT access token"""
        try:
            to_encode = user_data.copy()
            expire = datetime.now(timezone.utc) + timedelta(minutes=self.token_expire_minutes)
            to_encode.update({"exp": expire})
            
            return jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
        except Exception as e:
            logger.error(f"Token creation error: {e}")
            return ""
    
    def verify_token(self, token: str) -> Optional[Dict]:
        """Verify JWT token and return user data"""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            return payload
        except jwt.ExpiredSignatureError:
            logger.warning("Token expired")
            return None
        except jwt.JWTError as e:
            logger.warning(f"Token verification failed: {e}")
            return None
    
    def get_user_from_token(self, token: str) -> Optional[Dict]:
        """Get user information from JWT token"""
        return self.verify_token(token)
    
    def get_all_users(self, db: Session = None) -> List[Dict]:
        """Get all users with their roles (Admin only)"""
        try:
            if not db:
                from database import get_db
                db = next(get_db())
                
            users = db.query(User).all()
            return [
                {
                    "id": user.id,
                    "username": user.username,
                    "role": user.role,
                    "active": True
                }
                for user in users
            ]
        except Exception as e:
            logger.error(f"Error fetching users: {e}")
            return []
    
    def create_user(self, username: str, password: str, role: str, db: Session) -> Dict:
        """Create a new user with hashed password"""
        try:
            # Check if username already exists
            existing_user = db.query(User).filter(User.username == username).first()
            if existing_user:
                return {
                    "success": False,
                    "error": f"Username '{username}' already exists"
                }
            
            # Validate role
            if role not in ["Admin", "Monitor"]:
                return {
                    "success": False,
                    "error": f"Invalid role '{role}'. Must be 'Admin' or 'Monitor'"
                }
            
            # Hash the password
            password_hash = self.hash_password(password)
            
            # Create new user
            new_user = User(
                username=username,
                password_hash=password_hash,
                role=role
            )
            
            db.add(new_user)
            db.commit()
            db.refresh(new_user)
            
            logger.info(f"User '{username}' created successfully with role '{role}'")
            
            return {
                "success": True,
                "user_id": new_user.id,
                "username": new_user.username,
                "role": new_user.role,
                "created_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error creating user '{username}': {e}")
            return {
                "success": False,
                "error": f"Failed to create user: {str(e)}"
            }