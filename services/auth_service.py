"""Authentication service"""
from sqlalchemy.orm import Session
from sqlalchemy import and_
from models.users import User
from typing import Optional, Dict, List
import hashlib
import jwt
import os
from datetime import datetime, timedelta
import logging
from passlib.context import CryptContext

logger = logging.getLogger(__name__)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

class AuthService:
    def __init__(self):
        self.secret_key = os.getenv("SECRET_KEY")
        self.algorithm = "HS256"
        self.token_expire_minutes = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))
    
    def hash_password(self, password: str) -> str:
        """Hash a password using bcrypt"""
        return pwd_context.hash(password)
    
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify a password against its hash"""
        return pwd_context.verify(plain_password, hashed_password)
    
    def authenticate_user(self, username: str, password: str = None, db: Session = None) -> Optional[Dict]:
        """Authenticate user and return user info with role"""
        try:
            if not db:
                from database import get_db
                db = next(get_db())
                
            user = db.query(User).filter(User.username == username).first()
            
            if not user:
                # For demo purposes, create default users if they don't exist
                user = self.create_demo_user(username, db)
                if not user:
                    return None
            
            # Verify password if provided and user has a password hash
            if password and user.password_hash:
                if not self.verify_password(password, user.password_hash):
                    return None
            elif password and not user.password_hash:
                # User exists but no password set - for backward compatibility, allow specific demo passwords
                expected_passwords = {"admin": "admin@123", "monitor": "monitor@123"}
                if username.lower() in expected_passwords and password == expected_passwords[username.lower()]:
                    # Update user with hashed password
                    user.password_hash = self.hash_password(password)
                    db.commit()
                else:
                    return None
            
            # Update last login (skip if columns don't exist)
            # user.last_login = datetime.utcnow()
            # db.commit()
            
            return {
                "user_id": user.username,
                "username": user.username,
                "role": user.role,
                "permissions": self.get_role_permissions(user.role),
                "active": True
            }
            
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return None
    
    def create_demo_user(self, username: str, db: Session) -> Optional[User]:
        """Create user for testing"""
        try:
            # Determine role and password based on username
            if username.lower() == "admin":
                role = "Admin"
            elif username.lower() == "monitor":
                role = "Monitor"
            else:
                return None  # Only create admin and monitor users
            
            user = User(
                username=username,
                role=role
            )
            
            db.add(user)
            db.commit()
            db.refresh(user)
            
            logger.info(f"Created user: {username} with role: {role}")
            return user
            
        except Exception as e:
            logger.error(f"Error creating user: {e}")
            db.rollback()
            return None

    def create_demo_users(self, db: Session = None) -> List[Dict]:
        """Create admin and monitor users"""
        if not db:
            from database import get_db
            db = next(get_db())
            
        users_created = []
        demo_users = [
            {
                "username": os.getenv("DEFAULT_ADMIN_USERNAME", "admin"), 
                "password": os.getenv("DEFAULT_ADMIN_PASSWORD", "admin@123"), 
                "role": "Admin"
            },
            {
                "username": os.getenv("DEFAULT_MONITOR_USERNAME", "monitor"), 
                "password": os.getenv("DEFAULT_MONITOR_PASSWORD", "monitor@123"), 
                "role": "Monitor"
            }
        ]
        
        try:
            for user_data in demo_users:
                # Check if user already exists
                existing = db.query(User).filter(User.username == user_data["username"]).first()
                if not existing:
                    user = User(
                        username=user_data["username"],
                        role=user_data["role"]
                    )
                    db.add(user)
                    users_created.append(user_data)
                else:
                    logger.info(f"User {user_data['username']} already exists")
            
            if users_created:
                db.commit()
                logger.info(f"Created {len(users_created)} users")
            
            return users_created
            
        except Exception as e:
            logger.error(f"Error creating users: {e}")
            db.rollback()
            return []
    
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
            expire = datetime.utcnow() + timedelta(minutes=self.token_expire_minutes)
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