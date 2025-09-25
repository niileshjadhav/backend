"""
Script to create admin and monitor users in the database
Run this once to set up the initial users
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import get_db, Base, engine
from models.users import User
from services.auth_service import AuthService
from sqlalchemy.orm import Session
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_initial_users():
    """Create admin and monitor users in the database"""
    try:
        # Create all tables if they don't exist
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables initialized")
        
        # Get database session and auth service
        db = next(get_db())
        auth_service = AuthService()
        
        # Users to create with passwords
        users_to_create = [
            {
                "username": os.getenv("DEFAULT_ADMIN_USERNAME", "admin"), 
                "role": "Admin", 
                "password": os.getenv("DEFAULT_ADMIN_PASSWORD", "admin@123")
            },
            {
                "username": os.getenv("DEFAULT_MONITOR_USERNAME", "monitor"), 
                "role": "Monitor", 
                "password": os.getenv("DEFAULT_MONITOR_PASSWORD", "monitor@123")
            }
        ]
        
        users_created = []
        
        for user_data in users_to_create:
            # Check if user already exists
            existing_user = db.query(User).filter(User.username == user_data["username"]).first()
            
            if existing_user:
                # Update password if user exists but has no password hash
                if not existing_user.password_hash:
                    existing_user.password_hash = auth_service.hash_password(user_data["password"])
                    db.commit()
                    logger.info(f"Updated password for existing user '{user_data['username']}'")
                else:
                    logger.info(f"User '{user_data['username']}' already exists with role '{existing_user.role}' and password set")
                continue
            
            # Create new user with hashed password
            new_user = User(
                username=user_data["username"],
                role=user_data["role"],
                password_hash=auth_service.hash_password(user_data["password"])
            )
            
            db.add(new_user)
            users_created.append(user_data)
            logger.info(f"Created user '{user_data['username']}' with role '{user_data['role']}' and hashed password")
        
        if users_created:
            db.commit()
            logger.info(f"Successfully created {len(users_created)} users")
        else:
            logger.info("No new users were created (all users already exist)")
        
        # List all current users
        all_users = db.query(User).all()
        logger.info("Current users in database:")
        for user in all_users:
            logger.info(f"  - {user.username} ({user.role})")
        
        db.close()
        return True
        
    except Exception as e:
        logger.error(f"Error creating users: {e}")
        if 'db' in locals():
            db.rollback()
            db.close()
        return False

if __name__ == "__main__":
    logger.info("Creating initial admin and monitor users...")
    success = create_initial_users()
    
    if success:
        logger.info("✅ User creation completed successfully!")
        logger.info("Default credentials:")
        logger.info("  Admin: username='admin', password='admin@123'")
        logger.info("  Monitor: username='monitor', password='monitor@123'")
    else:
        logger.error("❌ User creation failed!")
        sys.exit(1)