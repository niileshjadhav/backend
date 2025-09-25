"""User model"""
from sqlalchemy import Column, Integer, String, Enum
from database import Base

class User(Base):
    __tablename__ = "users"
    
    id = Column("UserID", Integer, primary_key=True, autoincrement=True)
    username = Column("Username", String(50), unique=True, nullable=True)
    role = Column("Role", Enum('Admin', 'Monitor'), nullable=False)
    password_hash = Column("PasswordHash", String(255), nullable=True)