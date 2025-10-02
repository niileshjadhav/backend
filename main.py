"""FastAPI application for Cloud Inventory Log Management System"""
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from contextlib import asynccontextmanager
import logging

# Initialize logging first
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from database import test_connection, Base, engine
# Import all models to ensure they are registered with SQLAlchemy
from api import chat_router
from api.auth import router as auth_router
from api.regions import router as regions_router

# Create tables
try:
    from sqlalchemy import inspect
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    
    required_tables = ['dsiactivities', 'dsiactivitiesarchive', 'dsitransactionlog', 'dsitransactionlogarchive', 'region_config']
    missing_tables = [t for t in required_tables if t not in existing_tables]
    
    if missing_tables:
        logger.warning(f"Missing tables detected: {missing_tables}")
        # Only create missing tables
        Base.metadata.create_all(bind=engine, checkfirst=True)
        logger.info("🔧 Created missing tables")
        
        # Verify creation was successful
        updated_tables = inspector.get_table_names()
        still_missing = [t for t in required_tables if t not in updated_tables]
        if still_missing:
            logger.error(f"Failed to create tables: {still_missing}")
        else:
            logger.info("All required tables now present")
    else:
        logger.info("All required tables already exist - skipping creation")
        
except Exception as e:
    logger.error(f"Database table verification/creation failed: {e}")
    raise

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    if test_connection():
        logger.info("Database connection successful")
        logger.info("MCP server ready to handle requests")
        logger.info("Application started successfully")
    else:
        logger.error("Database connection failed")
        raise Exception("Database connection failed")
    
    yield

# Initialize FastAPI
app = FastAPI(
    title="Cloud Inventory Log Management API",
    description="Log Management System",
    version="2.0.0",
    lifespan=lifespan,
    # Add OpenAPI security scheme for better documentation
    openapi_tags=[
        {
            "name": "authentication",
            "description": "Authentication and user management operations"
        },
        {
            "name": "chat",
            "description": "Chat interface for database operations"
        },
        {
            "name": "regions", 
            "description": "Multi-region database management"
        },
        {
            "name": "region-config",
            "description": "Region configuration management"
        }
    ]
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global OPTIONS handler
@app.options("/{path:path}")
async def options_handler(request: Request, path: str):
    return Response(
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "*",
        }
    )

# Include routers
app.include_router(auth_router)
app.include_router(chat_router)
app.include_router(regions_router)
from api.region_config import router as region_config_router
app.include_router(region_config_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)