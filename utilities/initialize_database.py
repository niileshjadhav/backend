#!/usr/bin/env python3
"""
Initialize database with archive tables
This script ensures that archive tables are created if they don't exist
"""

import logging
import sys
from pathlib import Path

# Add backend directory to path
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

from dotenv import load_dotenv
load_dotenv()

from database import engine, Base, test_connection
from models.activities import DSIActivities, ArchiveDSIActivities
from models.transactions import DSITransactionLog, ArchiveDSITransactionLog
from services.database_service import setup_database_logging, REQUIRED_TABLES

# Set up logging
logger = setup_database_logging(__name__)

def initialize_database():
    """Initialize database with all required tables including archive tables"""
    try:
        # Test connection first
        if not test_connection():
            logger.error("❌ Database connection failed")
            return False
            
        logger.info("🔗 Database connection successful")
        
        # Create all tables (including archive tables)
        logger.info("📋 Creating database tables...")
        Base.metadata.create_all(bind=engine)
        
        logger.info("✅ All tables created successfully!")
        
        # Verify archive tables exist
        from sqlalchemy import inspect
        inspector = inspect(engine)
        
        existing_tables = inspector.get_table_names()
        
        logger.info("📊 Checking table existence:")
        all_exist = True
        for table in REQUIRED_TABLES:
            exists = table in existing_tables
            status = "✅" if exists else "❌"
            logger.info(f"  {status} {table}")
            if not exists:
                all_exist = False
        
        if all_exist:
            logger.info("🎉 All required tables exist!")
            
            # Test archive table functionality
            logger.info("🧪 Testing archive table functionality...")
            from database import get_db
            from sqlalchemy import func
            
            db = next(get_db())
            try:
                # Test counts
                main_activities = db.query(func.count(DSIActivities.SequenceID)).scalar()
                archive_activities = db.query(func.count(ArchiveDSIActivities.SequenceID)).scalar()
                main_transactions = db.query(func.count(DSITransactionLog.RecordID)).scalar()
                archive_transactions = db.query(func.count(ArchiveDSITransactionLog.RecordID)).scalar()
                
                logger.info("📈 Current record counts:")
                logger.info(f"  DSIActivities: {main_activities:,} main, {archive_activities:,} archive")
                logger.info(f"  DSITransactionLog: {main_transactions:,} main, {archive_transactions:,} archive")
                
            finally:
                db.close()
            
            return True
        else:
            logger.error("❌ Some required tables are missing")
            return False
            
    except Exception as e:
        logger.error(f"❌ Database initialization failed: {e}")
        return False

def main():
    """Main entry point"""
    logger.info("🚀 Starting database initialization...")
    
    success = initialize_database()
    
    if success:
        logger.info("✅ Database initialization completed successfully!")
        print("\n" + "="*50)
        print("✅ DATABASE READY")
        print("All archive tables are now available for statistics!")
        print("="*50)
        return 0
    else:
        logger.error("❌ Database initialization failed!")
        print("\n" + "="*50)
        print("❌ DATABASE INITIALIZATION FAILED")
        print("Please check the logs above for details.")
        print("="*50)
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())