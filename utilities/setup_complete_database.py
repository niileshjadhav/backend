#!/usr/bin/env python3
"""
Complete database setup utility
Combines configuration, initialization, and verification
"""

import os
import sys
import logging
from pathlib import Path

# Add backend directory to path
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

def setup_logging():
    """Setup consistent logging across all database utilities"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s:%(name)s:%(message)s'
    )
    return logging.getLogger(__name__)

def main():
    """Main setup workflow"""
    logger = setup_logging()
    
    print("🚀 Cloud Inventory Database Setup")
    print("=" * 50)
    print("This utility will guide you through the complete database setup process.")
    print()
    
    # Step 1: Configuration
    print("📋 Step 1: Database Configuration")
    from utilities.setup_db_config import setup_database_config
    
    if not setup_database_config():
        print("❌ Configuration setup failed. Exiting.")
        return 1
    
    print("\n" + "=" * 50)
    
    # Step 2: Database Initialization
    print("📋 Step 2: Database Initialization")
    from initialize_database import initialize_database
    
    if not initialize_database():
        print("❌ Database initialization failed. Exiting.")
        return 1
    
    print("\n" + "=" * 50)
    print("🎉 DATABASE SETUP COMPLETE!")
    print("✅ Configuration saved")
    print("✅ Database connection verified")
    print("✅ All tables created")
    print("✅ Archive tables ready for statistics")
    print("=" * 50)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())