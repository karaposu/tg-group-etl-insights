"""
Check database status and statistics

Usage:
    python -m db.scripts.check_db
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import create_engine, inspect, text
from db import MODELS


def check_database(db_url: str = None):
    """
    Check database status and show statistics
    
    Args:
        db_url: Database URL (defaults to SQLite in data folder)
    """
    if not db_url:
        db_path = project_root / "data" / "telegram.db"
        db_url = f"sqlite:///{db_path}"
        
        if not db_path.exists():
            print(f"❌ Database not found at: {db_path}")
            print("\nRun the following command to create it:")
            print("  python -m db.scripts.create_telegram_db")
            return
    
    print(f"📊 Checking database: {db_url}\n")
    
    engine = create_engine(db_url)
    inspector = inspect(engine)
    
    # Check if tables exist
    existing_tables = inspector.get_table_names()
    
    print("📋 Table Status:")
    print("-" * 50)
    
    for table_name, model in MODELS.items():
        tablename = model.__tablename__
        if tablename in existing_tables:
            # Get row count
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {tablename}"))
                count = result.scalar()
                print(f"✅ {tablename:30} {count:,} rows")
        else:
            print(f"❌ {tablename:30} NOT FOUND")
    
    # Show database metadata
    print("\n📈 Database Statistics:")
    print("-" * 50)
    
    with engine.connect() as conn:
        # Total messages
        try:
            result = conn.execute(text("SELECT COUNT(*) FROM messages"))
            message_count = result.scalar()
            print(f"Total messages: {message_count:,}")
        except:
            print("Total messages: N/A")
        
        # Total users
        try:
            result = conn.execute(text("SELECT COUNT(*) FROM users"))
            user_count = result.scalar()
            print(f"Total users: {user_count:,}")
        except:
            print("Total users: N/A")
        
        # Active groups
        try:
            result = conn.execute(text("SELECT COUNT(*) FROM groups WHERE is_active = 1"))
            group_count = result.scalar()
            print(f"Active groups: {group_count:,}")
        except:
            print("Active groups: N/A")
        
        # Latest ETL run
        try:
            result = conn.execute(text("""
                SELECT run_id, status, started_at, messages_processed 
                FROM etl_runs 
                ORDER BY started_at DESC 
                LIMIT 1
            """))
            row = result.fetchone()
            if row:
                print(f"\nLatest ETL Run:")
                print(f"  ID: {row[0]}")
                print(f"  Status: {row[1]}")
                print(f"  Started: {row[2]}")
                print(f"  Messages: {row[3]:,}" if row[3] else "  Messages: 0")
        except:
            pass
    
    print("\n✅ Database check complete!")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Check Telegram database status')
    parser.add_argument(
        '--db-url',
        type=str,
        help='Database URL (defaults to sqlite:///data/telegram.db)'
    )
    
    args = parser.parse_args()
    check_database(args.db_url)


if __name__ == "__main__":
    main()