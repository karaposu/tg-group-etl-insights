"""
Create Telegram ETL database schema

Usage:
    python -m db.scripts.create_telegram_db
    
    or with custom database:
    python -m db.scripts.create_telegram_db --db-url postgresql://user:pass@localhost/telegram
"""

import os
import sys
import argparse
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import create_engine
from db import Base, MODELS


def create_database(db_url: str = None, echo: bool = True):
    """
    Create all database tables
    
    Args:
        db_url: Database URL (defaults to SQLite in data folder)
        echo: Whether to echo SQL commands
    """
    if not db_url:
        # Default to SQLite in data folder
        data_dir = project_root / "data"
        data_dir.mkdir(exist_ok=True)
        
        db_path = data_dir / "telegram.db"
        db_url = f"sqlite:///{db_path}"
        
        print(f"Using SQLite database at: {db_path}")
    else:
        print(f"Using database URL: {db_url}")
    
    # Create engine
    engine = create_engine(db_url, echo=echo)
    
    # Create all tables
    print("\nCreating database schema...")
    Base.metadata.create_all(engine)
    
    # List created tables
    print("\nCreated tables:")
    for table_name, model in MODELS.items():
        print(f"  - {table_name}: {model.__tablename__}")
    
    print("\n✅ Database schema created successfully!")
    
    # Show connection info for reference
    print("\nConnection info for your application:")
    print(f"  Database URL: {db_url}")
    
    return engine


def drop_and_recreate(db_url: str = None):
    """
    Drop all tables and recreate (DANGEROUS - use with caution)
    
    Args:
        db_url: Database URL
    """
    if not db_url:
        data_dir = project_root / "data"
        db_path = data_dir / "telegram.db"
        db_url = f"sqlite:///{db_path}"
    
    engine = create_engine(db_url, echo=False)
    
    print("⚠️  WARNING: This will drop all existing tables and data!")
    response = input("Are you sure? Type 'yes' to continue: ")
    
    if response.lower() == 'yes':
        print("\nDropping existing tables...")
        Base.metadata.drop_all(engine)
        
        print("Recreating tables...")
        Base.metadata.create_all(engine)
        
        print("✅ Database recreated successfully!")
    else:
        print("❌ Operation cancelled")
    
    return engine


def main():
    """Main entry point with CLI arguments"""
    parser = argparse.ArgumentParser(
        description='Create Telegram ETL database schema',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create SQLite database (default)
  python -m db.scripts.create_telegram_db
  
  # Create PostgreSQL database
  python -m db.scripts.create_telegram_db --db-url postgresql://user:pass@localhost/telegram
  
  # Create BigQuery-compatible schema (local SQLite for development)
  python -m db.scripts.create_telegram_db --bigquery-compatible
  
  # Drop and recreate all tables (DANGEROUS)
  python -m db.scripts.create_telegram_db --recreate
        """
    )
    
    parser.add_argument(
        '--db-url',
        type=str,
        help='Database URL (defaults to sqlite:///data/telegram.db)'
    )
    
    parser.add_argument(
        '--no-echo',
        action='store_true',
        help='Disable SQL command echo'
    )
    
    parser.add_argument(
        '--recreate',
        action='store_true',
        help='Drop and recreate all tables (DANGEROUS - will delete all data)'
    )
    
    parser.add_argument(
        '--bigquery-compatible',
        action='store_true',
        help='Create BigQuery-compatible schema (uses SQLite locally for development)'
    )
    
    args = parser.parse_args()
    
    # Handle BigQuery compatibility mode
    if args.bigquery_compatible:
        print("📝 BigQuery-compatible mode")
        print("   Note: Using SQLite for local development")
        print("   Deploy to BigQuery using separate migration scripts")
        print()
    
    # Execute based on arguments
    if args.recreate:
        drop_and_recreate(args.db_url)
    else:
        create_database(args.db_url, echo=not args.no_echo)


if __name__ == "__main__":
    main()