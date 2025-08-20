#!/usr/bin/env python3
"""
Script to create the chat_daily table in the database
"""

import sys
from pathlib import Path

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db import create_all_tables, get_engine, ChatDaily
from sqlalchemy import inspect, text


def main():
    """Create chat_daily table"""
    
    print("Creating chat_daily table...")
    
    # Get engine with SQLite database
    db_path = "data/telegram.db"
    engine = get_engine(f"sqlite:///{db_path}")
    
    # Check if table exists
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    
    if 'chat_daily' in existing_tables:
        print("Table 'chat_daily' already exists")
        
        # Show column count
        columns = inspector.get_columns('chat_daily')
        print(f"Current table has {len(columns)} columns")
        
        response = input("Do you want to drop and recreate it? (y/n): ")
        if response.lower() == 'y':
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE chat_daily"))
                conn.commit()
                print("Table dropped")
        else:
            print("Keeping existing table")
            return
    
    # Create table
    ChatDaily.__table__.create(engine)
    print("Table 'chat_daily' created successfully")
    
    # Show table structure
    inspector = inspect(engine)
    columns = inspector.get_columns('chat_daily')
    
    print(f"\nTable structure ({len(columns)} columns):")
    print("-" * 60)
    
    for col in columns:
        nullable = "" if col.get('nullable', True) else " NOT NULL"
        default = f" DEFAULT {col.get('default')}" if col.get('default') else ""
        print(f"  - {col['name']}: {col['type']}{nullable}{default}")
    
    # Show indexes
    indexes = inspector.get_indexes('chat_daily')
    if indexes:
        print(f"\nIndexes ({len(indexes)}):")
        for idx in indexes:
            print(f"  - {idx['name']}: {', '.join(idx['column_names'])}")
    
    # Show primary key
    pk = inspector.get_pk_constraint('chat_daily')
    if pk and pk.get('constrained_columns'):
        print(f"\nPrimary Key: {', '.join(pk['constrained_columns'])}")
    
    print("\n✓ Table creation complete!")
    
    # Test with sample query
    print("\nTest query to verify table:")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) as count FROM chat_daily"))
        count = result.scalar()
        print(f"  Current row count: {count}")


if __name__ == "__main__":
    main()