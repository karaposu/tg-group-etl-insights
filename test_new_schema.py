"""
Test the new schema with chats table
"""

import asyncio
import sqlite3
from convoetl import Pipeline

async def test_new_schema():
    """Test extraction with new schema"""
    
    print("Testing new schema with chats table...")
    print("-" * 60)
    
    # Initialize pipeline
    pipeline = Pipeline(
        platform="telegram",
        storage_type="sqlite",
        storage_config={"db_path": "data/telegram.db"},
        extractor_config={"config_path": "config.ini"}
    )
    
    # Test with a small batch
    group_id = "1670178185"  # Bitcoinsensus
    
    print(f"Extracting messages from group {group_id}...")
    result = await pipeline.sync(
        source_id=group_id,
        limit=10  # Just 10 messages for testing
    )
    
    print(f"✅ Extracted {result.get('new_messages', 0)} messages")
    
    # Check the database structure
    conn = sqlite3.connect("data/telegram.db")
    cursor = conn.cursor()
    
    # Check tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print(f"\nTables created: {[t[0] for t in tables]}")
    
    # Check chats table
    cursor.execute("SELECT * FROM chats LIMIT 1")
    chat = cursor.fetchone()
    if chat:
        cursor.execute("PRAGMA table_info(chats)")
        columns = cursor.fetchall()
        print(f"\nChats table columns: {[c[1] for c in columns]}")
        print(f"Sample chat record: {chat[:5]}...")  # First 5 fields
    
    # Check messages with chat_id
    cursor.execute("SELECT chat_id, user_id, message_text FROM messages LIMIT 3")
    messages = cursor.fetchall()
    print(f"\nSample messages:")
    for msg in messages:
        print(f"  Chat: {msg[0]}, User: {msg[1]}, Text: {msg[2][:50]}...")
    
    # Check if we can query by chat_id
    cursor.execute("SELECT COUNT(*) FROM messages WHERE chat_id = ?", (group_id,))
    count = cursor.fetchone()[0]
    print(f"\nMessages for chat {group_id}: {count}")
    
    conn.close()
    
    return result

if __name__ == "__main__":
    asyncio.run(test_new_schema())