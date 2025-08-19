"""
Test chat info extraction and storage
"""

import asyncio
import sqlite3
from convoetl.extractors.telegram import TelegramExtractor
from convoetl.loaders.sqlite import SQLiteLoader

async def test_chat_info():
    """Test chat info extraction"""
    
    # Initialize extractor
    extractor = TelegramExtractor({"config_path": "config.ini"})
    
    # Extract chat info
    chat_info = await extractor.extract_chat_info("1670178185")
    print("Chat info extracted:")
    print(f"  Chat ID: {chat_info.get('chat_id')}")
    print(f"  Title: {chat_info.get('title')}")
    print(f"  Type: {chat_info.get('chat_type')}")
    print(f"  Participants: {chat_info.get('participants_count')}")
    
    # Store in database
    loader = SQLiteLoader({"db_path": "data/telegram.db"})
    await loader.store_chat_info(chat_info)
    print("\nChat info stored in database")
    
    # Verify
    conn = sqlite3.connect("data/telegram.db")
    cursor = conn.cursor()
    cursor.execute("SELECT chat_id, title, chat_type FROM chats WHERE chat_id = ?", ("1670178185",))
    result = cursor.fetchone()
    if result:
        print(f"\nVerified in DB: {result}")
    else:
        print("\nNot found in database!")
    
    conn.close()
    await loader.close()
    await extractor.close()

if __name__ == "__main__":
    asyncio.run(test_chat_info())