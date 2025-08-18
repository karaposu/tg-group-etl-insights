"""
Simple test of ConvoETL without Prefect complications
"""

import asyncio
import sys
from pathlib import Path

# Test imports first
print("Testing imports...")

try:
    import pandas as pd
    print("✓ pandas imported")
except ImportError:
    print("✗ pandas not found - install with: pip install pandas")
    sys.exit(1)

# Add tgdata to path
tgdata_path = Path.home() / "Desktop/projects/telegram-group-scraper"
if tgdata_path.exists():
    sys.path.insert(0, str(tgdata_path))
    print(f"✓ Added tgdata path: {tgdata_path}")
else:
    print(f"✗ tgdata path not found: {tgdata_path}")

try:
    from tgdata import TgData
    print("✓ tgdata imported")
except ImportError:
    print("✗ tgdata not found")
    sys.exit(1)

# Now test ConvoETL components
print("\nTesting ConvoETL components...")

from convoetl.extractors.telegram import TelegramExtractor
from convoetl.loaders.sqlite import SQLiteLoader

print("✓ ConvoETL modules imported")


async def test_basic():
    """Test basic extraction and loading"""
    
    print("\n" + "=" * 60)
    print("ConvoETL Basic Test (without Prefect)")
    print("=" * 60)
    
    # Test extractor
    print("\n1. Testing Telegram Extractor...")
    config_path = Path.home() / "Desktop/projects/telegram-group-scraper/config.ini"
    
    if not config_path.exists():
        print(f"✗ Config not found at {config_path}")
        print("  Please create config.ini with your Telegram credentials")
        return
    
    extractor = TelegramExtractor({"config_path": str(config_path)})
    print("✓ Extractor initialized")
    
    # Test loader
    print("\n2. Testing SQLite Loader...")
    loader = SQLiteLoader({"db_path": "data/test.db"})
    print("✓ Loader initialized")
    
    # Test extraction (small batch)
    print("\n3. Testing message extraction...")
    group_id = "1670178185"  # Example group
    
    try:
        messages_df = await extractor.extract_messages(
            source_id=group_id,
            limit=10  # Just get 10 messages for testing
        )
        print(f"✓ Extracted {len(messages_df)} messages")
        
        if not messages_df.empty:
            print(f"  First message ID: {messages_df['message_id'].iloc[0]}")
            print(f"  Last message ID: {messages_df['message_id'].iloc[-1]}")
    except Exception as e:
        print(f"✗ Extraction failed: {e}")
        return
    finally:
        await extractor.close()
    
    # Test loading
    if not messages_df.empty:
        print("\n4. Testing message loading...")
        try:
            count = await loader.store_messages(messages_df)
            print(f"✓ Loaded {count} messages to SQLite")
            
            # Get statistics
            stats = await loader.get_statistics()
            print(f"\n📊 Database Statistics:")
            print(f"   Total messages: {stats['total_messages']}")
            print(f"   Total users: {stats['total_users']}")
        except Exception as e:
            print(f"✗ Loading failed: {e}")
    
    await loader.close()
    
    print("\n" + "=" * 60)
    print("✅ Basic test completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(test_basic())