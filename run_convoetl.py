"""
Quick script to run ConvoETL
"""

import asyncio
import logging
from convoetl import Pipeline

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


async def main():
    """Run ConvoETL pipeline"""
    
    # Initialize pipeline for Telegram -> SQLite
    pipeline = Pipeline(
        platform="telegram",
        storage_type="sqlite",
        storage_config={"db_path": "data/telegram_messages.db"},
        extractor_config={"config_path": "../telegram-group-scraper/config.ini"}
    )
    
    # Example group ID (replace with your actual group ID)
    group_id = "1670178185"
    
    print("\n" + "=" * 60)
    print("🚀 ConvoETL - Version 1 (Local SQLite)")
    print("=" * 60)
    
    # Run in auto mode (will backfill if first run, sync if not)
    print(f"\n📊 Processing Telegram group: {group_id}")
    print("   Mode: Auto (backfill or incremental)")
    print("   Storage: SQLite (data/telegram_messages.db)")
    
    result = await pipeline.run(
        source_id=group_id,
        mode="auto"
    )
    
    # Show results
    print("\n✅ Processing completed!")
    if 'total_messages' in result:
        print(f"   Backfilled: {result['total_messages']} messages")
    elif 'new_messages' in result:
        print(f"   New messages: {result['new_messages']}")
    
    # Get and show statistics
    from convoetl.loaders import SQLiteLoader
    loader = SQLiteLoader({"db_path": "data/telegram_messages.db"})
    stats = await loader.get_statistics()
    
    print("\n📈 Database Statistics:")
    print(f"   Total messages: {stats['total_messages']:,}")
    print(f"   Total users: {stats['total_users']:,}")
    print(f"   Total sources: {stats['total_sources']}")
    if stats['first_message']:
        print(f"   Date range: {stats['first_message']} to {stats['last_message']}")
    
    await loader.close()
    
    print("\n" + "=" * 60)
    print("💡 Next steps:")
    print("   1. Run polling: python examples/basic_usage.py")
    print("   2. Deploy with schedule: python deploy.py --source-id " + group_id)
    print("   3. Check data: sqlite3 data/telegram_messages.db")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    asyncio.run(main())