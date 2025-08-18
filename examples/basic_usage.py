"""
Basic ConvoETL usage examples
"""

# python -m examples.basic_usage

import asyncio
import logging
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from convoetl import Pipeline

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


async def example_backfill():
    """Example: Backfill all historical messages from a Telegram group"""
    
    # Initialize pipeline
    pipeline = Pipeline(
        platform="telegram",
        storage_type="sqlite",
        storage_config={"db_path": "data/telegram.db"},
        extractor_config={"config_path": "config.ini"}
    )
    
    # Backfill all messages from a group
    group_id = "1670178185"  # Bitcoinsensus channel ID
    
    print(f"\n🔄 Starting backfill for group {group_id}...")
    result = await pipeline.backfill(
        source_id=group_id,
        batch_size=500  # Process 500 messages at a time
    )
    
    print(f"✅ Backfill completed!")
    print(f"   Total messages: {result['total_messages']}")
    print(f"   Last message ID: {result['last_message_id']}")
    
    return result


async def example_incremental_sync():
    """Example: Sync only new messages since last run"""
    
    pipeline = Pipeline(
        platform="telegram",
        storage_type="sqlite",
        storage_config={"db_path": "data/telegram.db"}
    )
    
    group_id = "1670178185"
    
    print(f"\n🔄 Syncing new messages for group {group_id}...")
    result = await pipeline.sync(
        source_id=group_id,
        limit=2500  # Max 2500 new messages
    )
    
    print(f"✅ Sync completed!")
    print(f"   New messages: {result['new_messages']}")
    print(f"   Last message ID: {result['last_message_id']}")
    
    return result


async def example_polling():
    """Example: Poll for new messages every 5 minutes"""
    
    pipeline = Pipeline(
        platform="telegram",
        storage_type="sqlite",
        storage_config={"db_path": "data/telegram.db"}
    )
    
    group_id = "1670178185"
    
    print(f"\n🔄 Starting polling for group {group_id}...")
    print("   Polling every 60 seconds (press Ctrl+C to stop)")
    
    try:
        result = await pipeline.poll(
            source_id=group_id,
            interval_seconds=60,  # Poll every minute
            max_iterations=5,  # Stop after 5 polls (for demo)
            analyze=False  # Don't run analysis yet
        )
        
        print(f"\n✅ Polling completed!")
        print(f"   Total iterations: {result['iterations']}")
        print(f"   Total new messages: {result['total_messages']}")
        print(f"   Average per poll: {result['average_messages_per_poll']:.1f}")
        
    except KeyboardInterrupt:
        print("\n⏹️  Polling stopped by user")
    
    return result


async def example_auto_mode():
    """Example: Auto-detect whether to backfill or sync"""
    
    pipeline = Pipeline(
        platform="telegram",
        storage_type="sqlite",
        storage_config={"db_path": "data/telegram.db"}
    )
    
    group_id = "1670178185"
    
    print(f"\n🔄 Running pipeline in auto mode for group {group_id}...")
    
    # This will automatically:
    # - Backfill if no messages exist
    # - Sync if messages already exist
    result = await pipeline.run(
        source_id=group_id,
        mode="auto"
    )
    
    print(f"✅ Pipeline completed!")
    print(f"   Mode used: {result.get('mode', 'unknown')}")
    print(f"   Messages processed: {result.get('total_messages', result.get('new_messages', 0))}")
    
    return result


async def example_multiple_groups():
    """Example: Process multiple groups in parallel"""
    
    pipeline = Pipeline(
        platform="telegram",
        storage_type="sqlite",
        storage_config={"db_path": "data/telegram.db"}
    )
    
    group_ids = ["1670178185", "1827810223"]  # Multiple groups
    
    print(f"\n🔄 Processing {len(group_ids)} groups in parallel...")
    
    result = await pipeline.run_multiple(
        source_ids=group_ids,
        mode="incremental"
    )
    
    print(f"✅ Multi-group processing completed!")
    print(f"   Groups processed: {result['sources_processed']}")
    print(f"   Total messages: {result['total_messages']}")
    
    # Show individual results
    for i, group_result in enumerate(result['individual_results']):
        print(f"   Group {i+1}: {group_result.get('new_messages', 0)} messages")
    
    return result


async def example_with_statistics():
    """Example: Get statistics after extraction"""
    
    from convoetl.loaders import SQLiteLoader
    
    # Run extraction
    pipeline = Pipeline(platform="telegram", storage_type="sqlite")
    group_id = "1670178185"
    
    await pipeline.run(source_id=group_id)
    
    # Get statistics
    loader = SQLiteLoader({"db_path": "data/telegram.db"})
    stats = await loader.get_statistics(source_id=group_id)
    
    print(f"\n📊 Statistics for group {group_id}:")
    print(f"   Total messages: {stats['total_messages']}")
    print(f"   Total users: {stats['total_users']}")
    print(f"   First message: {stats['first_message']}")
    print(f"   Last message: {stats['last_message']}")
    
    await loader.close()
    return stats


def main():
    """Run examples"""
    
    print("=" * 60)
    print("ConvoETL Examples")
    print("=" * 60)
    
    examples = {
        "1": ("Backfill historical messages", example_backfill),
        "2": ("Incremental sync", example_incremental_sync),
        "3": ("Continuous polling", example_polling),
        "4": ("Auto mode (backfill or sync)", example_auto_mode),
        "5": ("Multiple groups in parallel", example_multiple_groups),
        "6": ("Get statistics", example_with_statistics),
    }
    
    print("\nAvailable examples:")
    for key, (description, _) in examples.items():
        print(f"  {key}. {description}")
    
    choice = input("\nSelect example to run (1-6): ").strip()
    
    if choice in examples:
        description, example_func = examples[choice]
        print(f"\nRunning: {description}")
        print("-" * 40)
        
        result = asyncio.run(example_func())
        
        print("\n" + "=" * 60)
        print("Example completed successfully!")
    else:
        print("Invalid choice. Please run again and select 1-6.")


if __name__ == "__main__":
    main()