"""
Prefect flows and tasks for message extraction
"""

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta, datetime
from typing import Optional, Dict, Any
import pandas as pd

from ..extractors import TelegramExtractor
from ..loaders import SQLiteLoader


@task(
    name="extract_messages",
    description="Extract messages from platform",
    retries=3,
    retry_delay_seconds=60,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(minutes=10)
)
async def extract_messages_task(
    platform: str,
    source_id: str,
    config: Dict[str, Any],
    after_id: Optional[int] = None,
    limit: Optional[int] = None
) -> pd.DataFrame:
    """
    Prefect task to extract messages from a platform
    
    Args:
        platform: Platform name (telegram, youtube, etc.)
        source_id: Source identifier (group_id, channel_id, etc.)
        config: Platform-specific configuration
        after_id: Extract messages after this ID
        limit: Maximum number of messages
        
    Returns:
        DataFrame with extracted messages
    """
    logger = get_run_logger()
    logger.info(f"Extracting messages from {platform} source {source_id}")
    
    # Create appropriate extractor
    if platform == "telegram":
        extractor = TelegramExtractor(config)
    else:
        raise ValueError(f"Unsupported platform: {platform}")
    
    try:
        # Extract messages
        messages_df = await extractor.extract_messages(
            source_id=source_id,
            after_id=after_id,
            limit=limit
        )
        
        logger.info(f"Extracted {len(messages_df)} messages")
        return messages_df
        
    finally:
        await extractor.close()


@task(
    name="load_messages",
    description="Load messages to storage",
    retries=3,
    retry_delay_seconds=30
)
async def load_messages_task(
    messages_df: pd.DataFrame,
    storage_config: Dict[str, Any]
) -> int:
    """
    Prefect task to load messages to storage
    
    Args:
        messages_df: DataFrame with messages to load
        storage_config: Storage configuration
        
    Returns:
        Number of messages loaded
    """
    logger = get_run_logger()
    
    if messages_df.empty:
        logger.info("No messages to load")
        return 0
    
    loader = SQLiteLoader(storage_config)
    
    try:
        # Store chat info if available
        if 'chat_info' in messages_df.attrs:
            await loader.store_chat_info(messages_df.attrs['chat_info'])
            logger.info(f"Stored chat info for {messages_df.attrs['chat_info'].get('title', 'unknown')}")
        
        count = await loader.store_messages(messages_df)
        logger.info(f"Loaded {count} messages to storage")
        return count
    finally:
        await loader.close()


@task(
    name="get_last_message_id",
    description="Get last processed message ID",
    retries=2
)
async def get_last_message_id_task(
    source_id: str,
    platform: str,
    storage_config: Dict[str, Any]
) -> Optional[int]:
    """
    Get the last processed message ID for incremental extraction
    
    Args:
        source_id: Source identifier
        platform: Platform name
        storage_config: Storage configuration
        
    Returns:
        Last message ID or None
    """
    logger = get_run_logger()
    
    loader = SQLiteLoader(storage_config)
    
    try:
        last_id = await loader.get_last_message_id(source_id, platform)
        logger.info(f"Last message ID for {platform}/{source_id}: {last_id}")
        return last_id
    finally:
        await loader.close()


@flow(
    name="backfill_messages",
    description="Backfill all historical messages from a source"
)
async def backfill_flow(
    platform: str,
    source_id: str,
    extractor_config: Dict[str, Any],
    storage_config: Dict[str, Any],
    batch_size: int = 1000
) -> Dict[str, Any]:
    """
    Prefect flow for backfilling historical messages
    
    Args:
        platform: Platform name
        source_id: Source identifier
        extractor_config: Platform-specific extractor configuration
        storage_config: Storage configuration
        batch_size: Messages per batch
        
    Returns:
        Flow results with statistics
    """
    logger = get_run_logger()
    logger.info(f"Starting backfill for {platform}/{source_id}")
    
    total_messages = 0
    last_message_id = 0
    
    while True:
        # Extract batch
        messages_df = await extract_messages_task(
            platform=platform,
            source_id=source_id,
            config=extractor_config,
            after_id=last_message_id,
            limit=batch_size
        )
        
        if messages_df.empty:
            logger.info("No more messages to extract")
            break
        
        # Load batch
        count = await load_messages_task(messages_df, storage_config)
        total_messages += count
        
        # Update last message ID for next batch
        last_message_id = int(messages_df['message_id'].max())
        logger.info(f"Processed batch up to message {last_message_id}, total: {total_messages}")
        
        # If we got less than batch_size, we're done
        if len(messages_df) < batch_size:
            break
    
    logger.info(f"Backfill completed: {total_messages} messages processed")
    
    return {
        "status": "completed",
        "total_messages": total_messages,
        "last_message_id": last_message_id,
        "source_id": source_id,
        "platform": platform
    }


@flow(
    name="incremental_sync",
    description="Sync new messages since last run"
)
async def incremental_flow(
    platform: str,
    source_id: str,
    extractor_config: Dict[str, Any],
    storage_config: Dict[str, Any],
    limit: int = 5000
) -> Dict[str, Any]:
    """
    Prefect flow for incremental message synchronization
    
    Args:
        platform: Platform name
        source_id: Source identifier
        extractor_config: Platform-specific configuration
        storage_config: Storage configuration
        limit: Maximum messages to sync
        
    Returns:
        Flow results with statistics
    """
    logger = get_run_logger()
    logger.info(f"Starting incremental sync for {platform}/{source_id}")
    
    # Get last processed message ID
    last_id = await get_last_message_id_task(source_id, platform, storage_config)
    
    if last_id:
        logger.info(f"Syncing messages after ID {last_id}")
    else:
        logger.info("No previous messages found, starting from beginning")
    
    # Extract new messages
    messages_df = await extract_messages_task(
        platform=platform,
        source_id=source_id,
        config=extractor_config,
        after_id=last_id,
        limit=limit
    )
    
    # Load new messages
    count = await load_messages_task(messages_df, storage_config)
    
    new_last_id = int(messages_df['message_id'].max()) if not messages_df.empty else last_id
    
    logger.info(f"Incremental sync completed: {count} new messages")
    
    return {
        "status": "completed",
        "new_messages": count,
        "last_message_id": new_last_id,
        "source_id": source_id,
        "platform": platform
    }