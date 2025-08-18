"""
Main orchestration flows with Prefect
"""

from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from datetime import datetime, timedelta
import asyncio
from typing import Dict, Any, Optional

from .extraction import incremental_flow, backfill_flow


@flow(
    name="convoetl_main",
    description="Main ConvoETL orchestration flow",
    task_runner=ConcurrentTaskRunner(max_workers=3)
)
async def convoetl_flow(
    platform: str,
    source_id: str,
    extractor_config: Dict[str, Any],
    storage_config: Dict[str, Any],
    mode: str = "auto",
    analyze: bool = False
) -> Dict[str, Any]:
    """
    Main ConvoETL flow that determines whether to backfill or sync
    
    Args:
        platform: Platform name (telegram, youtube, etc.)
        source_id: Source identifier
        extractor_config: Platform-specific configuration
        storage_config: Storage configuration
        mode: 'backfill', 'incremental', or 'auto' (auto-detect)
        analyze: Whether to run analysis after extraction
        
    Returns:
        Flow execution results
    """
    logger = get_run_logger()
    logger.info(f"Starting ConvoETL flow for {platform}/{source_id} in {mode} mode")
    
    # Determine mode if auto
    if mode == "auto":
        from ..loaders import SQLiteLoader
        loader = SQLiteLoader(storage_config)
        try:
            last_id = await loader.get_last_message_id(source_id, platform)
            mode = "incremental" if last_id else "backfill"
            logger.info(f"Auto-detected mode: {mode}")
        finally:
            await loader.close()
    
    # Run appropriate flow
    if mode == "backfill":
        result = await backfill_flow(
            platform=platform,
            source_id=source_id,
            extractor_config=extractor_config,
            storage_config=storage_config
        )
    else:
        result = await incremental_flow(
            platform=platform,
            source_id=source_id,
            extractor_config=extractor_config,
            storage_config=storage_config
        )
    
    # Run analysis if requested
    if analyze and result.get("new_messages", 0) > 0:
        logger.info("Running analysis on new messages")
        # TODO: Add analysis flow
        # await analysis_flow(source_id, platform, storage_config)
    
    return result


@flow(
    name="polling_loop",
    description="Continuous polling for new messages"
)
async def polling_flow(
    platform: str,
    source_id: str,
    extractor_config: Dict[str, Any],
    storage_config: Dict[str, Any],
    interval_seconds: int = 300,
    max_iterations: Optional[int] = None,
    analyze: bool = False
) -> Dict[str, Any]:
    """
    Polling flow that continuously checks for new messages
    
    Args:
        platform: Platform name
        source_id: Source identifier
        extractor_config: Platform configuration
        storage_config: Storage configuration
        interval_seconds: Seconds between polls (default 5 minutes)
        max_iterations: Maximum number of polls (None for infinite)
        analyze: Whether to run analysis after each sync
        
    Returns:
        Polling statistics
    """
    logger = get_run_logger()
    logger.info(f"Starting polling for {platform}/{source_id} every {interval_seconds}s")
    
    iteration = 0
    total_messages = 0
    start_time = datetime.now()
    
    while True:
        iteration += 1
        logger.info(f"Polling iteration {iteration}")
        
        try:
            # Run incremental sync
            result = await incremental_flow(
                platform=platform,
                source_id=source_id,
                extractor_config=extractor_config,
                storage_config=storage_config
            )
            
            new_messages = result.get("new_messages", 0)
            total_messages += new_messages
            
            if new_messages > 0:
                logger.info(f"Found {new_messages} new messages")
                
                # Run analysis if requested and there are new messages
                if analyze:
                    logger.info("Running analysis on new messages")
                    # TODO: Add analysis
            else:
                logger.info("No new messages found")
            
        except Exception as e:
            logger.error(f"Error in polling iteration {iteration}: {e}")
            # Continue polling despite errors
        
        # Check if we should stop
        if max_iterations and iteration >= max_iterations:
            logger.info(f"Reached maximum iterations ({max_iterations})")
            break
        
        # Wait for next poll
        logger.info(f"Waiting {interval_seconds} seconds until next poll...")
        await asyncio.sleep(interval_seconds)
    
    # Calculate statistics
    duration = (datetime.now() - start_time).total_seconds()
    
    return {
        "status": "completed",
        "iterations": iteration,
        "total_messages": total_messages,
        "duration_seconds": duration,
        "average_messages_per_poll": total_messages / iteration if iteration > 0 else 0
    }


@flow(
    name="multi_source_sync",
    description="Sync multiple sources in parallel"
)
async def multi_source_flow(
    sources: list[Dict[str, Any]],
    storage_config: Dict[str, Any],
    mode: str = "incremental"
) -> Dict[str, Any]:
    """
    Flow to sync multiple sources in parallel
    
    Args:
        sources: List of source configurations
            [{"platform": "telegram", "source_id": "123", "config": {...}}, ...]
        storage_config: Storage configuration
        mode: 'backfill' or 'incremental'
        
    Returns:
        Combined results from all sources
    """
    logger = get_run_logger()
    logger.info(f"Starting multi-source sync for {len(sources)} sources")
    
    # Create tasks for each source
    tasks = []
    for source in sources:
        if mode == "backfill":
            task = backfill_flow.submit(
                platform=source["platform"],
                source_id=source["source_id"],
                extractor_config=source["config"],
                storage_config=storage_config
            )
        else:
            task = incremental_flow.submit(
                platform=source["platform"],
                source_id=source["source_id"],
                extractor_config=source["config"],
                storage_config=storage_config
            )
        tasks.append(task)
    
    # Wait for all tasks to complete
    results = []
    for task in tasks:
        result = await task
        results.append(result)
    
    # Aggregate results
    total_messages = sum(r.get("new_messages", r.get("total_messages", 0)) for r in results)
    
    logger.info(f"Multi-source sync completed: {total_messages} total messages")
    
    return {
        "status": "completed",
        "sources_processed": len(sources),
        "total_messages": total_messages,
        "individual_results": results
    }