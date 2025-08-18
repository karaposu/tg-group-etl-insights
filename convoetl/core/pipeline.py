"""
Pipeline class that provides a simple interface to Prefect flows
"""

import asyncio
from typing import Dict, Any, Optional
from pathlib import Path
import logging

from ..flows.orchestration import convoetl_flow, polling_flow, multi_source_flow

logger = logging.getLogger(__name__)


class Pipeline:
    """
    ConvoETL Pipeline - Simple interface for extraction and loading
    """
    
    def __init__(
        self,
        platform: str = "telegram",
        storage_type: str = "sqlite",
        storage_config: Optional[Dict[str, Any]] = None,
        extractor_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize ConvoETL pipeline
        
        Args:
            platform: Platform to extract from (telegram, youtube, etc.)
            storage_type: Storage backend (sqlite, postgres, bigquery)
            storage_config: Storage-specific configuration
            extractor_config: Platform-specific extractor configuration
        """
        self.platform = platform
        self.storage_type = storage_type
        
        # Default configurations
        self.storage_config = storage_config or self._get_default_storage_config()
        self.extractor_config = extractor_config or self._get_default_extractor_config()
        
        logger.info(f"Pipeline initialized for {platform} → {storage_type}")
    
    def _get_default_storage_config(self) -> Dict[str, Any]:
        """Get default storage configuration"""
        if self.storage_type == "sqlite":
            return {"db_path": "data/convoetl.db"}
        elif self.storage_type == "postgres":
            return {
                "host": "localhost",
                "port": 5432,
                "database": "convoetl",
                "user": "postgres"
            }
        elif self.storage_type == "bigquery":
            return {
                "project_id": "your-project",
                "dataset": "convoetl"
            }
        else:
            raise ValueError(f"Unsupported storage type: {self.storage_type}")
    
    def _get_default_extractor_config(self) -> Dict[str, Any]:
        """Get default extractor configuration"""
        if self.platform == "telegram":
            # Look for config.ini in project root
            config_path = Path("config.ini")
            if not config_path.exists():
                # Try parent directory (for tgdata)
                config_path = Path("../telegram-group-scraper/config.ini")
            
            return {"config_path": str(config_path)}
        else:
            return {}
    
    async def backfill(
        self,
        source_id: str,
        batch_size: int = 1000
    ) -> Dict[str, Any]:
        """
        Backfill all historical messages from a source
        
        Args:
            source_id: Source identifier (group_id, channel_id, etc.)
            batch_size: Messages per batch
            
        Returns:
            Backfill results
        """
        logger.info(f"Starting backfill for {self.platform}/{source_id}")
        
        from ..flows.extraction import backfill_flow
        
        result = await backfill_flow(
            platform=self.platform,
            source_id=source_id,
            extractor_config=self.extractor_config,
            storage_config=self.storage_config,
            batch_size=batch_size
        )
        
        logger.info(f"Backfill completed: {result.get('total_messages', 0)} messages")
        return result
    
    async def sync(
        self,
        source_id: str,
        limit: int = 5000
    ) -> Dict[str, Any]:
        """
        Sync new messages since last run
        
        Args:
            source_id: Source identifier
            limit: Maximum messages to sync
            
        Returns:
            Sync results
        """
        logger.info(f"Starting sync for {self.platform}/{source_id}")
        
        from ..flows.extraction import incremental_flow
        
        result = await incremental_flow(
            platform=self.platform,
            source_id=source_id,
            extractor_config=self.extractor_config,
            storage_config=self.storage_config,
            limit=limit
        )
        
        logger.info(f"Sync completed: {result.get('new_messages', 0)} new messages")
        return result
    
    async def poll(
        self,
        source_id: str,
        interval_seconds: int = 300,
        max_iterations: Optional[int] = None,
        analyze: bool = False
    ) -> Dict[str, Any]:
        """
        Start polling for new messages
        
        Args:
            source_id: Source identifier
            interval_seconds: Seconds between polls (default 5 minutes)
            max_iterations: Maximum polls (None for infinite)
            analyze: Whether to run analysis after each sync
            
        Returns:
            Polling statistics
        """
        logger.info(f"Starting polling for {self.platform}/{source_id} every {interval_seconds}s")
        
        result = await polling_flow(
            platform=self.platform,
            source_id=source_id,
            extractor_config=self.extractor_config,
            storage_config=self.storage_config,
            interval_seconds=interval_seconds,
            max_iterations=max_iterations,
            analyze=analyze
        )
        
        return result
    
    async def run(
        self,
        source_id: str,
        mode: str = "auto",
        analyze: bool = False
    ) -> Dict[str, Any]:
        """
        Run the pipeline (auto-detect whether to backfill or sync)
        
        Args:
            source_id: Source identifier
            mode: 'backfill', 'incremental', or 'auto'
            analyze: Whether to run analysis
            
        Returns:
            Execution results
        """
        result = await convoetl_flow(
            platform=self.platform,
            source_id=source_id,
            extractor_config=self.extractor_config,
            storage_config=self.storage_config,
            mode=mode,
            analyze=analyze
        )
        
        return result
    
    async def run_multiple(
        self,
        source_ids: list[str],
        mode: str = "incremental"
    ) -> Dict[str, Any]:
        """
        Run pipeline for multiple sources in parallel
        
        Args:
            source_ids: List of source identifiers
            mode: 'backfill' or 'incremental'
            
        Returns:
            Combined results
        """
        sources = [
            {
                "platform": self.platform,
                "source_id": source_id,
                "config": self.extractor_config
            }
            for source_id in source_ids
        ]
        
        result = await multi_source_flow(
            sources=sources,
            storage_config=self.storage_config,
            mode=mode
        )
        
        return result
    
    def run_sync(self, source_id: str, **kwargs) -> Dict[str, Any]:
        """
        Synchronous wrapper for run method
        
        Args:
            source_id: Source identifier
            **kwargs: Additional arguments for run()
            
        Returns:
            Execution results
        """
        return asyncio.run(self.run(source_id, **kwargs))