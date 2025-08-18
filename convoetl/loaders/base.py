"""
Base loader interface for all storage backends
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
import pandas as pd
from datetime import datetime


class BaseLoader(ABC):
    """
    Abstract base class for all data loaders
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize loader with configuration
        
        Args:
            config: Storage-specific configuration
        """
        self.config = config
        self._validate_config()
        self._initialize_storage()
    
    @abstractmethod
    def _validate_config(self):
        """Validate configuration for the specific storage backend"""
        pass
    
    @abstractmethod
    def _initialize_storage(self):
        """Initialize storage connection and create tables if needed"""
        pass
    
    @abstractmethod
    async def store_messages(self, messages_df: pd.DataFrame) -> int:
        """
        Store messages in the storage backend
        
        Args:
            messages_df: DataFrame with messages to store
            
        Returns:
            Number of messages stored
        """
        pass
    
    @abstractmethod
    async def get_last_message_id(self, source_id: str, platform: str) -> Optional[int]:
        """
        Get the last processed message ID for a source
        
        Args:
            source_id: Source identifier (group_id, channel_id, etc.)
            platform: Platform name (telegram, youtube, etc.)
            
        Returns:
            Last message ID or None if no messages exist
        """
        pass
    
    @abstractmethod
    async def get_messages(
        self,
        source_id: Optional[str] = None,
        platform: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Retrieve messages from storage
        
        Args:
            source_id: Filter by source
            platform: Filter by platform
            start_date: Filter messages after this date
            end_date: Filter messages before this date
            limit: Maximum number of messages to retrieve
            
        Returns:
            DataFrame with messages
        """
        pass
    
    @abstractmethod
    async def store_users(self, users_df: pd.DataFrame) -> int:
        """
        Store or update user information
        
        Args:
            users_df: DataFrame with user information
            
        Returns:
            Number of users stored/updated
        """
        pass
    
    @abstractmethod
    async def store_source_info(self, source_info: Dict[str, Any]) -> bool:
        """
        Store information about a source (group, channel, etc.)
        
        Args:
            source_info: Dictionary with source information
            
        Returns:
            Success status
        """
        pass
    
    @abstractmethod
    async def get_statistics(self, source_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get statistics about stored data
        
        Args:
            source_id: Optional source filter
            
        Returns:
            Dictionary with statistics
        """
        pass
    
    @abstractmethod
    async def close(self):
        """Clean up resources"""
        pass
    
    @property
    @abstractmethod
    def storage_type(self) -> str:
        """Return the storage type name"""
        pass