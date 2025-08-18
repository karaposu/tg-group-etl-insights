"""
Base extractor interface for all platforms
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
import pandas as pd
from datetime import datetime


class BaseExtractor(ABC):
    """
    Abstract base class for all platform extractors
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize extractor with configuration
        
        Args:
            config: Platform-specific configuration
        """
        self.config = config
        self._validate_config()
    
    @abstractmethod
    def _validate_config(self):
        """Validate configuration for the specific platform"""
        pass
    
    @abstractmethod
    async def extract_messages(
        self,
        source_id: str,
        after_id: Optional[int] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Extract messages from the platform
        
        Args:
            source_id: Platform-specific source identifier (group_id, channel_id, etc.)
            after_id: Extract messages after this ID (for incremental extraction)
            start_date: Extract messages after this date
            end_date: Extract messages before this date
            limit: Maximum number of messages to extract
            
        Returns:
            DataFrame with extracted messages
        """
        pass
    
    @abstractmethod
    async def get_message_count(self, source_id: str) -> int:
        """
        Get total message count for a source
        
        Args:
            source_id: Platform-specific source identifier
            
        Returns:
            Total message count
        """
        pass
    
    @abstractmethod
    async def get_source_info(self, source_id: str) -> Dict[str, Any]:
        """
        Get information about the source (group, channel, etc.)
        
        Args:
            source_id: Platform-specific source identifier
            
        Returns:
            Dictionary with source information
        """
        pass
    
    @abstractmethod
    async def close(self):
        """Clean up resources"""
        pass
    
    def standardize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize DataFrame columns across all platforms
        
        Args:
            df: Platform-specific DataFrame
            
        Returns:
            Standardized DataFrame with common columns
        """
        # Define standard columns that all platforms should have
        standard_columns = {
            'message_id': 'message_id',
            'user_id': 'user_id',
            'username': 'username',
            'message_text': 'message_text',
            'timestamp': 'timestamp',
            'platform': 'platform',
            'source_id': 'source_id',
            'reply_to_id': 'reply_to_id',
            'message_type': 'message_type'
        }
        
        # Rename columns to standard names if they exist
        df_standardized = df.copy()
        
        # Add platform column
        df_standardized['platform'] = self.platform_name
        
        return df_standardized
    
    @property
    @abstractmethod
    def platform_name(self) -> str:
        """Return the platform name"""
        pass