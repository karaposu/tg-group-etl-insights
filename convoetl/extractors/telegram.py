"""
Telegram extractor using tgdata
"""

import sys
import os
from pathlib import Path
from typing import Optional, Dict, Any
import pandas as pd
from datetime import datetime
import logging

# Add parent directories to path for tgdata import
# Try multiple possible locations for tgdata
possible_paths = [
    Path(__file__).parent.parent.parent.parent / "telegram-group-scraper",
    Path.home() / "Desktop/projects/telegram-group-scraper",
    Path.cwd().parent / "telegram-group-scraper",
]

for path in possible_paths:
    if path.exists():
        sys.path.insert(0, str(path))
        break

try:
    from tgdata import TgData
except ImportError:
    print("Error: Could not find tgdata module.")
    print("Please ensure telegram-group-scraper is in one of these locations:")
    for path in possible_paths:
        print(f"  - {path}")
    raise
from .base import BaseExtractor

logger = logging.getLogger(__name__)


class TelegramExtractor(BaseExtractor):
    """
    Extractor for Telegram messages using tgdata
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Telegram extractor
        
        Args:
            config: Configuration with 'config_path' for tgdata config file
        """
        super().__init__(config)
        self.tg = None
        self._initialize_tgdata()
    
    def _validate_config(self):
        """Validate Telegram configuration"""
        if 'config_path' not in self.config:
            raise ValueError("config_path is required for TelegramExtractor")
    
    def _initialize_tgdata(self):
        """Initialize tgdata connection"""
        try:
            # Always use the telegram-group-scraper directory for config and session
            # This ensures we use the existing session with cached entities
            tg_scraper_dir = Path.home() / "Desktop/projects/telegram-group-scraper"
            config_path = tg_scraper_dir / "config.ini"
            
            if not config_path.exists():
                # Fallback to provided config path
                config_path = self.config.get('config_path', 'config.ini')
                logger.warning(f"Using fallback config path: {config_path}")
            
            # Change to telegram-group-scraper directory to ensure session file is found
            import os
            original_cwd = os.getcwd()
            os.chdir(tg_scraper_dir)
            
            logger.info(f"Using config path: {config_path}")
            self.tg = TgData("config.ini")  # Use relative path since we're in the right directory
            logger.info("TgData initialized successfully")
            
            # Change back to original directory
            os.chdir(original_cwd)
        except Exception as e:
            logger.error(f"Failed to initialize TgData: {e}")
            raise
    
    async def extract_messages(
        self,
        source_id: str,
        after_id: Optional[int] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None,
        batch_size: int = 1000
    ) -> pd.DataFrame:
        """
        Extract messages from Telegram group/channel
        
        Args:
            source_id: Telegram group/channel ID or username
            after_id: Extract messages after this message ID
            start_date: Extract messages after this date
            end_date: Extract messages before this date
            limit: Maximum number of messages to extract
            batch_size: Batch size for extraction
            
        Returns:
            DataFrame with messages
        """
        try:
            # Convert string ID to int for tgdata
            group_id = int(source_id)
            logger.info(f"Extracting messages from Telegram group {group_id}")
            
            logger.info(f"Parameters: after_id={after_id}, limit={limit}, batch_size={batch_size}")
            
            # Use tgdata to get messages
            messages_df = await self.tg.get_messages(
                group_id=group_id,
                after_id=after_id or 0,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
                batch_size=batch_size,
                batch_delay=1.0,  # Rate limiting
                rate_limit_strategy='wait',
                with_progress=True
            )
            
            if messages_df.empty:
                logger.info("No messages extracted")
                return pd.DataFrame()
            
            logger.info(f"Extracted {len(messages_df)} messages")
            
            # Standardize column names for ConvoETL
            messages_df = self._standardize_telegram_df(messages_df, group_id)
            
            # Also extract and store chat info
            await self.extract_chat_info(str(group_id))
            
            return messages_df
            
        except Exception as e:
            logger.error(f"Error extracting messages: {e}")
            raise
    
    async def extract_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """
        Extract information about a chat/group/channel
        
        Args:
            chat_id: Chat ID
            
        Returns:
            Dictionary with chat information
        """
        try:
            # Get groups list to find chat info
            groups_df = await self.tg.list_groups()
            
            # Find the specific group
            group_id = int(chat_id)
            group_info = groups_df[groups_df['GroupID'] == group_id]
            
            if not group_info.empty:
                info = group_info.iloc[0]
                return {
                    'chat_id': str(chat_id),
                    'platform': self.platform_name,
                    'title': info.get('Title', ''),
                    'username': info.get('Username', ''),
                    'chat_type': 'channel' if info.get('IsChannel', False) else 'group',
                    'participants_count': info.get('ParticipantsCount', 0),
                    'is_verified': False,
                    'metadata': info.to_json()
                }
            
            return {
                'chat_id': str(chat_id),
                'platform': self.platform_name,
                'title': f'Chat {chat_id}',
                'chat_type': 'unknown'
            }
            
        except Exception as e:
            logger.warning(f"Could not extract chat info: {e}")
            return {
                'chat_id': str(chat_id),
                'platform': self.platform_name
            }
    
    def _standardize_telegram_df(self, df: pd.DataFrame, chat_id: int) -> pd.DataFrame:
        """
        Standardize Telegram DataFrame to ConvoETL format
        
        Args:
            df: TgData DataFrame
            chat_id: The chat/group ID
            
        Returns:
            Standardized DataFrame
        """
        # Map TgData columns to ConvoETL standard columns
        column_mapping = {
            'MessageId': 'message_id',
            'SenderId': 'user_id',
            'Username': 'username',
            'Name': 'user_name',
            'Message': 'message_text',
            'Date': 'timestamp',
            'ReplyToId': 'reply_to_id'
        }
        
        # Rename columns that we care about
        df_standardized = df.rename(columns=column_mapping)
        
        # Drop any columns not in our schema
        columns_to_keep = list(column_mapping.values()) + ['platform', 'chat_id', 'source_id', 'message_type']
        
        # Add platform and chat information
        df_standardized['platform'] = self.platform_name
        df_standardized['chat_id'] = str(chat_id)
        df_standardized['source_id'] = str(chat_id)  # Keep for backward compatibility
        df_standardized['message_type'] = 'text'  # Default, can be enhanced
        
        # Keep only the columns we need
        df_standardized = df_standardized[df_standardized.columns.intersection(columns_to_keep)]
        
        # Ensure timestamp is datetime
        if 'timestamp' in df_standardized.columns:
            df_standardized['timestamp'] = pd.to_datetime(df_standardized['timestamp'])
        
        return df_standardized
    
    async def get_message_count(self, source_id: str) -> int:
        """
        Get total message count for a Telegram group
        
        Args:
            source_id: Telegram group ID
            
        Returns:
            Total message count
        """
        try:
            group_id = int(source_id)
            count = await self.tg.get_message_count(group_id=group_id)
            logger.info(f"Group {group_id} has {count} messages")
            return count
        except Exception as e:
            logger.error(f"Error getting message count: {e}")
            raise
    
    async def get_source_info(self, source_id: str) -> Dict[str, Any]:
        """
        Get information about a Telegram group
        
        Args:
            source_id: Telegram group ID
            
        Returns:
            Dictionary with group information
        """
        try:
            # List all groups and find the specific one
            groups_df = await self.tg.list_groups()
            
            group_id = int(source_id)
            group_info = groups_df[groups_df['GroupID'] == group_id]
            
            if group_info.empty:
                return {'error': f'Group {group_id} not found'}
            
            return group_info.iloc[0].to_dict()
            
        except Exception as e:
            logger.error(f"Error getting group info: {e}")
            raise
    
    async def close(self):
        """Clean up TgData connection"""
        if self.tg:
            try:
                await self.tg.close()
                logger.info("TgData connection closed")
            except Exception as e:
                logger.error(f"Error closing TgData connection: {e}")
    
    @property
    def platform_name(self) -> str:
        """Return platform name"""
        return "telegram"