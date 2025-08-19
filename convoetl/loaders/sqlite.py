"""
SQLite loader for local storage
"""

import sqlite3
from pathlib import Path
from typing import Optional, Dict, Any
import pandas as pd
from datetime import datetime
import logging
import json

from .base import BaseLoader

logger = logging.getLogger(__name__)


class SQLiteLoader(BaseLoader):
    """
    SQLite storage backend for ConvoETL
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize SQLite loader
        
        Args:
            config: Configuration with 'db_path' for database file location
        """
        self.conn = None
        super().__init__(config)
    
    def _validate_config(self):
        """Validate SQLite configuration"""
        if 'db_path' not in self.config:
            # Default to data/convoetl.db
            self.config['db_path'] = 'data/convoetl.db'
        
        # Ensure directory exists
        db_path = Path(self.config['db_path'])
        db_path.parent.mkdir(parents=True, exist_ok=True)
    
    def _initialize_storage(self):
        """Initialize SQLite connection and create tables"""
        try:
            db_path = self.config['db_path']
            self.conn = sqlite3.connect(db_path, check_same_thread=False)
            logger.info(f"Connected to SQLite database: {db_path}")
            
            # Create tables
            self._create_tables()
            
        except Exception as e:
            logger.error(f"Failed to initialize SQLite: {e}")
            raise
    
    def _create_tables(self):
        """Create necessary tables if they don't exist"""
        cursor = self.conn.cursor()
        
        # Chats/Sources table (groups, channels, etc.)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chats (
                chat_id TEXT NOT NULL,
                platform TEXT NOT NULL,
                title TEXT,
                username TEXT,
                description TEXT,
                chat_type TEXT,  -- 'group', 'channel', 'supergroup', 'private'
                participants_count INTEGER,
                is_active BOOLEAN DEFAULT 1,
                is_verified BOOLEAN DEFAULT 0,
                metadata TEXT,  -- JSON string for platform-specific data
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                first_message_at DATETIME,
                last_message_at DATETIME,
                PRIMARY KEY (chat_id, platform)
            )
        """)
        
        # Users table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER NOT NULL,
                platform TEXT NOT NULL,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                display_name TEXT,
                is_bot BOOLEAN DEFAULT 0,
                is_verified BOOLEAN DEFAULT 0,
                first_seen_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_seen_at DATETIME,
                total_messages INTEGER DEFAULT 0,
                metadata TEXT,  -- JSON for additional user data
                PRIMARY KEY (user_id, platform)
            )
        """)
        
        # Messages table with proper foreign keys
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                message_id INTEGER NOT NULL,
                platform TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT,
                user_name TEXT,
                message_text TEXT,
                timestamp DATETIME,
                reply_to_id INTEGER,
                message_type TEXT DEFAULT 'text',
                extracted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (message_id, platform, chat_id),
                FOREIGN KEY (chat_id, platform) REFERENCES chats(chat_id, platform),
                FOREIGN KEY (user_id, platform) REFERENCES users(user_id, platform)
            )
        """)
        
        # Create indexes for performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_chat ON messages(chat_id, platform)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_user ON messages(user_id, platform)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp)")
        
        # Keep sources as alias for backward compatibility
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sources (
                source_id TEXT,
                platform TEXT NOT NULL,
                title TEXT,
                description TEXT,
                participants_count INTEGER,
                is_active BOOLEAN DEFAULT 1,
                metadata TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (source_id, platform)
            )
        """)
        
        # ETL runs table for tracking
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS etl_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                source_id TEXT NOT NULL,
                run_type TEXT,  -- 'backfill' or 'incremental'
                start_message_id INTEGER,
                end_message_id INTEGER,
                messages_processed INTEGER,
                started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                completed_at DATETIME,
                status TEXT DEFAULT 'running',
                error_message TEXT
            )
        """)
        
        # Create indexes for performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp)")
        
        self.conn.commit()
        logger.info("Database tables created/verified")
    
    async def store_messages(self, messages_df: pd.DataFrame) -> int:
        """
        Store messages in SQLite
        
        Args:
            messages_df: DataFrame with messages
            
        Returns:
            Number of messages stored
        """
        if messages_df.empty:
            return 0
        
        try:
            # Add extraction timestamp
            messages_df['extracted_at'] = datetime.now()
            
            # Store messages with replace to handle duplicates
            messages_df.to_sql(
                'messages',
                self.conn,
                if_exists='append',
                index=False,
                method='multi'
            )
            
            # Update user statistics
            await self._update_user_stats(messages_df)
            
            self.conn.commit()
            
            count = len(messages_df)
            logger.info(f"Stored {count} messages to SQLite")
            return count
            
        except Exception as e:
            logger.error(f"Error storing messages: {e}")
            self.conn.rollback()
            raise
    
    async def _update_user_stats(self, messages_df: pd.DataFrame):
        """Update user statistics based on new messages"""
        if 'user_id' not in messages_df.columns:
            return
        
        cursor = self.conn.cursor()
        
        # Group by user and platform
        user_stats = messages_df.groupby(['user_id', 'platform']).agg({
            'username': 'first',
            'user_name': 'first',
            'message_id': 'count'
        }).reset_index()
        
        for _, user in user_stats.iterrows():
            # Insert or update user
            cursor.execute("""
                INSERT INTO users (user_id, platform, username, first_name, last_seen_at, total_messages)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id, platform) DO UPDATE SET
                    username = COALESCE(excluded.username, username),
                    first_name = COALESCE(excluded.first_name, first_name),
                    last_seen_at = excluded.last_seen_at,
                    total_messages = total_messages + excluded.total_messages
            """, (
                user['user_id'],
                user['platform'],
                user.get('username'),
                user.get('user_name'),
                datetime.now(),
                user['message_id']  # count of messages
            ))
    
    async def get_last_message_id(self, source_id: str, platform: str) -> Optional[int]:
        """
        Get the last message ID for a source
        
        Args:
            source_id: Source identifier
            platform: Platform name
            
        Returns:
            Last message ID or None
        """
        cursor = self.conn.cursor()
        result = cursor.execute("""
            SELECT MAX(message_id) 
            FROM messages 
            WHERE chat_id = ? AND platform = ?
        """, (source_id, platform)).fetchone()
        
        return result[0] if result and result[0] else None
    
    async def get_messages(
        self,
        source_id: Optional[str] = None,
        platform: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Retrieve messages from SQLite
        
        Args:
            source_id: Filter by source
            platform: Filter by platform
            start_date: Filter messages after this date
            end_date: Filter messages before this date
            limit: Maximum number of messages
            
        Returns:
            DataFrame with messages
        """
        query = "SELECT * FROM messages WHERE 1=1"
        params = []
        
        if source_id:
            query += " AND chat_id = ?"
            params.append(source_id)
        
        if platform:
            query += " AND platform = ?"
            params.append(platform)
        
        if start_date:
            query += " AND timestamp >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND timestamp <= ?"
            params.append(end_date)
        
        query += " ORDER BY timestamp DESC"
        
        if limit:
            query += f" LIMIT {limit}"
        
        return pd.read_sql_query(query, self.conn, params=params)
    
    async def store_users(self, users_df: pd.DataFrame) -> int:
        """Store or update user information"""
        if users_df.empty:
            return 0
        
        users_df.to_sql(
            'users',
            self.conn,
            if_exists='replace',
            index=False
        )
        
        self.conn.commit()
        return len(users_df)
    
    async def store_chat_info(self, chat_info: Dict[str, Any]) -> bool:
        """Store chat (group/channel) information"""
        cursor = self.conn.cursor()
        
        # Convert metadata to JSON string if present
        metadata = json.dumps(chat_info.get('metadata', {})) if 'metadata' in chat_info else None
        
        cursor.execute("""
            INSERT OR REPLACE INTO chats 
            (chat_id, platform, title, username, description, chat_type, 
             participants_count, is_verified, metadata, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            chat_info['chat_id'],
            chat_info['platform'],
            chat_info.get('title'),
            chat_info.get('username'),
            chat_info.get('description'),
            chat_info.get('chat_type', 'group'),
            chat_info.get('participants_count'),
            chat_info.get('is_verified', False),
            metadata,
            datetime.now()
        ))
        
        self.conn.commit()
        return True
    
    async def store_source_info(self, source_info: Dict[str, Any]) -> bool:
        """Store source (group/channel) information - kept for backward compatibility"""
        cursor = self.conn.cursor()
        
        # Convert metadata to JSON string
        metadata = json.dumps(source_info.get('metadata', {}))
        
        cursor.execute("""
            INSERT OR REPLACE INTO sources 
            (source_id, platform, title, description, participants_count, metadata, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            source_info['source_id'],
            source_info['platform'],
            source_info.get('title'),
            source_info.get('description'),
            source_info.get('participants_count'),
            metadata,
            datetime.now()
        ))
        
        self.conn.commit()
        return True
    
    async def get_statistics(self, source_id: Optional[str] = None) -> Dict[str, Any]:
        """Get statistics about stored data"""
        cursor = self.conn.cursor()
        
        stats = {}
        
        # Total messages
        query = "SELECT COUNT(*) FROM messages"
        params = []
        if source_id:
            query += " WHERE chat_id = ?"
            params.append(source_id)
        
        stats['total_messages'] = cursor.execute(query, params).fetchone()[0]
        
        # Total users
        query = "SELECT COUNT(DISTINCT user_id) FROM messages"
        if source_id:
            query += " WHERE source_id = ?"
        
        stats['total_users'] = cursor.execute(query, params).fetchone()[0]
        
        # Date range
        query = "SELECT MIN(timestamp), MAX(timestamp) FROM messages"
        if source_id:
            query += " WHERE source_id = ?"
        
        result = cursor.execute(query, params).fetchone()
        stats['first_message'] = result[0]
        stats['last_message'] = result[1]
        
        # Sources count
        stats['total_sources'] = cursor.execute("SELECT COUNT(*) FROM sources").fetchone()[0]
        
        return stats
    
    async def close(self):
        """Close SQLite connection"""
        if self.conn:
            self.conn.close()
            logger.info("SQLite connection closed")
    
    @property
    def storage_type(self) -> str:
        """Return storage type"""
        return "sqlite"