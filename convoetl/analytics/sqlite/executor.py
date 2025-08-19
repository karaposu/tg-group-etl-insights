"""
SQLite analytics executor
"""

import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Dict, Any, Optional, Union, List
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class SQLiteAnalyticsExecutor:
    """Executes analytics queries on SQLite database"""
    
    def __init__(self, db_path: str):
        """
        Initialize SQLite executor
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {db_path}")
            
        self.engine = create_engine(f'sqlite:///{db_path}')
        logger.info(f"SQLite executor initialized with {db_path}")
    
    def execute_query(
        self, 
        query: str, 
        parameters: Optional[Dict[str, Any]] = None,
        return_type: str = 'dataframe'
    ) -> Union[pd.DataFrame, List[Dict], List[List]]:
        """
        Execute SQL query and return results
        
        Args:
            query: SQL query string
            parameters: Query parameters
            return_type: 'dataframe', 'dict', or 'raw'
            
        Returns:
            Query results as specified type
        """
        try:
            with self.engine.connect() as conn:
                result = pd.read_sql(
                    sql=text(query),
                    con=conn,
                    params=parameters or {}
                )
                
                logger.debug(f"Query executed, returned {len(result)} rows")
                
                if return_type == 'dict':
                    return result.to_dict(orient='records')
                elif return_type == 'raw':
                    return result.values.tolist()
                else:
                    return result
                    
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            logger.error(f"Query: {query[:200]}...")
            logger.error(f"Parameters: {parameters}")
            raise
    
    def execute_multiple(
        self,
        queries: Dict[str, tuple[str, Optional[Dict]]],
        return_type: str = 'dataframe'
    ) -> Dict[str, Any]:
        """
        Execute multiple queries and return results
        
        Args:
            queries: Dictionary of {name: (query, params)}
            return_type: Format for results
            
        Returns:
            Dictionary of {name: results}
        """
        results = {}
        
        for name, (query, params) in queries.items():
            try:
                logger.info(f"Executing query: {name}")
                results[name] = self.execute_query(query, params, return_type)
            except Exception as e:
                logger.error(f"Failed to execute {name}: {e}")
                results[name] = None
                
        return results
    
    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """
        Get information about a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            DataFrame with column information
        """
        query = f"PRAGMA table_info({table_name})"
        return self.execute_query(query)
    
    def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """
        Get basic statistics about a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table statistics
        """
        stats = {}
        
        # Row count
        count_query = f"SELECT COUNT(*) as count FROM {table_name}"
        result = self.execute_query(count_query, return_type='dict')
        stats['row_count'] = result[0]['count'] if result else 0
        
        # Table size
        size_query = """
            SELECT 
                name,
                SUM(pgsize) as size_bytes
            FROM dbstat
            WHERE name = :table_name
            GROUP BY name
        """
        try:
            result = self.execute_query(size_query, {'table_name': table_name}, return_type='dict')
            stats['size_bytes'] = result[0]['size_bytes'] if result else None
        except:
            stats['size_bytes'] = None
            
        return stats
    
    def create_indexes(self, indexes: Dict[str, str]):
        """
        Create indexes for better query performance
        
        Args:
            indexes: Dictionary of {index_name: create_index_sql}
        """
        with self.engine.connect() as conn:
            for index_name, create_sql in indexes.items():
                try:
                    conn.execute(text(create_sql))
                    conn.commit()
                    logger.info(f"Created index: {index_name}")
                except Exception as e:
                    if "already exists" in str(e):
                        logger.debug(f"Index {index_name} already exists")
                    else:
                        logger.error(f"Failed to create index {index_name}: {e}")
    
    def optimize_for_analytics(self):
        """Create indexes and optimize database for analytics queries"""
        indexes = {
            'idx_messages_chat': 'CREATE INDEX IF NOT EXISTS idx_messages_chat ON messages(chat_id)',
            'idx_messages_user': 'CREATE INDEX IF NOT EXISTS idx_messages_user ON messages(user_id)',
            'idx_messages_timestamp': 'CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp)',
            'idx_messages_reply': 'CREATE INDEX IF NOT EXISTS idx_messages_reply ON messages(reply_to_id)',
            'idx_messages_chat_timestamp': 'CREATE INDEX IF NOT EXISTS idx_messages_chat_timestamp ON messages(chat_id, timestamp)',
            'idx_users_platform': 'CREATE INDEX IF NOT EXISTS idx_users_platform ON users(user_id, platform)'
        }
        
        self.create_indexes(indexes)
        
        # Analyze tables for query optimizer
        with self.engine.connect() as conn:
            conn.execute(text("ANALYZE"))
            conn.commit()
            logger.info("Database analyzed for optimization")
    
    def close(self):
        """Close the database connection"""
        if hasattr(self, 'engine'):
            self.engine.dispose()
            logger.info("SQLite executor closed")