"""
Save analytics results to database tables
"""

import sqlite3
import json
import uuid
from datetime import datetime
from typing import Dict, Any, Optional
import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class AnalyticsResultsSaver:
    """Saves analytics results to database tables"""
    
    def __init__(self, db_path: str):
        """
        Initialize results saver
        
        Args:
            db_path: Path to SQLite database
        """
        self.db_path = Path(db_path)
        self.conn = None
        self._init_tables()
    
    def _init_tables(self):
        """Create analytics result tables if they don't exist"""
        sql_file = Path(__file__).parent / "analytics_tables.sql"
        
        if sql_file.exists():
            with open(sql_file, 'r') as f:
                create_sql = f.read()
            
            conn = sqlite3.connect(self.db_path)
            try:
                # Execute all CREATE statements
                conn.executescript(create_sql)
                conn.commit()
                logger.info("Analytics result tables initialized")
            finally:
                conn.close()
    
    def start_run(
        self,
        chat_id: str,
        run_type: str = "full"
    ) -> str:
        """
        Start a new analytics run
        
        Args:
            chat_id: Chat being analyzed
            run_type: Type of run (full, incremental, scheduled)
            
        Returns:
            run_id for this analytics run
        """
        run_id = str(uuid.uuid4())
        
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("""
                INSERT INTO analytics_runs (
                    run_id, chat_id, run_type, status, run_timestamp
                ) VALUES (?, ?, ?, 'running', ?)
            """, (run_id, chat_id, run_type, datetime.now()))
            conn.commit()
        finally:
            conn.close()
        
        logger.info(f"Started analytics run {run_id} for chat {chat_id}")
        return run_id
    
    def complete_run(
        self,
        run_id: str,
        total_messages: int,
        total_users: int,
        duration_seconds: float,
        metadata: Optional[Dict] = None
    ):
        """Mark run as completed with statistics"""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("""
                UPDATE analytics_runs
                SET status = 'completed',
                    total_messages_analyzed = ?,
                    total_users_analyzed = ?,
                    duration_seconds = ?,
                    metadata = ?
                WHERE run_id = ?
            """, (
                total_messages,
                total_users,
                duration_seconds,
                json.dumps(metadata) if metadata else None,
                run_id
            ))
            conn.commit()
        finally:
            conn.close()
        
        logger.info(f"Completed analytics run {run_id}")
    
    def fail_run(self, run_id: str, error_message: str):
        """Mark run as failed"""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("""
                UPDATE analytics_runs
                SET status = 'failed',
                    error_message = ?
                WHERE run_id = ?
            """, (error_message, run_id))
            conn.commit()
        finally:
            conn.close()
        
        logger.error(f"Analytics run {run_id} failed: {error_message}")
    
    def save_message_analytics(
        self,
        run_id: str,
        chat_id: str,
        results: Dict[str, pd.DataFrame]
    ):
        """
        Save message analytics results
        
        Args:
            run_id: Current run ID
            chat_id: Chat ID
            results: Dictionary of query_name -> DataFrame
        """
        conn = sqlite3.connect(self.db_path)
        try:
            for metric_name, df in results.items():
                if df.empty:
                    continue
                
                # Aggregate metrics from the DataFrame
                if metric_name == "hourly_distribution":
                    # Find peak hour
                    if len(df) > 0:
                        peak_row = df.loc[df['message_count'].idxmax()]
                        conn.execute("""
                            INSERT INTO message_analytics_results (
                                run_id, chat_id, metric_name, metric_value, metric_details
                            ) VALUES (?, ?, ?, ?, ?)
                        """, (
                            run_id,
                            chat_id,
                            "peak_hour",
                            float(peak_row['hour']),
                            json.dumps({
                                "hour": int(peak_row['hour']),
                                "message_count": int(peak_row['message_count']),
                                "percentage": float(peak_row['percentage'])
                            })
                        ))
                
                elif metric_name == "question_patterns":
                    # Save question statistics
                    if len(df) > 0:
                        row = df.iloc[0]
                        conn.execute("""
                            INSERT INTO message_analytics_results (
                                run_id, chat_id, metric_name, metric_value, metric_details
                            ) VALUES (?, ?, ?, ?, ?)
                        """, (
                            run_id,
                            chat_id,
                            "question_rate",
                            float(row.get('question_rate', 0)),
                            json.dumps({
                                "questions": int(row.get('questions', 0)),
                                "exclamations": int(row.get('exclamations', 0)),
                                "caps_messages": int(row.get('caps_messages', 0)),
                                "total_messages": int(row.get('total_messages', 0))
                            })
                        ))
                
                # Save the full distribution as well
                conn.execute("""
                    INSERT INTO message_analytics_results (
                        run_id, chat_id, metric_name, metric_value, metric_details
                    ) VALUES (?, ?, ?, ?, ?)
                """, (
                    run_id,
                    chat_id,
                    f"{metric_name}_count",
                    float(len(df)),
                    df.to_json(orient='records')
                ))
            
            conn.commit()
        finally:
            conn.close()
        
        logger.info(f"Saved message analytics for run {run_id}")
    
    def save_user_analytics(
        self,
        run_id: str,
        chat_id: str,
        results: Dict[str, pd.DataFrame]
    ):
        """Save user analytics results"""
        conn = sqlite3.connect(self.db_path)
        try:
            for metric_name, df in results.items():
                if df.empty:
                    continue
                
                if metric_name == "user_activity":
                    # Save per-user metrics
                    for _, row in df.iterrows():
                        conn.execute("""
                            INSERT INTO user_analytics_results (
                                run_id, chat_id, user_id, metric_name, 
                                metric_value, metric_details
                            ) VALUES (?, ?, ?, ?, ?, ?)
                        """, (
                            run_id,
                            chat_id,
                            str(row['user_id']),
                            "total_messages",
                            float(row['total_messages']),
                            json.dumps({
                                "username": row.get('username'),
                                "full_name": row.get('full_name'),
                                "avg_message_length": float(row.get('avg_message_length', 0)),
                                "active_days": int(row.get('active_days', 0)),
                                "message_share": float(row.get('message_share', 0))
                            })
                        ))
                
                elif metric_name == "user_engagement":
                    # Save engagement levels
                    for _, row in df.iterrows():
                        conn.execute("""
                            INSERT INTO user_analytics_results (
                                run_id, chat_id, user_id, metric_name,
                                metric_value, metric_details
                            ) VALUES (?, ?, ?, ?, ?, ?)
                        """, (
                            run_id,
                            chat_id,
                            str(row['user_id']),
                            "engagement_level",
                            float(row['percentile_rank']),
                            json.dumps({
                                "message_count": int(row['message_count']),
                                "engagement_level": row['engagement_level']
                            })
                        ))
            
            conn.commit()
        finally:
            conn.close()
        
        logger.info(f"Saved user analytics for run {run_id}")
    
    def save_chat_analytics(
        self,
        run_id: str,
        chat_id: str,
        results: Dict[str, pd.DataFrame]
    ):
        """Save chat analytics results"""
        conn = sqlite3.connect(self.db_path)
        try:
            for metric_name, df in results.items():
                if df.empty:
                    continue
                
                if metric_name == "chat_health":
                    # Save overall chat metrics
                    if len(df) > 0:
                        row = df.iloc[0]
                        
                        # Save individual metrics
                        metrics = {
                            "total_messages": row.get('total_messages', 0),
                            "active_users": row.get('active_users', 0),
                            "participation_rate": row.get('participation_rate', 0),
                            "chat_duration_days": row.get('chat_duration_days', 0),
                            "avg_messages_per_user": row.get('avg_messages_per_user', 0),
                            "avg_messages_per_day": row.get('avg_messages_per_day', 0)
                        }
                        
                        for key, value in metrics.items():
                            if value is not None:
                                conn.execute("""
                                    INSERT INTO chat_analytics_results (
                                        run_id, chat_id, metric_name, metric_value
                                    ) VALUES (?, ?, ?, ?)
                                """, (run_id, chat_id, key, float(value)))
                
                elif metric_name == "response_dynamics":
                    # Save response time metrics
                    if len(df) > 0:
                        row = df.iloc[0]
                        avg_response = row.get('avg_response_minutes')
                        if avg_response is not None and pd.notna(avg_response):
                            conn.execute("""
                                INSERT INTO chat_analytics_results (
                                    run_id, chat_id, metric_name, metric_value, metric_details
                                ) VALUES (?, ?, ?, ?, ?)
                            """, (
                                run_id,
                                chat_id,
                                "avg_response_minutes",
                                float(avg_response),
                                json.dumps({
                                    "total_replies": int(row.get('total_replies', 0)),
                                    "fastest_response": float(row.get('fastest_response', 0)) if pd.notna(row.get('fastest_response')) else None,
                                    "slowest_response": float(row.get('slowest_response', 0)) if pd.notna(row.get('slowest_response')) else None,
                                    "quick_replies_under_5min": int(row.get('quick_replies_under_5min', 0)),
                                    "replies_within_hour": int(row.get('replies_within_hour', 0))
                                })
                            ))
            
            conn.commit()
        finally:
            conn.close()
        
        logger.info(f"Saved chat analytics for run {run_id}")
    
    def save_daily_stats(
        self,
        chat_id: str,
        date: str,
        stats: Dict[str, Any]
    ):
        """
        Save or update daily statistics
        
        Args:
            chat_id: Chat ID
            date: Date in YYYY-MM-DD format
            stats: Dictionary of statistics
        """
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("""
                INSERT OR REPLACE INTO daily_chat_stats (
                    chat_id, date, total_messages, active_users,
                    new_users, avg_message_length, peak_hour,
                    peak_hour_messages, questions_count, replies_count,
                    computed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                chat_id,
                date,
                stats.get('total_messages', 0),
                stats.get('active_users', 0),
                stats.get('new_users', 0),
                stats.get('avg_message_length', 0),
                stats.get('peak_hour'),
                stats.get('peak_hour_messages', 0),
                stats.get('questions_count', 0),
                stats.get('replies_count', 0),
                datetime.now()
            ))
            conn.commit()
        finally:
            conn.close()
        
        logger.info(f"Saved daily stats for {chat_id} on {date}")
    
    def get_latest_run(self, chat_id: str) -> Optional[Dict]:
        """Get the latest analytics run for a chat"""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.execute("""
                SELECT run_id, run_timestamp, status, total_messages_analyzed
                FROM analytics_runs
                WHERE chat_id = ? AND status = 'completed'
                ORDER BY run_timestamp DESC
                LIMIT 1
            """, (chat_id,))
            
            row = cursor.fetchone()
            if row:
                return {
                    'run_id': row[0],
                    'run_timestamp': row[1],
                    'status': row[2],
                    'total_messages_analyzed': row[3]
                }
            return None
        finally:
            conn.close()
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()