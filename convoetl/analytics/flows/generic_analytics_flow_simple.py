"""
Simplified Prefect flow for running generic SQL analytics
Focus: Compute analytics and save to message_analytics table
"""

from prefect import flow, task, get_run_logger
from typing import Dict, Any, Optional, List
import pandas as pd
from datetime import datetime
import time
from pathlib import Path

from ..sqlite.executor import SQLiteAnalyticsExecutor
from ..sqlite.queries import MESSAGE_QUERIES, USER_QUERIES, CHAT_QUERIES
from ..sqlite.message_analytics_saver import MessageAnalyticsSaver


@task(
    name="compute_message_analytics",
    description="Compute analytics for individual messages",
    retries=2
)
async def compute_message_analytics(
    chat_id: str,
    db_path: str,
    limit: int = 1000
) -> List[Dict[str, Any]]:
    """
    Compute generic analytics for messages
    
    Args:
        chat_id: Chat to analyze
        db_path: Path to database
        limit: Max messages to analyze
        
    Returns:
        List of analytics dictionaries (one per message)
    """
    logger = get_run_logger()
    logger.info(f"Computing message analytics for chat {chat_id}")
    
    saver = MessageAnalyticsSaver(db_path)
    
    # Get messages that need analysis
    messages_df = saver.get_messages_for_analysis(chat_id, 'generic', limit)
    
    if messages_df.empty:
        logger.info("No messages need generic analysis")
        return []
    
    logger.info(f"Analyzing {len(messages_df)} messages")
    
    analytics_data = []
    
    for _, msg in messages_df.iterrows():
        text = msg['message_text']
        
        # Basic text metrics
        char_count = len(text)
        words = text.split()
        word_count = len(words)
        unique_words = set(words)
        unique_word_count = len(unique_words)
        
        analytics = {
            'message_id': msg['message_id'],
            'chat_id': msg['chat_id'],
            'user_id': msg['user_id'],
            'platform': 'telegram',
            
            # Text metrics
            'char_count': char_count,
            'word_count': word_count,
            'sentence_count': text.count('.') + text.count('!') + text.count('?'),
            'avg_word_length': sum(len(w) for w in words) / word_count if word_count > 0 else 0,
            'unique_word_count': unique_word_count,
            'lexical_diversity': unique_word_count / word_count if word_count > 0 else 0,
            
            # Content flags
            'contains_question': '?' in text,
            'caps_ratio': sum(1 for c in text if c.isupper()) / char_count if char_count > 0 else 0,
            'emoji_count': sum(1 for c in text if ord(c) > 127462),  # Simple emoji detection
            'contains_link': 'http://' in text or 'https://' in text,
            'link_count': text.count('http://') + text.count('https://'),
            'contains_mention': '@' in text,
            'mention_count': text.count('@'),
            'contains_hashtag': '#' in text,
            'hashtag_count': text.count('#'),
            
            # Context
            'is_reply': msg.get('reply_to_id') is not None,
            'hour_of_day': pd.to_datetime(msg['timestamp']).hour if pd.notna(msg.get('timestamp')) else None,
            'day_of_week': pd.to_datetime(msg['timestamp']).dayofweek if pd.notna(msg.get('timestamp')) else None,
        }
        
        analytics_data.append(analytics)
    
    logger.info(f"Computed analytics for {len(analytics_data)} messages")
    return analytics_data


@task(
    name="save_message_analytics",
    description="Save computed analytics to database"
)
async def save_message_analytics(
    analytics_data: List[Dict[str, Any]],
    db_path: str
) -> int:
    """
    Save analytics to message_analytics table
    
    Args:
        analytics_data: List of analytics dictionaries
        db_path: Path to database
        
    Returns:
        Number of records saved
    """
    logger = get_run_logger()
    
    if not analytics_data:
        logger.info("No analytics data to save")
        return 0
    
    saver = MessageAnalyticsSaver(db_path)
    saved_count = saver.save_generic_analytics(analytics_data)
    
    logger.info(f"Saved {saved_count} analytics records")
    return saved_count


@task(
    name="run_aggregate_queries",
    description="Run SQL queries for aggregate analytics"
)
async def run_aggregate_queries(
    executor: SQLiteAnalyticsExecutor,
    chat_id: str,
    query_type: str = "messages"
) -> Dict[str, pd.DataFrame]:
    """
    Run aggregate SQL queries (for dashboards/reports)
    
    Args:
        executor: Database executor
        chat_id: Chat to analyze
        query_type: Type of queries to run
        
    Returns:
        Dictionary of query results
    """
    logger = get_run_logger()
    logger.info(f"Running {query_type} aggregate queries for chat {chat_id}")
    
    # Select queries based on type
    if query_type == "messages":
        queries = MESSAGE_QUERIES
    elif query_type == "users":
        queries = USER_QUERIES
    elif query_type == "chat":
        queries = CHAT_QUERIES
    else:
        raise ValueError(f"Unknown query type: {query_type}")
    
    results = {}
    
    for query_name, query_obj in queries.items():
        logger.info(f"Running: {query_obj.name}")
        
        params = {"chat_id": chat_id}
        if query_obj.parameters:
            params.update(query_obj.parameters)
        
        try:
            results[query_name] = executor.execute_query(
                query_obj.sql,
                params
            )
            logger.info(f"✓ Completed {query_name}: {len(results[query_name])} rows")
        except Exception as e:
            logger.error(f"Failed {query_name}: {e}")
            results[query_name] = pd.DataFrame()
    
    return results


@flow(
    name="message_analytics_pipeline",
    description="Compute and save message-level analytics"
)
async def message_analytics_flow(
    chat_id: str,
    db_path: str = "data/telegram.db",
    limit: int = 1000,
    save_to_db: bool = True,
    run_aggregates: bool = False
) -> Dict[str, Any]:
    """
    Main flow for message analytics
    
    Args:
        chat_id: Chat to analyze
        db_path: Path to database
        limit: Max messages to analyze
        save_to_db: Whether to save to message_analytics table
        run_aggregates: Whether to run aggregate queries
        
    Returns:
        Dictionary with results
    """
    logger = get_run_logger()
    logger.info(f"Starting message analytics pipeline for chat {chat_id}")
    
    start_time = time.time()
    
    # Step 1: Compute analytics for individual messages
    analytics_data = await compute_message_analytics(chat_id, db_path, limit)
    
    # Step 2: Save to database if requested
    saved_count = 0
    if save_to_db and analytics_data:
        saved_count = await save_message_analytics(analytics_data, db_path)
    
    # Step 3: Run aggregate queries if requested
    aggregate_results = {}
    if run_aggregates:
        executor = SQLiteAnalyticsExecutor(db_path)
        try:
            logger.info("Running aggregate queries...")
            
            # Run different query types
            message_results = await run_aggregate_queries(executor, chat_id, "messages")
            user_results = await run_aggregate_queries(executor, chat_id, "users")
            chat_results = await run_aggregate_queries(executor, chat_id, "chat")
            
            aggregate_results = {
                "messages": message_results,
                "users": user_results,
                "chat": chat_results
            }
        finally:
            executor.close()
    
    duration = time.time() - start_time
    
    result = {
        "status": "success",
        "chat_id": chat_id,
        "messages_analyzed": len(analytics_data),
        "messages_saved": saved_count,
        "aggregate_results": aggregate_results,
        "duration_seconds": duration,
        "timestamp": datetime.now().isoformat()
    }
    
    logger.info(f"Pipeline completed in {duration:.2f} seconds")
    logger.info(f"Analyzed: {len(analytics_data)} messages, Saved: {saved_count}")
    
    return result