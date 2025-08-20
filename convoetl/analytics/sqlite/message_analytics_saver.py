"""
Simple saver for message analytics to the message_analytics table
"""

import sqlite3
import pandas as pd
from typing import Dict, List, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class MessageAnalyticsSaver:
    """Saves computed analytics to message_analytics table"""
    
    def __init__(self, db_path: str):
        """
        Initialize saver with database path
        
        Args:
            db_path: Path to SQLite database
        """
        self.db_path = db_path
    
    def save_generic_analytics(
        self,
        analytics_data: List[Dict[str, Any]]
    ) -> int:
        """
        Save generic (non-LLM) analytics for messages
        
        Args:
            analytics_data: List of dictionaries with analytics for each message
            
        Returns:
            Number of records saved
        """
        if not analytics_data:
            logger.warning("No analytics data to save")
            return 0
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Prepare insert statement
            insert_sql = """
                INSERT OR REPLACE INTO message_analytics (
                    message_id, chat_id, user_id, platform,
                    analyzed_at, analysis_version,
                    -- Text metrics
                    char_count, word_count, sentence_count,
                    avg_word_length, unique_word_count, lexical_diversity,
                    -- Content flags
                    contains_question, caps_ratio, emoji_count,
                    contains_link, link_count,
                    contains_mention, mention_count,
                    contains_hashtag, hashtag_count,
                    contains_media, media_type,
                    -- Language
                    language_code, language_confidence, complexity_level,
                    -- Context
                    is_reply, time_since_prev_message,
                    hour_of_day, day_of_week, is_conversation_starter
                ) VALUES (
                    :message_id, :chat_id, :user_id, :platform,
                    :analyzed_at, :analysis_version,
                    :char_count, :word_count, :sentence_count,
                    :avg_word_length, :unique_word_count, :lexical_diversity,
                    :contains_question, :caps_ratio, :emoji_count,
                    :contains_link, :link_count,
                    :contains_mention, :mention_count,
                    :contains_hashtag, :hashtag_count,
                    :contains_media, :media_type,
                    :language_code, :language_confidence, :complexity_level,
                    :is_reply, :time_since_prev_message,
                    :hour_of_day, :day_of_week, :is_conversation_starter
                )
            """
            
            # Add default values
            for record in analytics_data:
                record.setdefault('analyzed_at', datetime.now().isoformat())
                record.setdefault('analysis_version', '1.0')
                record.setdefault('platform', 'telegram')
            
            # Execute batch insert
            cursor.executemany(insert_sql, analytics_data)
            conn.commit()
            
            saved_count = cursor.rowcount
            logger.info(f"Saved {saved_count} message analytics records")
            return saved_count
            
        except Exception as e:
            logger.error(f"Failed to save analytics: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def save_llm_analytics(
        self,
        analytics_data: List[Dict[str, Any]]
    ) -> int:
        """
        Update messages with LLM-based analytics
        
        Args:
            analytics_data: List of dictionaries with LLM analytics
            
        Returns:
            Number of records updated
        """
        if not analytics_data:
            return 0
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Update statement for LLM fields
            update_sql = """
                UPDATE message_analytics
                SET 
                    -- Sentiment & Emotion
                    sentiment = :sentiment,
                    emotion = :emotion,
                    emotion_confidence = :emotion_confidence,
                    tone = :tone,
                    formality_level = :formality_level,
                    -- Classification
                    topic = :topic,
                    topic_confidence = :topic_confidence,
                    intent = :intent,
                    message_type = :message_type,
                    summary = :summary,
                    -- Quality & Safety
                    toxicity_score = :toxicity_score,
                    spam_score = :spam_score,
                    is_promotional = :is_promotional,
                    contains_financial_advice = :contains_financial_advice,
                    contains_legal_advice = :contains_legal_advice,
                    misinformation_risk = :misinformation_risk,
                    -- Engagement
                    information_density = :information_density,
                    conversation_value = :conversation_value,
                    requires_response = :requires_response,
                    response_urgency = :response_urgency,
                    conversation_role = :conversation_role,
                    -- Entities
                    entities_json = :entities_json,
                    key_phrases_json = :key_phrases_json,
                    mentioned_topics_json = :mentioned_topics_json,
                    action_items_json = :action_items_json,
                    -- LLM metadata
                    llm_model = :llm_model,
                    llm_processing_time = :llm_processing_time,
                    llm_total_tokens = :llm_total_tokens,
                    llm_total_cost = :llm_total_cost,
                    llm_usage_json = :llm_usage_json,
                    requires_reanalysis = 0,
                    analyzed_at = :analyzed_at
                WHERE message_id = :message_id
            """
            
            # Add timestamp
            for record in analytics_data:
                record['analyzed_at'] = datetime.now().isoformat()
            
            cursor.executemany(update_sql, analytics_data)
            conn.commit()
            
            updated_count = cursor.rowcount
            logger.info(f"Updated {updated_count} messages with LLM analytics")
            return updated_count
            
        except Exception as e:
            logger.error(f"Failed to save LLM analytics: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def get_messages_for_analysis(
        self,
        chat_id: str,
        analysis_type: str = 'generic',
        limit: int = 1000
    ) -> pd.DataFrame:
        """
        Get messages that need analysis
        
        Args:
            chat_id: Chat ID to analyze
            analysis_type: 'generic' or 'llm'
            limit: Maximum number of messages to return
            
        Returns:
            DataFrame with messages needing analysis
        """
        conn = sqlite3.connect(self.db_path)
        
        if analysis_type == 'generic':
            # Get messages not yet in message_analytics
            query = """
                SELECT m.message_id, m.chat_id, m.user_id, 
                       m.message_text, m.timestamp, m.reply_to_id
                FROM messages m
                LEFT JOIN message_analytics ma ON m.message_id = ma.message_id
                WHERE m.chat_id = ? 
                  AND ma.message_id IS NULL
                  AND m.message_text IS NOT NULL
                ORDER BY m.timestamp DESC
                LIMIT ?
            """
        else:  # llm
            # Get messages without LLM analysis
            query = """
                SELECT ma.message_id, m.message_text, ma.chat_id, ma.user_id
                FROM message_analytics ma
                JOIN messages m ON ma.message_id = m.message_id
                WHERE ma.chat_id = ?
                  AND (ma.sentiment IS NULL OR ma.requires_reanalysis = 1)
                  AND m.message_text IS NOT NULL
                ORDER BY m.timestamp DESC
                LIMIT ?
            """
        
        df = pd.read_sql_query(query, conn, params=(chat_id, limit))
        conn.close()
        
        logger.info(f"Found {len(df)} messages for {analysis_type} analysis")
        return df