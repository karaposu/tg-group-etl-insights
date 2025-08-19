"""
SQLite-specific analytics queries
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional


@dataclass
class AnalyticsQuery:
    """Container for analytics queries"""
    name: str
    description: str
    sql: str
    parameters: Optional[Dict[str, Any]] = None


# Message Analytics Queries
MESSAGE_QUERIES = {
    "individual_messages": AnalyticsQuery(
        name="Individual Message Analysis",
        description="Analyzes properties of each individual message",
        sql="""
            SELECT 
                message_id,
                user_id,
                message_text,
                LENGTH(message_text) as char_count,
                LENGTH(message_text) - LENGTH(REPLACE(message_text, ' ', '')) + 1 as word_count,
                CASE 
                    WHEN message_text LIKE '%?%' THEN 'question'
                    WHEN message_text LIKE '%!%' THEN 'exclamation'
                    WHEN message_text = UPPER(message_text) AND LENGTH(message_text) > 5 THEN 'shouting'
                    ELSE 'statement'
                END as message_type,
                CASE 
                    WHEN reply_to_id IS NOT NULL THEN 'reply'
                    ELSE 'new_thread'
                END as conversation_type,
                timestamp,
                LAG(timestamp) OVER (PARTITION BY user_id ORDER BY timestamp) as prev_message_time,
                (julianday(timestamp) - julianday(LAG(timestamp) OVER (PARTITION BY user_id ORDER BY timestamp))) * 24 * 60 as minutes_since_last_message
            FROM messages
            WHERE chat_id = :chat_id
            ORDER BY timestamp DESC
            LIMIT :limit
        """,
        parameters={"limit": 100}
    ),
    
    "hourly_distribution": AnalyticsQuery(
        name="Hourly Message Distribution",
        description="Messages per hour of day",
        sql="""
            SELECT 
                CAST(strftime('%H', timestamp) AS INTEGER) as hour,
                COUNT(*) as message_count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
            FROM messages
            WHERE chat_id = :chat_id
            GROUP BY hour
            ORDER BY hour
        """
    ),
    
    "weekly_pattern": AnalyticsQuery(
        name="Weekly Message Pattern",
        description="Messages per day of week",
        sql="""
            SELECT 
                CAST(strftime('%w', timestamp) AS INTEGER) as day_of_week,
                CASE CAST(strftime('%w', timestamp) AS INTEGER)
                    WHEN 0 THEN 'Sunday'
                    WHEN 1 THEN 'Monday'
                    WHEN 2 THEN 'Tuesday'
                    WHEN 3 THEN 'Wednesday'
                    WHEN 4 THEN 'Thursday'
                    WHEN 5 THEN 'Friday'
                    WHEN 6 THEN 'Saturday'
                END as day_name,
                COUNT(*) as message_count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
            FROM messages
            WHERE chat_id = :chat_id
            GROUP BY day_of_week, day_name
            ORDER BY day_of_week
        """
    ),
    
    "message_types": AnalyticsQuery(
        name="Message Type Distribution",
        description="Distribution of message types (reply vs new)",
        sql="""
            SELECT 
                CASE 
                    WHEN reply_to_id IS NOT NULL THEN 'reply'
                    ELSE 'new'
                END as message_type,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
            FROM messages
            WHERE chat_id = :chat_id
            GROUP BY message_type
        """
    ),
    
    "question_patterns": AnalyticsQuery(
        name="Question and Exclamation Patterns",
        description="Analysis of questions, exclamations, and caps messages",
        sql="""
            SELECT 
                COUNT(CASE WHEN message_text LIKE '%?%' THEN 1 END) as questions,
                COUNT(CASE WHEN message_text LIKE '%!%' THEN 1 END) as exclamations,
                COUNT(CASE WHEN message_text = UPPER(message_text) AND LENGTH(message_text) > 5 THEN 1 END) as caps_messages,
                COUNT(*) as total_messages,
                ROUND(COUNT(CASE WHEN message_text LIKE '%?%' THEN 1 END) * 100.0 / COUNT(*), 2) as question_rate
            FROM messages
            WHERE chat_id = :chat_id
        """
    ),
    
    "message_velocity": AnalyticsQuery(
        name="Message Velocity and Bursts",
        description="Identify burst periods of high activity",
        sql="""
            WITH hourly_stats AS (
                SELECT 
                    strftime('%Y-%m-%d %H:00:00', timestamp) as hour_bucket,
                    COUNT(*) as message_count
                FROM messages
                WHERE chat_id = :chat_id
                GROUP BY hour_bucket
            ),
            stats AS (
                SELECT 
                    AVG(message_count) as avg_hourly,
                    AVG(message_count) + 2 * (
                        SELECT SQRT(AVG((message_count - sub.avg) * (message_count - sub.avg)))
                        FROM hourly_stats, (SELECT AVG(message_count) as avg FROM hourly_stats) sub
                    ) as burst_threshold
                FROM hourly_stats
            )
            SELECT 
                h.hour_bucket,
                h.message_count,
                ROUND(h.message_count * 1.0 / s.avg_hourly, 2) as intensity_ratio,
                'burst' as activity_level
            FROM hourly_stats h, stats s
            WHERE h.message_count > s.burst_threshold
            ORDER BY h.message_count DESC
            LIMIT 10
        """
    )
}


# User Analytics Queries
USER_QUERIES = {
    "user_activity": AnalyticsQuery(
        name="User Activity Metrics",
        description="Basic user statistics and activity levels",
        sql="""
            SELECT 
                u.user_id,
                u.username,
                u.first_name || ' ' || u.last_name as full_name,
                COUNT(m.message_id) as total_messages,
                AVG(LENGTH(m.message_text)) as avg_message_length,
                MIN(m.timestamp) as first_message,
                MAX(m.timestamp) as last_message,
                COUNT(DISTINCT DATE(m.timestamp)) as active_days,
                ROUND(COUNT(m.message_id) * 100.0 / (SELECT COUNT(*) FROM messages WHERE chat_id = :chat_id), 2) as message_share
            FROM users u
            LEFT JOIN messages m ON u.user_id = m.user_id AND u.platform = m.platform
            WHERE m.chat_id = :chat_id
            GROUP BY u.user_id, u.username, full_name
            ORDER BY total_messages DESC
            LIMIT :limit
        """,
        parameters={"limit": 20}
    ),
    
    "user_engagement": AnalyticsQuery(
        name="User Engagement Levels",
        description="Classify users by engagement level",
        sql="""
            WITH user_stats AS (
                SELECT 
                    user_id,
                    COUNT(*) as message_count,
                    PERCENT_RANK() OVER (ORDER BY COUNT(*)) as percentile
                FROM messages
                WHERE chat_id = :chat_id
                GROUP BY user_id
            )
            SELECT 
                user_id,
                message_count,
                ROUND(percentile * 100, 2) as percentile_rank,
                CASE 
                    WHEN percentile >= 0.9 THEN 'power_user'
                    WHEN percentile >= 0.75 THEN 'active'
                    WHEN percentile >= 0.5 THEN 'regular'
                    WHEN percentile >= 0.25 THEN 'occasional'
                    ELSE 'lurker'
                END as engagement_level
            FROM user_stats
            ORDER BY message_count DESC
        """
    ),
    
    "user_consistency": AnalyticsQuery(
        name="User Consistency Analysis",
        description="Analyze user posting regularity",
        sql="""
            WITH user_daily AS (
                SELECT 
                    user_id,
                    DATE(timestamp) as date,
                    COUNT(*) as daily_messages
                FROM messages
                WHERE chat_id = :chat_id
                GROUP BY user_id, DATE(timestamp)
            ),
            user_consistency AS (
                SELECT 
                    user_id,
                    COUNT(DISTINCT date) as active_days,
                    AVG(daily_messages) as avg_daily_messages,
                    MIN(date) as first_active,
                    MAX(date) as last_active,
                    julianday(MAX(date)) - julianday(MIN(date)) + 1 as total_span_days
                FROM user_daily
                GROUP BY user_id
            )
            SELECT 
                user_id,
                active_days,
                total_span_days,
                ROUND(active_days * 100.0 / total_span_days, 2) as consistency_rate,
                ROUND(avg_daily_messages, 2) as avg_daily_messages,
                CASE 
                    WHEN active_days * 1.0 / total_span_days > 0.5 THEN 'highly_consistent'
                    WHEN active_days * 1.0 / total_span_days > 0.25 THEN 'regular'
                    WHEN active_days * 1.0 / total_span_days > 0.1 THEN 'sporadic'
                    ELSE 'rare'
                END as consistency_level
            FROM user_consistency
            WHERE total_span_days > 7
            ORDER BY consistency_rate DESC
        """
    ),
    
    "user_content_patterns": AnalyticsQuery(
        name="User Content Patterns",
        description="Analyze the type of content users contribute",
        sql="""
            SELECT 
                user_id,
                COUNT(*) as total_messages,
                COUNT(CASE WHEN message_text LIKE '%?%' THEN 1 END) as questions,
                COUNT(CASE WHEN reply_to_id IS NULL THEN 1 END) as conversation_starters,
                COUNT(CASE WHEN reply_to_id IS NOT NULL THEN 1 END) as replies,
                ROUND(COUNT(CASE WHEN message_text LIKE '%?%' THEN 1 END) * 100.0 / COUNT(*), 2) as question_rate,
                ROUND(COUNT(CASE WHEN reply_to_id IS NULL THEN 1 END) * 100.0 / COUNT(*), 2) as starter_rate
            FROM messages
            WHERE chat_id = :chat_id
            GROUP BY user_id
            HAVING total_messages > 10
            ORDER BY total_messages DESC
        """
    )
}


# Chat Analytics Queries
CHAT_QUERIES = {
    "chat_health": AnalyticsQuery(
        name="Overall Chat Health",
        description="High-level metrics about the chat",
        sql="""
            WITH chat_stats AS (
                SELECT 
                    c.chat_id,
                    c.title,
                    c.chat_type,
                    c.participants_count as total_members,
                    COUNT(DISTINCT m.user_id) as active_users,
                    COUNT(m.message_id) as total_messages,
                    MIN(m.timestamp) as first_message,
                    MAX(m.timestamp) as last_message,
                    CAST(julianday(MAX(m.timestamp)) - julianday(MIN(m.timestamp)) AS INTEGER) as chat_duration_days
                FROM chats c
                LEFT JOIN messages m ON c.chat_id = m.chat_id
                WHERE c.chat_id = :chat_id
                GROUP BY c.chat_id, c.title, c.chat_type, c.participants_count
            )
            SELECT 
                chat_id,
                title,
                chat_type,
                total_members,
                active_users,
                total_messages,
                first_message,
                last_message,
                chat_duration_days,
                ROUND(active_users * 100.0 / NULLIF(total_members, 0), 2) as participation_rate,
                ROUND(total_messages * 1.0 / NULLIF(active_users, 0), 2) as avg_messages_per_user,
                ROUND(total_messages * 1.0 / NULLIF(chat_duration_days, 0), 2) as avg_messages_per_day
            FROM chat_stats
        """
    ),
    
    "weekly_trends": AnalyticsQuery(
        name="Weekly Activity Trends",
        description="Weekly activity trends with growth",
        sql="""
            WITH weekly_stats AS (
                SELECT 
                    strftime('%Y-W%W', timestamp) as week,
                    COUNT(*) as messages,
                    COUNT(DISTINCT user_id) as active_users,
                    COUNT(DISTINCT CASE WHEN reply_to_id IS NOT NULL THEN message_id END) as replies
                FROM messages
                WHERE chat_id = :chat_id
                GROUP BY week
            )
            SELECT 
                week,
                messages,
                active_users,
                replies,
                LAG(messages) OVER (ORDER BY week) as prev_week_messages,
                ROUND((messages - LAG(messages) OVER (ORDER BY week)) * 100.0 / 
                      NULLIF(LAG(messages) OVER (ORDER BY week), 0), 2) as growth_rate
            FROM weekly_stats
            ORDER BY week DESC
            LIMIT 10
        """
    ),
    
    "response_dynamics": AnalyticsQuery(
        name="Conversation Response Dynamics",
        description="Analyze response times and patterns",
        sql="""
            WITH message_pairs AS (
                SELECT 
                    m1.message_id,
                    m1.user_id as responder,
                    m1.timestamp as response_time,
                    m2.timestamp as original_time,
                    (julianday(m1.timestamp) - julianday(m2.timestamp)) * 24 * 60 as response_minutes
                FROM messages m1
                JOIN messages m2 ON m1.reply_to_id = m2.message_id
                WHERE m1.chat_id = :chat_id AND m2.chat_id = :chat_id
                    AND (julianday(m1.timestamp) - julianday(m2.timestamp)) * 24 < 24
            )
            SELECT 
                COUNT(*) as total_replies,
                ROUND(AVG(response_minutes), 2) as avg_response_minutes,
                ROUND(MIN(response_minutes), 2) as fastest_response,
                ROUND(MAX(response_minutes), 2) as slowest_response,
                COUNT(CASE WHEN response_minutes < 5 THEN 1 END) as quick_replies_under_5min,
                COUNT(CASE WHEN response_minutes < 60 THEN 1 END) as replies_within_hour
            FROM message_pairs
        """
    )
}