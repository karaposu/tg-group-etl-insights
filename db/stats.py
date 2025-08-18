"""
Daily Group Statistics model
"""

from datetime import datetime
from sqlalchemy import (
    Column, BigInteger, Integer, DateTime, Float, 
    Text, JSON, ForeignKey, Index
)
from sqlalchemy.sql import func

from .base import Base


class DailyGroupStats(Base):
    """
    Pre-aggregated daily statistics for groups
    For performance optimization
    """
    __tablename__ = 'daily_group_stats'
    
    # Composite primary key
    group_id = Column(BigInteger, ForeignKey('groups.group_id'), primary_key=True)
    date = Column(DateTime, primary_key=True, comment='Date (day precision)')
    
    # Message statistics
    total_messages = Column(Integer, nullable=False, default=0)
    unique_users = Column(Integer, nullable=False, default=0)
    
    # Activity by hour (24 hour array)
    hourly_messages = Column(JSON, nullable=True, comment='Array of message counts by hour')
    
    # Content statistics
    total_words = Column(Integer, nullable=True)
    total_characters = Column(Integer, nullable=True)
    avg_message_length = Column(Float, nullable=True)
    
    # Engagement metrics
    replies_count = Column(Integer, nullable=True, comment='Number of replies')
    forwards_count = Column(Integer, nullable=True, comment='Number of forwards')
    media_count = Column(Integer, nullable=True, comment='Number of media messages')
    
    # Top entities
    top_users = Column(JSON, nullable=True, comment='Top 10 active users with message counts')
    top_hashtags = Column(JSON, nullable=True, comment='Top hashtags used')
    top_mentioned = Column(JSON, nullable=True, comment='Top mentioned users')
    
    # Sentiment summary
    positive_messages = Column(Integer, nullable=True)
    negative_messages = Column(Integer, nullable=True)
    neutral_messages = Column(Integer, nullable=True)
    avg_sentiment = Column(Float, nullable=True)
    
    # LLM-generated daily summary
    daily_topics = Column(JSON, nullable=True, comment='Main topics discussed')
    daily_summary = Column(Text, nullable=True, comment='LLM-generated daily summary')
    
    # Processing metadata
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Indexes
    __table_args__ = (
        Index('idx_daily_stats_date', 'date'),
        Index('idx_daily_stats_group_date', 'group_id', 'date'),
    )

    def __repr__(self):
        return f"<DailyGroupStats(group={self.group_id}, date={self.date}, messages={self.total_messages})>"