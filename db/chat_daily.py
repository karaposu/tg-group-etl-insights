"""
SQLAlchemy model for chat_daily table
"""

from sqlalchemy import (
    Column, String, Integer, Date, Text, Float, DateTime,
    PrimaryKeyConstraint, Index
)
from sqlalchemy.sql import func
from .base import Base


class ChatDaily(Base):
    """
    Daily snapshot of chat statistics and LLM-generated summaries.
    One row per chat per day.
    """
    __tablename__ = 'chat_daily'
    
    # Primary Keys
    chat_id = Column(String, nullable=False)
    date = Column(Date, nullable=False)
    
    # Daily Metrics (snapshots)
    total_member_count = Column(Integer)  # Total members in chat on this day
    total_message_count = Column(Integer, nullable=False, default=0)  # Messages sent this day
    total_active_member_count = Column(Integer, nullable=False, default=0)  # Members who sent messages
    
    # LLM Generated Content
    daily_summary = Column(Text)  # LLM-generated summary of the day's conversations
    
    # LLM Metadata
    llm_model = Column(String)  # e.g., 'gpt-4', 'claude-3'
    llm_cost = Column(Float)  # Cost in USD for generating this summary
    
    # Metadata
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    
    # Composite Primary Key
    __table_args__ = (
        PrimaryKeyConstraint('chat_id', 'date'),
        Index('idx_chat_daily_date', 'date'),
        Index('idx_chat_daily_chat_date', 'chat_id', 'date'),
    )
    
    def __repr__(self):
        return f"<ChatDaily(chat_id={self.chat_id}, date={self.date}, messages={self.total_message_count})>"
    
    def to_dict(self):
        """Convert to dictionary for serialization"""
        return {
            'chat_id': self.chat_id,
            'date': self.date.isoformat() if self.date else None,
            'total_member_count': self.total_member_count,
            'total_message_count': self.total_message_count,
            'total_active_member_count': self.total_active_member_count,
            'daily_summary': self.daily_summary,
            'llm_model': self.llm_model,
            'llm_cost': self.llm_cost,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }
    
    @property
    def participation_rate(self):
        """Calculate participation rate for the day"""
        if self.total_member_count and self.total_member_count > 0:
            return (self.total_active_member_count / self.total_member_count) * 100
        return 0.0
    
    @property
    def avg_messages_per_active_user(self):
        """Calculate average messages per active user"""
        if self.total_active_member_count and self.total_active_member_count > 0:
            return self.total_message_count / self.total_active_member_count
        return 0.0
    
    @classmethod
    def compute_from_messages(cls, chat_id: str, date, session):
        """
        Compute daily statistics from messages table
        
        Args:
            chat_id: Chat identifier
            date: Date to compute stats for
            session: SQLAlchemy session
            
        Returns:
            Dictionary with computed stats (not including LLM summary)
        """
        from sqlalchemy import text
        
        # Count messages and active users for the day
        result = session.execute(
            text("""
                SELECT 
                    COUNT(*) as total_message_count,
                    COUNT(DISTINCT user_id) as total_active_member_count
                FROM messages
                WHERE chat_id = :chat_id
                    AND DATE(timestamp) = :date
            """),
            {'chat_id': chat_id, 'date': date}
        ).fetchone()
        
        return {
            'chat_id': chat_id,
            'date': date,
            'total_message_count': result.total_message_count or 0,
            'total_active_member_count': result.total_active_member_count or 0
        }