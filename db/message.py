"""
Telegram Message model
"""

from datetime import datetime
from sqlalchemy import (
    Column, BigInteger, String, Text, DateTime, Integer, 
    Float, JSON, ForeignKey, Index, UniqueConstraint
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .base import Base


class Message(Base):
    """
    Stores individual Telegram messages
    Core table with full message attribution
    """
    __tablename__ = 'messages'
    
    # Composite primary key for uniqueness
    message_id = Column(BigInteger, primary_key=True, comment='Telegram message ID')
    group_id = Column(BigInteger, ForeignKey('groups.group_id'), primary_key=True, comment='Group where message was sent')
    
    # User attribution (critical as per philosophy)
    user_id = Column(BigInteger, ForeignKey('users.user_id'), nullable=False, index=True, comment='Message sender')
    
    # Message content
    message_text = Column(Text, nullable=True, comment='Message text content')
    message_type = Column(String(50), nullable=False, default='text', comment='Type: text, photo, video, document, etc.')
    
    # Message metadata
    date = Column(DateTime, nullable=False, index=True, comment='Message timestamp')
    edit_date = Column(DateTime, nullable=True, comment='Last edit timestamp')
    
    # Reply and forward tracking
    reply_to_message_id = Column(BigInteger, nullable=True, comment='ID of message being replied to')
    forward_from_user_id = Column(BigInteger, nullable=True, comment='Original sender if forwarded')
    forward_from_chat_id = Column(BigInteger, nullable=True, comment='Original chat if forwarded')
    forward_date = Column(DateTime, nullable=True, comment='Original message date if forwarded')
    
    # Media and attachments
    media_id = Column(String(255), nullable=True, comment='Media file ID if attached')
    media_type = Column(String(50), nullable=True, comment='Media type: photo, video, document, etc.')
    media_size = Column(BigInteger, nullable=True, comment='Media file size in bytes')
    
    # Extracted entities (for analysis)
    entities = Column(JSON, nullable=True, comment='Extracted entities: mentions, hashtags, URLs, etc.')
    mentioned_users = Column(JSON, nullable=True, comment='List of mentioned user IDs')
    hashtags = Column(JSON, nullable=True, comment='List of hashtags in message')
    urls = Column(JSON, nullable=True, comment='List of URLs in message')
    
    # Text statistics
    word_count = Column(Integer, nullable=True, comment='Number of words')
    character_count = Column(Integer, nullable=True, comment='Number of characters')
    
    # Sentiment and analysis (to be filled by ML pipeline)
    sentiment_score = Column(Float, nullable=True, comment='Sentiment score (-1 to 1)')
    sentiment_label = Column(String(20), nullable=True, comment='Sentiment: positive, negative, neutral')
    toxicity_score = Column(Float, nullable=True, comment='Toxicity score (0 to 1)')
    
    # ETL metadata
    extracted_at = Column(DateTime, nullable=False, default=func.now(), comment='When message was extracted')
    processed_at = Column(DateTime, nullable=True, comment='When message was processed/transformed')
    
    # Relationships
    user = relationship("TelegramUser", back_populates="messages")
    group = relationship("TelegramGroup", back_populates="messages")
    
    # Indexes for query performance
    __table_args__ = (
        # Composite unique constraint
        UniqueConstraint('message_id', 'group_id', name='uq_message_group'),
        
        # Performance indexes
        Index('idx_message_user', 'user_id', 'date'),
        Index('idx_message_date', 'date'),
        Index('idx_message_group_date', 'group_id', 'date'),
        Index('idx_message_reply', 'reply_to_message_id'),
        Index('idx_message_type', 'message_type'),
        Index('idx_message_sentiment', 'sentiment_label', 'sentiment_score'),
    )

    def __repr__(self):
        return f"<Message(id={self.message_id}, group={self.group_id}, user={self.user_id})>"