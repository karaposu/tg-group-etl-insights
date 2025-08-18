"""
Telegram User model
"""

from datetime import datetime
from sqlalchemy import (
    Column, BigInteger, String, Text, DateTime, Boolean, 
    Integer, Float, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .base import Base


class TelegramUser(Base):
    """
    Stores information about Telegram users
    User-centric model as per philosophy
    """
    __tablename__ = 'users'
    
    # Primary key
    user_id = Column(BigInteger, primary_key=True, comment='Telegram user ID')
    
    # User identification
    username = Column(String(100), nullable=True, index=True, comment='User @username')
    first_name = Column(String(255), nullable=True, comment='User first name')
    last_name = Column(String(255), nullable=True, comment='User last name')
    phone = Column(String(50), nullable=True, comment='User phone number (if available)')
    
    # User metadata
    is_bot = Column(Boolean, default=False, nullable=False, comment='True if this is a bot')
    is_verified = Column(Boolean, default=False, nullable=False, comment='True if user is verified')
    is_premium = Column(Boolean, default=False, nullable=False, comment='True if user has Telegram Premium')
    is_fake = Column(Boolean, default=False, nullable=False, comment='True if marked as fake account')
    is_scam = Column(Boolean, default=False, nullable=False, comment='True if marked as scam')
    
    # Profile
    bio = Column(Text, nullable=True, comment='User bio/about text')
    profile_photo_id = Column(String(255), nullable=True, comment='Profile photo file ID')
    
    # Activity tracking
    first_seen_at = Column(DateTime, nullable=False, default=func.now(), comment='When we first saw this user')
    last_seen_at = Column(DateTime, nullable=False, default=func.now(), comment='Last activity timestamp')
    last_updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
    
    # Statistics (denormalized for performance)
    total_messages = Column(Integer, default=0, nullable=False, comment='Total messages sent')
    total_words = Column(Integer, default=0, nullable=False, comment='Total words in messages')
    total_characters = Column(Integer, default=0, nullable=False, comment='Total characters in messages')
    avg_message_length = Column(Float, nullable=True, comment='Average message length')
    
    # Relationships
    messages = relationship("Message", back_populates="user", cascade="all, delete-orphan")
    group_memberships = relationship("UserGroupMembership", back_populates="user")
    user_insights = relationship("UserInsight", back_populates="user", cascade="all, delete-orphan")
    
    # Indexes
    __table_args__ = (
        Index('idx_user_username', 'username'),
        Index('idx_user_activity', 'last_seen_at', 'total_messages'),
        Index('idx_user_bot', 'is_bot'),
    )

    def __repr__(self):
        return f"<TelegramUser(id={self.user_id}, username='{self.username}')>"