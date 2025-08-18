"""
Telegram Group model
"""

from datetime import datetime
from sqlalchemy import (
    Column, BigInteger, String, Text, DateTime, Boolean, 
    Integer, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .base import Base


class TelegramGroup(Base):
    """
    Stores information about Telegram groups/channels
    """
    __tablename__ = 'groups'
    
    # Primary key
    group_id = Column(BigInteger, primary_key=True, comment='Telegram group/channel ID')
    
    # Group metadata
    title = Column(String(255), nullable=False, comment='Group/channel title')
    username = Column(String(100), nullable=True, index=True, comment='Group username (e.g., @groupname)')
    description = Column(Text, nullable=True, comment='Group description')
    
    # Group type flags
    is_channel = Column(Boolean, default=False, nullable=False, comment='True if this is a channel')
    is_megagroup = Column(Boolean, default=False, nullable=False, comment='True if this is a megagroup')
    is_private = Column(Boolean, default=False, nullable=False, comment='True if group is private')
    is_verified = Column(Boolean, default=False, nullable=False, comment='True if group is verified')
    
    # Statistics
    participants_count = Column(Integer, nullable=True, comment='Number of participants')
    
    # Tracking fields
    first_seen_at = Column(DateTime, nullable=False, default=func.now(), comment='When we first discovered this group')
    last_updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now(), comment='Last metadata update')
    last_message_at = Column(DateTime, nullable=True, comment='Timestamp of the last message in group')
    
    # ETL metadata
    is_active = Column(Boolean, default=True, nullable=False, comment='Whether we are actively monitoring this group')
    extraction_enabled = Column(Boolean, default=True, nullable=False, comment='Whether extraction is enabled')
    
    # Relationships
    messages = relationship("Message", back_populates="group", cascade="all, delete-orphan")
    user_memberships = relationship("UserGroupMembership", back_populates="group")
    
    # Indexes for performance
    __table_args__ = (
        Index('idx_group_username', 'username'),
        Index('idx_group_active', 'is_active', 'extraction_enabled'),
        Index('idx_group_last_message', 'last_message_at'),
    )

    def __repr__(self):
        return f"<TelegramGroup(id={self.group_id}, title='{self.title}')>"