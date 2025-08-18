"""
User-Group Membership model
"""

from datetime import datetime
from sqlalchemy import (
    Column, BigInteger, String, DateTime, Boolean, 
    Integer, Float, ForeignKey, Index
)
from sqlalchemy.orm import relationship

from .base import Base


class UserGroupMembership(Base):
    """
    Tracks user membership in groups
    Many-to-many relationship with additional metadata
    """
    __tablename__ = 'user_group_memberships'
    
    # Composite primary key
    user_id = Column(BigInteger, ForeignKey('users.user_id'), primary_key=True)
    group_id = Column(BigInteger, ForeignKey('groups.group_id'), primary_key=True)
    
    # Membership metadata
    joined_at = Column(DateTime, nullable=True, comment='When user joined the group')
    first_message_at = Column(DateTime, nullable=True, comment='When user sent first message')
    last_message_at = Column(DateTime, nullable=True, comment='When user sent last message')
    
    # User role in group
    role = Column(String(50), nullable=True, comment='User role: member, admin, creator')
    is_admin = Column(Boolean, default=False, nullable=False)
    is_creator = Column(Boolean, default=False, nullable=False)
    
    # Activity statistics
    message_count = Column(Integer, default=0, nullable=False, comment='Messages sent in this group')
    avg_daily_messages = Column(Float, nullable=True, comment='Average messages per day')
    activity_score = Column(Float, nullable=True, comment='User activity score in group')
    
    # Status
    is_active = Column(Boolean, default=True, nullable=False, comment='Whether user is still in group')
    left_at = Column(DateTime, nullable=True, comment='When user left the group')
    
    # Relationships
    user = relationship("TelegramUser", back_populates="group_memberships")
    group = relationship("TelegramGroup", back_populates="user_memberships")
    
    # Indexes
    __table_args__ = (
        Index('idx_membership_active', 'is_active'),
        Index('idx_membership_activity', 'group_id', 'message_count'),
    )

    def __repr__(self):
        return f"<UserGroupMembership(user={self.user_id}, group={self.group_id}, role='{self.role}')>"