"""
Database models and utilities for Telegram ETL pipeline
"""

# Base configuration
from .base import Base, metadata, get_engine, get_session, create_all_tables

# Models
from .group import TelegramGroup
from .user import TelegramUser
from .message import Message
from .membership import UserGroupMembership
from .insights import UserInsight
from .etl import ETLRun
from .stats import DailyGroupStats
from .message_analytics import MessageAnalytics
from .chat_daily import ChatDaily
from .user_analytics import UserAnalytics

# Model registry for easy access
MODELS = {
    'groups': TelegramGroup,
    'users': TelegramUser,
    'messages': Message,
    'user_group_memberships': UserGroupMembership,
    'user_insights': UserInsight,
    'etl_runs': ETLRun,
    'daily_group_stats': DailyGroupStats,
    'message_analytics': MessageAnalytics,
    'chat_daily': ChatDaily,
    'user_analytics': UserAnalytics
}

__all__ = [
    # Base
    'Base',
    'metadata',
    'get_engine',
    'get_session',
    'create_all_tables',
    
    # Models
    'TelegramGroup',
    'TelegramUser',
    'Message',
    'UserGroupMembership',
    'UserInsight',
    'ETLRun',
    'DailyGroupStats',
    'MessageAnalytics',
    'ChatDaily',
    'UserAnalytics',
    
    # Registry
    'MODELS'
]