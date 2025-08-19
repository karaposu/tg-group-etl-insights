"""
ConvoETL Analytics Module

Provides SQL-based analytics for extracted conversational data.
Supports multiple database backends (SQLite, BigQuery) with
database-specific query implementations.
"""

from .flows.generic_analytics_flow import (
    generic_analytics_flow,
    run_message_analytics,
    run_user_analytics,
    run_chat_analytics
)

__all__ = [
    'generic_analytics_flow',
    'run_message_analytics',
    'run_user_analytics',
    'run_chat_analytics'
]