"""
ConvoETL Analytics Module

Provides SQL-based analytics for extracted conversational data.
Supports multiple database backends (SQLite, BigQuery) with
database-specific query implementations.
"""

from .flows.generic_analytics_flow_simple import (
    message_analytics_flow,
    compute_message_analytics,
    save_message_analytics,
    run_aggregate_queries
)

__all__ = [
    'message_analytics_flow',
    'compute_message_analytics', 
    'save_message_analytics',
    'run_aggregate_queries'
]