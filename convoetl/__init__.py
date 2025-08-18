"""
ConvoETL - Extract, Transform, Load conversational data from various platforms

A production-ready ETL pipeline for extracting user conversations from platforms like 
Telegram, YouTube, Discord, and more. Provides unified interface for data extraction,
storage, and analysis.
"""

__version__ = "0.1.0"

from .core.pipeline import Pipeline

__all__ = [
    'Pipeline',
]