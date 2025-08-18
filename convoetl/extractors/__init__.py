"""
Extractors for different platforms
"""

from .base import BaseExtractor
from .telegram import TelegramExtractor

__all__ = [
    'BaseExtractor',
    'TelegramExtractor',
]