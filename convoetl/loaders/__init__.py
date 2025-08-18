"""
Data loaders for different storage backends
"""

from .base import BaseLoader
from .sqlite import SQLiteLoader

__all__ = [
    'BaseLoader',
    'SQLiteLoader',
]