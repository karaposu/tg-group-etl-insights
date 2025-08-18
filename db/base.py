"""
Base configuration for SQLAlchemy models
"""

from sqlalchemy import MetaData, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Create base class for all models
Base = declarative_base()

# Metadata instance for additional operations
metadata = MetaData()


def get_engine(connection_string: str):
    """
    Create and return a database engine
    
    Args:
        connection_string: Database connection string
        
    Returns:
        SQLAlchemy engine
    """
    return create_engine(connection_string)


def get_session(connection_string: str):
    """
    Create and return a database session
    
    Args:
        connection_string: Database connection string
        
    Returns:
        SQLAlchemy session
    """
    engine = get_engine(connection_string)
    Session = sessionmaker(bind=engine)
    return Session()


def create_all_tables(engine):
    """Create all tables in the database"""
    Base.metadata.create_all(engine)