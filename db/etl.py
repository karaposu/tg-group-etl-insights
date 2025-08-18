"""
ETL Run tracking model
"""

from datetime import datetime
from sqlalchemy import (
    Column, String, BigInteger, Integer, DateTime, Float, 
    Text, Index
)
from sqlalchemy.sql import func

from .base import Base


class ETLRun(Base):
    """
    Tracks ETL pipeline runs for monitoring and debugging
    """
    __tablename__ = 'etl_runs'
    
    # Primary key
    run_id = Column(String(100), primary_key=True, comment='Unique run ID (UUID)')
    
    # Run metadata
    flow_name = Column(String(100), nullable=False, comment='Prefect flow name')
    flow_run_id = Column(String(100), nullable=True, comment='Prefect flow run ID')
    run_type = Column(String(50), nullable=False, comment='Run type: historical, incremental, reprocess')
    
    # Target information
    group_id = Column(BigInteger, nullable=False, index=True, comment='Target group ID')
    
    # Processing range
    start_message_id = Column(BigInteger, nullable=True, comment='First message ID processed')
    end_message_id = Column(BigInteger, nullable=True, comment='Last message ID processed')
    messages_processed = Column(Integer, default=0, comment='Total messages processed')
    
    # Timing
    started_at = Column(DateTime, nullable=False, default=func.now())
    completed_at = Column(DateTime, nullable=True)
    duration_seconds = Column(Integer, nullable=True, comment='Run duration in seconds')
    
    # Status
    status = Column(String(50), nullable=False, default='running', comment='Status: running, completed, failed, cancelled')
    error_message = Column(Text, nullable=True, comment='Error details if failed')
    
    # Performance metrics
    messages_per_second = Column(Float, nullable=True, comment='Processing rate')
    api_calls_made = Column(Integer, nullable=True, comment='Number of Telegram API calls')
    bytes_processed = Column(BigInteger, nullable=True, comment='Total bytes processed')
    
    # Indexes
    __table_args__ = (
        Index('idx_etl_run_status', 'status', 'started_at'),
        Index('idx_etl_run_group', 'group_id', 'started_at'),
    )

    def __repr__(self):
        return f"<ETLRun(id='{self.run_id}', status='{self.status}', group={self.group_id})>"