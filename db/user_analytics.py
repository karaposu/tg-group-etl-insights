"""
SQLAlchemy model for user analytics table
"""

from sqlalchemy import (
    Column, String, Integer, Float, Text, DateTime, Date,
    PrimaryKeyConstraint, Index
)
from sqlalchemy.sql import func
from .base import Base


class UserAnalytics(Base):
    """
    User analytics table aggregating insights from message_analytics.
    One row per user per chat with comprehensive behavioral and topic analysis.
    """
    __tablename__ = 'user_analytics'
    
    # Primary Keys
    user_id = Column(String, nullable=False)
    chat_id = Column(String, nullable=False)
    
    # Metadata
    analyzed_at = Column(DateTime, server_default=func.now(), nullable=False)
    analysis_version = Column(String, default='1.0')
    
    # Basic Metrics
    total_messages = Column(Integer, default=0)
    active_days = Column(Integer, default=0)
    first_seen = Column(Date)
    last_seen = Column(Date)
    
    # ==============================================
    # Topic Intelligence (PRIMARY FOCUS)
    # ==============================================
    
    # Top 5 favorite topics with counts and percentages
    # Example: [{"topic": "bitcoin", "count": 45, "percentage": 23.5}, ...]
    favorite_topics_json = Column(Text)
    
    # Topics where user provides high-value contributions
    # Example: [{"topic": "defi", "avg_value": "high", "confidence": 0.85}, ...]
    expertise_topics_json = Column(Text)
    
    # How user feels about different topics
    # Example: {"bitcoin": "positive", "regulation": "negative"}
    topic_sentiments_json = Column(Text)
    
    # Depth of topic discussions (0-100)
    # High score = detailed analysis, Low score = surface comments
    topic_depth_score = Column(Float)
    
    # Topic diversity (0-1)
    # 0 = talks about one thing, 1 = talks about everything
    topic_diversity = Column(Float)
    
    # Main area of expertise
    primary_domain = Column(String)  # e.g., "Technical Analysis", "DeFi", "News"
    
    # ==============================================
    # Communication Profile
    # ==============================================
    
    # Sentiment distribution
    # Example: {"positive": 0.6, "negative": 0.1, "neutral": 0.3}
    sentiment_distribution_json = Column(Text)
    
    dominant_emotion = Column(String)  # joy/anger/fear/sadness/surprise
    avg_sentiment_score = Column(Float)  # Average sentiment (-1 to 1)
    
    # Message type distribution
    # Example: {"question": 0.2, "answer": 0.35, "statement": 0.3, "opinion": 0.15}
    message_type_distribution_json = Column(Text)
    
    formality_level = Column(String)  # formal/semi-formal/informal
    avg_message_length = Column(Float)
    
    # ==============================================
    # Behavioral Patterns
    # ==============================================
    
    messages_per_active_day = Column(Float)
    consistency_score = Column(Float)  # 0-100, how regularly they post
    most_active_hour = Column(Integer)  # 0-23
    avg_response_time_minutes = Column(Float)
    conversation_starter_ratio = Column(Float)  # % of messages that start threads
    reply_ratio = Column(Float)  # % of messages that are replies
    
    # ==============================================
    # Content Quality
    # ==============================================
    
    avg_information_density = Column(Float)  # 0-1, substance vs fluff
    avg_conversation_value = Column(String)  # high/medium/low
    vocabulary_richness = Column(Float)  # unique words / total words
    unique_words_used = Column(Integer)
    links_shared = Column(Integer)
    media_shared = Column(Integer)
    
    # ==============================================
    # Social Dynamics
    # ==============================================
    
    # Conversation roles breakdown
    # Example: {"questioner": 0.2, "answerer": 0.35, "moderator": 0.05, ...}
    conversation_roles_json = Column(Text)
    
    replies_received_ratio = Column(Float)  # Their messages that get replies
    mentions_received = Column(Integer)
    influence_score = Column(Float)  # 0-100, composite metric
    
    # ==============================================
    # Safety & Trust
    # ==============================================
    
    toxicity_rate = Column(Float)  # % of toxic messages
    spam_score = Column(Float)  # 0-1
    trust_score = Column(Float)  # 0-100, composite of quality and safety
    
    # ==============================================
    # LLM-Generated Insights
    # ==============================================
    
    personality_type = Column(String)  # e.g., "Analytical Contributor"
    communication_style = Column(Text)  # e.g., "Direct, fact-based, helpful"
    
    # Detailed expertise summary
    expertise_summary = Column(Text)
    
    # Topic-specific expertise description
    topic_expertise_summary = Column(Text)
    
    # How to best engage with this user
    engagement_style = Column(Text)
    
    # LLM Processing Metadata
    llm_model = Column(String)
    llm_cost = Column(Float)
    llm_analyzed_at = Column(DateTime)
    
    # Composite Primary Key
    __table_args__ = (
        PrimaryKeyConstraint('user_id', 'chat_id'),
        Index('idx_user_analytics_chat', 'chat_id'),
        Index('idx_user_analytics_trust', 'trust_score'),
        Index('idx_user_analytics_influence', 'influence_score'),
        Index('idx_user_analytics_primary_domain', 'primary_domain'),
        Index('idx_user_analytics_last_seen', 'last_seen'),
    )
    
    def __repr__(self):
        return f"<UserAnalytics(user_id={self.user_id}, chat_id={self.chat_id}, messages={self.total_messages})>"
    
    def to_dict(self):
        """Convert to dictionary for serialization"""
        return {
            'user_id': self.user_id,
            'chat_id': self.chat_id,
            'analyzed_at': self.analyzed_at.isoformat() if self.analyzed_at else None,
            'analysis_version': self.analysis_version,
            # Basic metrics
            'total_messages': self.total_messages,
            'active_days': self.active_days,
            'first_seen': self.first_seen.isoformat() if self.first_seen else None,
            'last_seen': self.last_seen.isoformat() if self.last_seen else None,
            # Topic intelligence
            'favorite_topics_json': self.favorite_topics_json,
            'expertise_topics_json': self.expertise_topics_json,
            'topic_sentiments_json': self.topic_sentiments_json,
            'topic_depth_score': self.topic_depth_score,
            'topic_diversity': self.topic_diversity,
            'primary_domain': self.primary_domain,
            # Communication profile
            'sentiment_distribution_json': self.sentiment_distribution_json,
            'dominant_emotion': self.dominant_emotion,
            'avg_sentiment_score': self.avg_sentiment_score,
            'message_type_distribution_json': self.message_type_distribution_json,
            'formality_level': self.formality_level,
            'avg_message_length': self.avg_message_length,
            # Behavioral patterns
            'messages_per_active_day': self.messages_per_active_day,
            'consistency_score': self.consistency_score,
            'most_active_hour': self.most_active_hour,
            'avg_response_time_minutes': self.avg_response_time_minutes,
            'conversation_starter_ratio': self.conversation_starter_ratio,
            'reply_ratio': self.reply_ratio,
            # Content quality
            'avg_information_density': self.avg_information_density,
            'avg_conversation_value': self.avg_conversation_value,
            'vocabulary_richness': self.vocabulary_richness,
            'unique_words_used': self.unique_words_used,
            'links_shared': self.links_shared,
            'media_shared': self.media_shared,
            # Social dynamics
            'conversation_roles_json': self.conversation_roles_json,
            'replies_received_ratio': self.replies_received_ratio,
            'mentions_received': self.mentions_received,
            'influence_score': self.influence_score,
            # Safety & trust
            'toxicity_rate': self.toxicity_rate,
            'spam_score': self.spam_score,
            'trust_score': self.trust_score,
            # LLM insights
            'personality_type': self.personality_type,
            'communication_style': self.communication_style,
            'expertise_summary': self.expertise_summary,
            'topic_expertise_summary': self.topic_expertise_summary,
            'engagement_style': self.engagement_style,
            'llm_model': self.llm_model,
            'llm_cost': self.llm_cost,
            'llm_analyzed_at': self.llm_analyzed_at.isoformat() if self.llm_analyzed_at else None
        }
    
    @property
    def is_expert(self):
        """Check if user is considered an expert based on metrics"""
        return (
            self.trust_score and self.trust_score > 80 and
            self.topic_depth_score and self.topic_depth_score > 70 and
            self.avg_conversation_value in ['high', 'medium-high']
        )
    
    @property
    def is_active(self):
        """Check if user is currently active"""
        from datetime import datetime, timedelta
        if self.last_seen:
            days_inactive = (datetime.now().date() - self.last_seen).days
            return days_inactive <= 30
        return False
    
    @property
    def engagement_level(self):
        """Categorize user engagement level"""
        if not self.messages_per_active_day:
            return 'inactive'
        elif self.messages_per_active_day >= 20:
            return 'power_user'
        elif self.messages_per_active_day >= 10:
            return 'highly_active'
        elif self.messages_per_active_day >= 5:
            return 'active'
        elif self.messages_per_active_day >= 1:
            return 'regular'
        else:
            return 'lurker'
    
    def get_favorite_topics(self):
        """Parse and return favorite topics as list"""
        import json
        if self.favorite_topics_json:
            try:
                return json.loads(self.favorite_topics_json)
            except:
                return []
        return []
    
    def get_expertise_topics(self):
        """Parse and return expertise topics as list"""
        import json
        if self.expertise_topics_json:
            try:
                return json.loads(self.expertise_topics_json)
            except:
                return []
        return []
    
    def matches_topic(self, topic):
        """Check if user is expert or interested in a topic"""
        topic_lower = topic.lower()
        
        # Check favorite topics
        favorites = self.get_favorite_topics()
        for fav in favorites:
            if topic_lower in fav.get('topic', '').lower():
                return True
        
        # Check expertise topics
        expertise = self.get_expertise_topics()
        for exp in expertise:
            if topic_lower in exp.get('topic', '').lower():
                return True
        
        # Check primary domain
        if self.primary_domain and topic_lower in self.primary_domain.lower():
            return True
        
        return False