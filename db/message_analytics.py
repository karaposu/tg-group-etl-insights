"""
SQLAlchemy model for message analytics table
"""

from sqlalchemy import (
    Column, String, Integer, Float, Boolean, Text, 
    DateTime, ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.sql import func
from .base import Base


class MessageAnalytics(Base):
    """
    Detailed per-message analytics table containing both
    generic (rule-based) and advanced (LLM-based) analysis results
    """
    __tablename__ = 'message_analytics'
    
    # Basic Identifiers
    message_id = Column(String, primary_key=True)
    chat_id = Column(String, nullable=False)
    user_id = Column(String, nullable=False)
    platform = Column(String, nullable=False)
    analyzed_at = Column(DateTime, server_default=func.now(), nullable=False)
    analysis_version = Column(String, default='1.0')
    
    # Text Metrics (Generic)
    char_count = Column(Integer)
    word_count = Column(Integer)
    sentence_count = Column(Integer)
    avg_word_length = Column(Float)
    unique_word_count = Column(Integer)
    lexical_diversity = Column(Float)  # unique/total words ratio
    
    # Content Flags (Generic)
    contains_question = Column(Boolean, default=False)
    caps_ratio = Column(Float)  # % of caps letters
    emoji_count = Column(Integer, default=0)
    contains_link = Column(Boolean, default=False)
    link_count = Column(Integer, default=0)
    contains_mention = Column(Boolean, default=False)
    mention_count = Column(Integer, default=0)
    contains_hashtag = Column(Boolean, default=False)
    hashtag_count = Column(Integer, default=0)
    contains_media = Column(Boolean, default=False)
    media_type = Column(String)  # photo/video/document/audio
    
    # Language & Readability (Generic)
    language_code = Column(String)  # ISO 639-1 code
    language_confidence = Column(Float)
    complexity_level = Column(String)  # simple/medium/complex
    
    # Conversation Context (Generic)
    is_reply = Column(Boolean, default=False)
    time_since_prev_message = Column(Integer)  # seconds
    hour_of_day = Column(Integer)  # 0-23
    day_of_week = Column(Integer)  # 0-6 (Monday-Sunday)
    is_conversation_starter = Column(Boolean, default=False)
    
    # ==========================================
    # Advanced Analysis Fields (LLM-based)
    # ==========================================
    
    # Sentiment & Emotion
    sentiment = Column(String)  # positive/negative/neutral
    emotion = Column(String)  # joy/anger/fear/sadness/surprise/disgust
    emotion_confidence = Column(Float)
    tone = Column(String)  # friendly/professional/casual/aggressive
    formality_level = Column(String)  # formal/semi-formal/informal
    
    # Content Classification
    topic = Column(String)  # main topic/theme
    topic_confidence = Column(Float)
    intent = Column(String)  # question/answer/statement/request/greeting/goodbye
    message_type = Column(String)  # informational/opinion/personal/promotional
    summary = Column(Text)  # brief summary for long messages
    
    # Quality & Safety
    toxicity_score = Column(Float)  # 0.0 to 1.0
    spam_score = Column(Float)
    is_promotional = Column(Boolean, default=False)
    contains_financial_advice = Column(Boolean, default=False)
    contains_legal_advice = Column(Boolean, default=False)
    misinformation_risk = Column(Float)
    
    # Engagement Metrics
    information_density = Column(Float)  # info vs filler ratio
    conversation_value = Column(String)  # high/medium/low
    requires_response = Column(Boolean, default=False)
    response_urgency = Column(String)  # none/low/medium/high
    conversation_role = Column(String)  # questioner/answerer/moderator/observer
    
    # Extracted Entities (stored as JSON)
    entities_json = Column(Text)  # {"people": [], "places": [], "orgs": []}
    key_phrases_json = Column(Text)  # ["phrase1", "phrase2"]
    mentioned_topics_json = Column(Text)  # ["topic1", "topic2"]
    action_items_json = Column(Text)  # ["action1", "action2"]
    
    # LLM Processing Metadata
    llm_model = Column(String)  # gpt-4/claude-3/etc
    llm_processing_time = Column(Float)  # seconds
    llm_total_tokens = Column(Integer)
    llm_total_cost = Column(Float)  # USD
    llm_usage_json = Column(Text)  # Full usage details as JSON
    requires_reanalysis = Column(Boolean, default=False)
    
    # Indexes for performance
    __table_args__ = (
        Index('idx_msg_analytics_chat', 'chat_id'),
        Index('idx_msg_analytics_user', 'user_id'),
        Index('idx_msg_analytics_analyzed', 'analyzed_at'),
        Index('idx_msg_analytics_sentiment', 'sentiment'),
        Index('idx_msg_analytics_topic', 'topic'),
        Index('idx_msg_analytics_conversation_value', 'conversation_value'),
        Index('idx_msg_analytics_requires_response', 'requires_response'),
        Index('idx_msg_analytics_reanalysis', 'requires_reanalysis'),
        # Composite indexes for common queries
        Index('idx_msg_analytics_chat_sentiment', 'chat_id', 'sentiment'),
        Index('idx_msg_analytics_chat_user', 'chat_id', 'user_id'),
        Index('idx_msg_analytics_chat_topic', 'chat_id', 'topic'),
    )
    
    def __repr__(self):
        return f"<MessageAnalytics(message_id={self.message_id}, sentiment={self.sentiment}, topic={self.topic})>"
    
    def to_dict(self):
        """Convert to dictionary for serialization"""
        return {
            'message_id': self.message_id,
            'chat_id': self.chat_id,
            'user_id': self.user_id,
            'platform': self.platform,
            'analyzed_at': self.analyzed_at.isoformat() if self.analyzed_at else None,
            'analysis_version': self.analysis_version,
            # Text metrics
            'char_count': self.char_count,
            'word_count': self.word_count,
            'sentence_count': self.sentence_count,
            'avg_word_length': self.avg_word_length,
            'unique_word_count': self.unique_word_count,
            'lexical_diversity': self.lexical_diversity,
            # Content flags
            'contains_question': self.contains_question,
            'caps_ratio': self.caps_ratio,
            'emoji_count': self.emoji_count,
            'contains_link': self.contains_link,
            'link_count': self.link_count,
            'contains_mention': self.contains_mention,
            'mention_count': self.mention_count,
            'contains_hashtag': self.contains_hashtag,
            'hashtag_count': self.hashtag_count,
            'contains_media': self.contains_media,
            'media_type': self.media_type,
            # Language
            'language_code': self.language_code,
            'language_confidence': self.language_confidence,
            'complexity_level': self.complexity_level,
            # Context
            'is_reply': self.is_reply,
            'time_since_prev_message': self.time_since_prev_message,
            'hour_of_day': self.hour_of_day,
            'day_of_week': self.day_of_week,
            'is_conversation_starter': self.is_conversation_starter,
            # Advanced analysis
            'sentiment': self.sentiment,
            'emotion': self.emotion,
            'emotion_confidence': self.emotion_confidence,
            'tone': self.tone,
            'formality_level': self.formality_level,
            'topic': self.topic,
            'topic_confidence': self.topic_confidence,
            'intent': self.intent,
            'message_type': self.message_type,
            'summary': self.summary,
            'toxicity_score': self.toxicity_score,
            'spam_score': self.spam_score,
            'is_promotional': self.is_promotional,
            'contains_financial_advice': self.contains_financial_advice,
            'contains_legal_advice': self.contains_legal_advice,
            'misinformation_risk': self.misinformation_risk,
            'information_density': self.information_density,
            'conversation_value': self.conversation_value,
            'requires_response': self.requires_response,
            'response_urgency': self.response_urgency,
            'conversation_role': self.conversation_role,
            'entities_json': self.entities_json,
            'key_phrases_json': self.key_phrases_json,
            'mentioned_topics_json': self.mentioned_topics_json,
            'action_items_json': self.action_items_json,
            # LLM metadata
            'llm_model': self.llm_model,
            'llm_processing_time': self.llm_processing_time,
            'llm_total_tokens': self.llm_total_tokens,
            'llm_total_cost': self.llm_total_cost,
            'llm_usage_json': self.llm_usage_json,
            'requires_reanalysis': self.requires_reanalysis
        }
    
    @classmethod
    def from_generic_analysis(cls, message_id: str, chat_id: str, user_id: str, 
                              platform: str, analysis_results: dict):
        """
        Create instance from generic (non-LLM) analysis results
        
        Args:
            message_id: Message ID
            chat_id: Chat ID
            user_id: User ID
            platform: Platform name
            analysis_results: Dictionary with analysis results
            
        Returns:
            MessageAnalytics instance
        """
        return cls(
            message_id=message_id,
            chat_id=chat_id,
            user_id=user_id,
            platform=platform,
            **analysis_results
        )
    
    def update_llm_analysis(self, llm_results: dict):
        """
        Update instance with LLM analysis results
        
        Args:
            llm_results: Dictionary with LLM analysis results
        """
        for key, value in llm_results.items():
            if hasattr(self, key):
                setattr(self, key, value)
        
        # Mark as analyzed
        self.analyzed_at = func.now()
        self.requires_reanalysis = False