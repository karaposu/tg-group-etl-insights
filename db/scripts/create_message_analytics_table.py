#!/usr/bin/env python3
"""
Script to create the message_analytics table in the database
"""

import sys
from pathlib import Path

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db import create_all_tables, get_engine, MessageAnalytics
from sqlalchemy import inspect, text


def main():
    """Create message_analytics table"""
    
    print("Creating message_analytics table...")
    
    # Get engine with SQLite database
    db_path = "data/telegram.db"
    engine = get_engine(f"sqlite:///{db_path}")
    
    # Check if table exists
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    
    if 'message_analytics' in existing_tables:
        print("Table 'message_analytics' already exists")
        
        # Show column count
        columns = inspector.get_columns('message_analytics')
        print(f"Current table has {len(columns)} columns")
        
        response = input("Do you want to drop and recreate it? (y/n): ")
        if response.lower() == 'y':
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE message_analytics"))
                conn.commit()
                print("Table dropped")
        else:
            print("Keeping existing table")
            return
    
    # Create table
    MessageAnalytics.__table__.create(engine)
    print("Table 'message_analytics' created successfully")
    
    # Show table structure
    inspector = inspect(engine)
    columns = inspector.get_columns('message_analytics')
    
    print(f"\nTable structure ({len(columns)} columns):")
    print("-" * 60)
    
    # Group columns by category
    categories = {
        'identifiers': ['message_id', 'chat_id', 'user_id', 'platform', 'analyzed_at', 'analysis_version'],
        'text_metrics': ['char_count', 'word_count', 'sentence_count', 'avg_word_length', 'unique_word_count', 'lexical_diversity'],
        'content_flags': ['contains_question', 'caps_ratio', 'emoji_count', 'contains_link', 'link_count', 
                         'contains_mention', 'mention_count', 'contains_hashtag', 'hashtag_count', 
                         'contains_media', 'media_type'],
        'language': ['language_code', 'language_confidence', 'complexity_level'],
        'context': ['is_reply', 'time_since_prev_message', 'hour_of_day', 'day_of_week', 'is_conversation_starter'],
        'sentiment': ['sentiment', 'emotion', 'emotion_confidence', 'tone', 'formality_level'],
        'classification': ['topic', 'topic_confidence', 'intent', 'message_type', 'summary'],
        'quality': ['toxicity_score', 'spam_score', 'is_promotional', 'contains_financial_advice', 
                   'contains_legal_advice', 'misinformation_risk'],
        'engagement': ['information_density', 'conversation_value', 'requires_response', 'response_urgency', 'conversation_role'],
        'entities': ['entities_json', 'key_phrases_json', 'mentioned_topics_json', 'action_items_json'],
        'llm_metadata': ['llm_model', 'llm_processing_time', 'llm_total_tokens', 'llm_total_cost', 
                        'llm_usage_json', 'requires_reanalysis']
    }
    
    column_dict = {col['name']: col for col in columns}
    
    for category, col_names in categories.items():
        print(f"\n{category.upper().replace('_', ' ')}:")
        for col_name in col_names:
            if col_name in column_dict:
                col = column_dict[col_name]
                print(f"  - {col['name']}: {col['type']}")
    
    # Show indexes
    indexes = inspector.get_indexes('message_analytics')
    if indexes:
        print(f"\nIndexes ({len(indexes)}):")
        for idx in indexes:
            print(f"  - {idx['name']}: {', '.join(idx['column_names'])}")
    
    print("\n✓ Table creation complete!")


if __name__ == "__main__":
    main()