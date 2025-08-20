#!/usr/bin/env python3
"""
Script to create the user_analytics table in the database
"""

import sys
from pathlib import Path

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db import get_engine, UserAnalytics
from sqlalchemy import inspect, text


def main():
    """Create user_analytics table"""
    
    print("Creating user_analytics table...")
    
    # Get engine with SQLite database
    db_path = "data/telegram.db"
    engine = get_engine(f"sqlite:///{db_path}")
    
    # Check if table exists
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    
    if 'user_analytics' in existing_tables:
        print("Table 'user_analytics' already exists")
        
        # Show column count
        columns = inspector.get_columns('user_analytics')
        print(f"Current table has {len(columns)} columns")
        
        response = input("Do you want to drop and recreate it? (y/n): ")
        if response.lower() == 'y':
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE user_analytics"))
                conn.commit()
                print("Table dropped")
        else:
            print("Keeping existing table")
            return
    
    # Create table
    UserAnalytics.__table__.create(engine)
    print("Table 'user_analytics' created successfully")
    
    # Show table structure
    inspector = inspect(engine)
    columns = inspector.get_columns('user_analytics')
    
    print(f"\nTable structure ({len(columns)} columns):")
    print("-" * 60)
    
    # Group columns by category
    categories = {
        'Primary Keys & Metadata': ['user_id', 'chat_id', 'analyzed_at', 'analysis_version'],
        'Basic Metrics': ['total_messages', 'active_days', 'first_seen', 'last_seen'],
        'Topic Intelligence': [
            'favorite_topics_json', 'expertise_topics_json', 'topic_sentiments_json',
            'topic_depth_score', 'topic_diversity', 'primary_domain'
        ],
        'Communication Profile': [
            'sentiment_distribution_json', 'dominant_emotion', 'avg_sentiment_score',
            'message_type_distribution_json', 'formality_level', 'avg_message_length'
        ],
        'Behavioral Patterns': [
            'messages_per_active_day', 'consistency_score', 'most_active_hour',
            'avg_response_time_minutes', 'conversation_starter_ratio', 'reply_ratio'
        ],
        'Content Quality': [
            'avg_information_density', 'avg_conversation_value', 'vocabulary_richness',
            'unique_words_used', 'links_shared', 'media_shared'
        ],
        'Social Dynamics': [
            'conversation_roles_json', 'replies_received_ratio', 'mentions_received',
            'influence_score'
        ],
        'Safety & Trust': ['toxicity_rate', 'spam_score', 'trust_score'],
        'LLM Insights': [
            'personality_type', 'communication_style', 'expertise_summary',
            'topic_expertise_summary', 'engagement_style', 'llm_model', 'llm_cost',
            'llm_analyzed_at'
        ]
    }
    
    column_dict = {col['name']: col for col in columns}
    
    for category, col_names in categories.items():
        print(f"\n{category}:")
        for col_name in col_names:
            if col_name in column_dict:
                col = column_dict[col_name]
                nullable = "" if col.get('nullable', True) else " NOT NULL"
                print(f"  - {col['name']}: {col['type']}{nullable}")
    
    # Show indexes
    indexes = inspector.get_indexes('user_analytics')
    if indexes:
        print(f"\nIndexes ({len(indexes)}):")
        for idx in indexes:
            print(f"  - {idx['name']}: {', '.join(idx['column_names'])}")
    
    # Show primary key
    pk = inspector.get_pk_constraint('user_analytics')
    if pk and pk.get('constrained_columns'):
        print(f"\nPrimary Key: {', '.join(pk['constrained_columns'])}")
    
    print("\n✓ Table creation complete!")
    
    # Show example queries
    print("\nExample queries:")
    print("-" * 60)
    print("""
-- Find topic experts
SELECT user_id, expertise_summary, favorite_topics_json
FROM user_analytics
WHERE chat_id = '1670178185'
  AND favorite_topics_json LIKE '%bitcoin%'
  AND trust_score > 80
ORDER BY influence_score DESC;

-- Find high-value contributors
SELECT user_id, total_messages, avg_conversation_value, primary_domain
FROM user_analytics
WHERE chat_id = '1670178185'
  AND avg_conversation_value IN ('high', 'medium-high')
ORDER BY influence_score DESC;
    """)


if __name__ == "__main__":
    main()