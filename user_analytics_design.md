# User Analytics Design

## Overview

User analytics aggregates per-message analytics from `message_analytics` table to create comprehensive user profiles. Since we have rich NLP analysis for each message (sentiment, topics, intent, etc.), we can derive deep insights about user behavior, expertise, and communication patterns.

## Key Focus: Topic Analysis

Topics are the most valuable insight because they reveal:
- **What users care about** (interests)
- **What they know about** (expertise)
- **How they contribute** (value to community)
- **Who to engage for what** (routing/matching)

## Core Metrics

### 1. Topic Intelligence (PRIMARY FOCUS)

#### Top Topics Metrics
```python
# Top 5 favorite topics (by frequency)
favorite_topics_json: [
    {"topic": "bitcoin", "count": 45, "percentage": 23.5},
    {"topic": "trading", "count": 38, "percentage": 19.8},
    {"topic": "defi", "count": 28, "percentage": 14.6},
    {"topic": "news", "count": 20, "percentage": 10.4},
    {"topic": "price", "count": 18, "percentage": 9.4}
]

# Topic expertise (topics with high-value contributions)
expertise_topics_json: [
    {"topic": "defi", "avg_value": "high", "confidence": 0.85},
    {"topic": "smart_contracts", "avg_value": "high", "confidence": 0.78}
]

# Topic sentiment (how they feel about topics)
topic_sentiments_json: {
    "bitcoin": "positive",
    "regulation": "negative",
    "ethereum": "neutral"
}

# Topic depth score (0-100)
# Measures if user provides detailed analysis vs surface comments
topic_depth_score: 78

# Topic diversity index (0-1)
# 0 = talks about one thing, 1 = talks about everything
topic_diversity: 0.34
```

#### Derived Topic Insights
- **Primary domain**: Main area of contribution (e.g., "Technical Analysis")
- **Secondary interests**: Supporting topics
- **Topic evolution**: How interests change over time
- **Cross-topic connections**: Links different topics (valuable for insights)

### 2. Communication Profile

```python
# Style metrics
sentiment_distribution: {"positive": 0.6, "negative": 0.1, "neutral": 0.3}
dominant_emotion: "joy"
avg_sentiment_score: 0.65
emotional_range: 0.4  # 0=same emotion, 1=very varied

# Message types
message_type_distribution: {
    "question": 0.2,
    "answer": 0.35,
    "statement": 0.3,
    "opinion": 0.15
}

# Formality
formality_level: "semi-formal"
avg_message_complexity: "medium"
avg_message_length: 127
```

### 3. Behavioral Patterns

```python
# Activity
total_messages: 534
active_days: 45
messages_per_active_day: 11.9
first_seen: "2024-01-15"
last_seen: "2024-11-18"
consistency_score: 72  # How regularly they post

# Timing
most_active_hour: 14  # 2 PM
most_active_day: 2  # Tuesday
response_time_minutes: 8.5  # How fast they reply
weekend_activity_ratio: 0.3

# Engagement style
conversation_starter_ratio: 0.15
reply_ratio: 0.65
thread_participation_avg: 3.2  # messages per thread
```

### 4. Content Quality Metrics

```python
# Value metrics
avg_information_density: 0.72
avg_conversation_value: "medium-high"
vocabulary_richness: 0.45
unique_words_used: 1823

# Contribution types
links_shared: 45
media_shared: 12
code_snippets_shared: 8
references_cited: 23

# Quality scores
signal_to_noise_ratio: 0.83
actionable_content_ratio: 0.22
insight_generation_score: 65
```

### 5. Social Dynamics

```python
# Roles
conversation_roles: {
    "questioner": 0.2,
    "answerer": 0.35,
    "moderator": 0.05,
    "observer": 0.1,
    "commenter": 0.3
}

# Influence
replies_received_ratio: 0.42  # Their messages that get replies
mentions_received: 89
influence_score: 67  # Composite metric

# Network position
centrality_score: 0.54  # 0=peripheral, 1=central
interaction_diversity: 32  # Number of unique users interacted with
```

### 6. Safety & Trust

```python
# Risk metrics
toxicity_rate: 0.02
spam_score: 0.05
misinformation_risk: 0.08
promotional_content_ratio: 0.03

# Trust score (0-100)
trust_score: 87  # Composite of quality and safety metrics
```

### 7. LLM-Generated Insights

```python
# Personality profile (updated monthly)
personality_type: "Analytical Contributor"
communication_style: "Direct, fact-based, helpful"

# Expertise summary (updated monthly)
expertise_summary: """
Experienced DeFi analyst with deep knowledge of liquidity protocols 
and yield farming strategies. Provides data-driven insights and 
frequently shares market analysis. Particularly strong in explaining 
complex concepts in accessible terms.
"""

# Topic expertise paragraph (updated weekly)
topic_expertise_summary: """
Primary expertise in DeFi protocols (Uniswap, Aave, Compound) with 
strong technical understanding. Regularly discusses yield optimization,
impermanent loss, and smart contract security. Shows growing interest 
in L2 scaling solutions and cross-chain bridges.
"""

# Engagement recommendation
engagement_style: "Responds well to technical questions. Appreciates data-backed arguments."

# Metadata
llm_model: "gpt-4"
llm_tokens_used: 2500
llm_cost: 0.075
llm_analyzed_at: "2024-11-18"
```

## Implementation Strategy

### Phase 1: Basic Aggregations (SQL-based)
Compute from `message_analytics`:
- Message counts and averages
- Topic frequency from `topic` field
- Sentiment/emotion distributions
- Basic behavioral patterns

### Phase 2: Topic Intelligence (Python + SQL)
- Extract topics from each message's `topic` field
- Aggregate `mentioned_topics_json` from messages
- Calculate topic-sentiment correlations
- Build topic expertise scores

### Phase 3: LLM Enhancement
- Generate personality profiles
- Create expertise summaries
- Identify topic expertise
- Generate engagement recommendations

## Table Schema

```sql
CREATE TABLE user_analytics (
    -- Primary Keys
    user_id TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    
    -- Metadata
    analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    analysis_version TEXT DEFAULT '1.0',
    
    -- Basic Metrics
    total_messages INTEGER,
    active_days INTEGER,
    first_seen DATE,
    last_seen DATE,
    
    -- Topic Intelligence (PRIMARY FOCUS)
    favorite_topics_json TEXT,  -- Top 5 topics by frequency
    expertise_topics_json TEXT,  -- Topics with high-value contributions
    topic_sentiments_json TEXT,  -- Sentiment per topic
    topic_depth_score REAL,  -- 0-100
    topic_diversity REAL,  -- 0-1
    primary_domain TEXT,  -- Main area of expertise
    
    -- Communication Profile
    sentiment_distribution_json TEXT,
    dominant_emotion TEXT,
    avg_sentiment_score REAL,
    message_type_distribution_json TEXT,
    formality_level TEXT,
    avg_message_length REAL,
    
    -- Behavioral Patterns
    messages_per_active_day REAL,
    consistency_score REAL,
    most_active_hour INTEGER,
    avg_response_time_minutes REAL,
    conversation_starter_ratio REAL,
    reply_ratio REAL,
    
    -- Content Quality
    avg_information_density REAL,
    avg_conversation_value TEXT,
    vocabulary_richness REAL,
    unique_words_used INTEGER,
    links_shared INTEGER,
    
    -- Social Dynamics
    conversation_roles_json TEXT,
    replies_received_ratio REAL,
    mentions_received INTEGER,
    influence_score REAL,
    
    -- Safety & Trust
    toxicity_rate REAL,
    spam_score REAL,
    trust_score REAL,
    
    -- LLM Insights
    personality_type TEXT,
    communication_style TEXT,
    expertise_summary TEXT,
    topic_expertise_summary TEXT,
    engagement_style TEXT,
    llm_model TEXT,
    llm_cost REAL,
    llm_analyzed_at TIMESTAMP,
    
    PRIMARY KEY (user_id, chat_id)
);

CREATE INDEX idx_user_analytics_chat ON user_analytics(chat_id);
CREATE INDEX idx_user_analytics_trust ON user_analytics(trust_score);
CREATE INDEX idx_user_analytics_influence ON user_analytics(influence_score);
CREATE INDEX idx_user_analytics_primary_domain ON user_analytics(primary_domain);
```

## Query Examples

### Find Topic Experts
```sql
SELECT 
    user_id,
    expertise_summary,
    favorite_topics_json
FROM user_analytics
WHERE chat_id = ?
    AND favorite_topics_json LIKE '%bitcoin%'
    AND trust_score > 80
ORDER BY influence_score DESC;
```

### Identify High-Value Contributors
```sql
SELECT 
    user_id,
    total_messages,
    avg_conversation_value,
    topic_depth_score,
    primary_domain
FROM user_analytics
WHERE chat_id = ?
    AND avg_conversation_value IN ('high', 'medium-high')
    AND topic_depth_score > 70
ORDER BY influence_score DESC;
```

### Topic-Based User Matching
```sql
-- Find users who can answer DeFi questions
SELECT 
    user_id,
    expertise_topics_json,
    topic_expertise_summary
FROM user_analytics
WHERE chat_id = ?
    AND expertise_topics_json LIKE '%defi%'
    AND conversation_roles_json LIKE '%"answerer": 0.%'
ORDER BY trust_score DESC;
```

## Value Proposition

1. **Content Routing**: Match questions to topic experts
2. **Community Insights**: Understand expertise distribution
3. **User Engagement**: Personalized engagement based on interests
4. **Quality Control**: Identify valuable vs noisy contributors
5. **Topic Analysis**: See what the community cares about
6. **Expert Discovery**: Find hidden experts in specific domains

## Update Frequency

- **Daily**: Basic metrics, behavioral patterns
- **Weekly**: Topic analysis, content quality
- **Monthly**: LLM insights, personality profiles

This design prioritizes topic intelligence while maintaining comprehensive user profiling for maximum insight value.