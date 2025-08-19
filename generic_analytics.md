# Generic Analytics Queries

## Overview
SQL-based analytics queries that work on both SQLite and BigQuery with minimal modifications.
Queries are organized in a sequential pipeline: Messages → Users → Chat

---

## 1. Message Analytics (Foundation Layer)

### 1.1 Individual Message Characteristics

**Purpose**: Analyzes properties of each individual message - length, type, timing, and patterns.

**Insights**: 
- Message length and complexity
- Whether it's a question, statement, or reply
- Time gaps between messages
- Language patterns in individual messages

```sql
-- Analyze individual messages with their characteristics
SELECT 
    message_id,
    user_id,
    message_text,
    LENGTH(message_text) as char_count,
    LENGTH(message_text) - LENGTH(REPLACE(message_text, ' ', '')) + 1 as word_count,
    CASE 
        WHEN message_text LIKE '%?%' THEN 'question'
        WHEN message_text LIKE '%!%' THEN 'exclamation'
        WHEN message_text = UPPER(message_text) AND LENGTH(message_text) > 5 THEN 'shouting'
        ELSE 'statement'
    END as message_type,
    CASE 
        WHEN reply_to_id IS NOT NULL THEN 'reply'
        ELSE 'new_thread'
    END as conversation_type,
    timestamp,
    LAG(timestamp) OVER (PARTITION BY user_id ORDER BY timestamp) as prev_message_time,
    (julianday(timestamp) - julianday(LAG(timestamp) OVER (PARTITION BY user_id ORDER BY timestamp))) * 24 * 60 as minutes_since_last_message
FROM messages
WHERE chat_id = ?
ORDER BY timestamp DESC
LIMIT 100;
```

**Example Output** (individual messages):
```
message_id | user_id | message_text                          | char_count | word_count | message_type | conversation_type | minutes_since_last_message
-----------|---------|---------------------------------------|------------|------------|--------------|-------------------|-------------------------
2216       | 123456  | "What's the current Bitcoin price?"   | 33         | 5          | question     | new_thread        | 45.2
2215       | 789012  | "HODL! 🚀"                           | 8          | 1          | exclamation  | reply             | 12.7
2214       | 345678  | "The market looks bullish today"      | 29         | 5          | statement    | new_thread        | 120.5
```

### 1.2 Message Distribution Over Time

**Purpose**: Identifies when the chat is most active, helping understand user behavior patterns and optimal engagement times.

**Insights**:
- Peak hours show when users are most engaged (useful for announcements)
- Day patterns reveal work vs weekend activity
- Daily trends show growth or decline in activity

#### Hourly Distribution
```sql
-- Messages per hour of day
SELECT 
    CAST(strftime('%H', timestamp) AS INTEGER) as hour,
    COUNT(*) as message_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM messages
WHERE chat_id = ?
GROUP BY hour
ORDER BY hour;
```

**Example Output**:
```
hour | message_count | percentage
-----|--------------|----------
09   | 125          | 5.76
10   | 189          | 8.71
11   | 201          | 9.26
...
21   | 342          | 15.77  <- Peak hour
22   | 298          | 13.74
```

#### Weekly Pattern
```sql
-- Messages per day of week (0=Sunday, 6=Saturday)
SELECT 
    CAST(strftime('%w', timestamp) AS INTEGER) as day_of_week,
    CASE CAST(strftime('%w', timestamp) AS INTEGER)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END as day_name,
    COUNT(*) as message_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM messages
WHERE chat_id = ?
GROUP BY day_of_week, day_name
ORDER BY day_of_week;
```

**Example Output**:
```
day_of_week | day_name  | message_count | percentage
------------|-----------|---------------|----------
1           | Monday    | 298           | 13.74
2           | Tuesday   | 345           | 15.90
3           | Wednesday | 389           | 17.93  <- Most active
4           | Thursday  | 367           | 16.92
5           | Friday    | 312           | 14.38
6           | Saturday  | 234           | 10.79
0           | Sunday    | 224           | 10.33
```

### 1.3 Message Types and Patterns

**Purpose**: Analyzes communication patterns to understand how users interact - are they asking questions, replying to others, or starting new topics?

**Insights**:
- High reply rate indicates engaged discussions
- Many questions suggest knowledge-seeking community
- Caps lock usage might indicate excitement or frustration

```sql
-- Reply vs new messages
SELECT 
    CASE 
        WHEN reply_to_id IS NOT NULL THEN 'reply'
        ELSE 'new'
    END as message_type,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM messages
WHERE chat_id = ?
GROUP BY message_type;
```

**Example Output**:
```
message_type | count | percentage
-------------|-------|----------
new          | 1456  | 67.11
reply        | 713   | 32.89
```

```sql
-- Question and exclamation patterns
SELECT 
    COUNT(CASE WHEN message_text LIKE '%?%' THEN 1 END) as questions,
    COUNT(CASE WHEN message_text LIKE '%!%' THEN 1 END) as exclamations,
    COUNT(CASE WHEN message_text = UPPER(message_text) AND LENGTH(message_text) > 5 THEN 1 END) as caps_messages,
    COUNT(*) as total_messages,
    ROUND(COUNT(CASE WHEN message_text LIKE '%?%' THEN 1 END) * 100.0 / COUNT(*), 2) as question_rate
FROM messages
WHERE chat_id = ?;
```

**Example Output**:
```
questions: 423
exclamations: 567
caps_messages: 34
total_messages: 2169
question_rate: 19.50%
```

### 1.4 Top Words Analysis

**Purpose**: Identifies the most frequently used words to understand main topics and themes discussed in the chat.

**Insights**:
- Reveals primary discussion topics
- Shows specialized vocabulary (technical terms, jargon)
- Helps identify spam or repetitive content

```sql
-- Most frequent words (SQLite version using simple approach)
-- Note: For production, use proper text processing
WITH words AS (
    SELECT LOWER(message_text) as text
    FROM messages
    WHERE chat_id = ?
)
-- Simplified version - in practice, use proper tokenization
SELECT 
    'bitcoin' as word, COUNT(*) as frequency FROM words WHERE text LIKE '%bitcoin%'
UNION ALL
SELECT 'price', COUNT(*) FROM words WHERE text LIKE '%price%'
UNION ALL
SELECT 'market', COUNT(*) FROM words WHERE text LIKE '%market%'
-- Add more words or use dynamic approach
ORDER BY frequency DESC
LIMIT 20;
```

**Example Output** (Bitcoin group):
```
word       | frequency
-----------|----------
bitcoin    | 892
price      | 743
market     | 651
crypto     | 589
trading    | 412
analysis   | 387
chart      | 342
```

### 1.5 Message Velocity and Bursts

**Purpose**: Identifies periods of unusually high activity, which often correspond to important events or heated discussions.

**Insights**:
- Burst periods indicate hot topics or events
- Velocity changes show engagement trends
- Helps identify what triggers high activity

```sql
-- Identify burst periods (high activity)
WITH hourly_stats AS (
    SELECT 
        strftime('%Y-%m-%d %H:00:00', timestamp) as hour_bucket,
        COUNT(*) as message_count
    FROM messages
    WHERE chat_id = ?
    GROUP BY hour_bucket
),
stats AS (
    SELECT 
        AVG(message_count) as avg_hourly,
        AVG(message_count) + 2 * STDEV(message_count) as burst_threshold
    FROM hourly_stats
)
SELECT 
    h.hour_bucket,
    h.message_count,
    ROUND(h.message_count * 1.0 / s.avg_hourly, 2) as intensity_ratio,
    'burst' as activity_level
FROM hourly_stats h, stats s
WHERE h.message_count > s.burst_threshold
ORDER BY h.message_count DESC
LIMIT 10;
```

**Example Output**:
```
hour_bucket          | message_count | intensity_ratio | activity_level
--------------------|---------------|-----------------|---------------
2024-03-12 14:00:00 | 89            | 4.45           | burst
2024-02-28 16:00:00 | 76            | 3.80           | burst
2024-04-01 13:00:00 | 71            | 3.55           | burst
```

---

## 2. User Analytics (Aggregation Layer)

### 2.1 User Activity Metrics

**Purpose**: Provides comprehensive view of individual user behavior and contribution levels.

**Insights**:
- Identifies most active contributors
- Shows user engagement patterns
- Helps identify lurkers vs active participants

```sql
-- Basic user statistics
SELECT 
    u.user_id,
    u.username,
    u.first_name || ' ' || u.last_name as full_name,
    COUNT(m.message_id) as total_messages,
    AVG(LENGTH(m.message_text)) as avg_message_length,
    MIN(m.timestamp) as first_message,
    MAX(m.timestamp) as last_message,
    COUNT(DISTINCT DATE(m.timestamp)) as active_days,
    ROUND(COUNT(m.message_id) * 100.0 / (SELECT COUNT(*) FROM messages WHERE chat_id = ?), 2) as message_share
FROM users u
LEFT JOIN messages m ON u.user_id = m.user_id AND u.platform = m.platform
WHERE m.chat_id = ?
GROUP BY u.user_id, u.username, full_name
ORDER BY total_messages DESC
LIMIT 20;
```

**Example Output**:
```
user_id  | username    | full_name      | total_messages | avg_msg_length | active_days | message_share
---------|-------------|----------------|----------------|----------------|-------------|-------------
123456   | @johndoe    | John Doe       | 234           | 145.3          | 67          | 10.79%
789012   | @janesmith  | Jane Smith     | 198           | 89.7           | 54          | 9.13%
345678   | @bobwilson  | Bob Wilson     | 156           | 234.1          | 89          | 7.19%
```

### 2.2 User Engagement Patterns

**Purpose**: Classifies users by engagement level and identifies interaction patterns between users.

**Insights**:
- 80/20 rule validation (20% of users create 80% of content)
- Identifies power users and influencers
- Shows community interaction networks

```sql
-- User ranking and engagement levels
WITH user_stats AS (
    SELECT 
        user_id,
        COUNT(*) as message_count,
        PERCENT_RANK() OVER (ORDER BY COUNT(*)) as percentile
    FROM messages
    WHERE chat_id = ?
    GROUP BY user_id
)
SELECT 
    user_id,
    message_count,
    ROUND(percentile * 100, 2) as percentile_rank,
    CASE 
        WHEN percentile >= 0.9 THEN 'power_user'
        WHEN percentile >= 0.75 THEN 'active'
        WHEN percentile >= 0.5 THEN 'regular'
        WHEN percentile >= 0.25 THEN 'occasional'
        ELSE 'lurker'
    END as engagement_level
FROM user_stats
ORDER BY message_count DESC;
```

**Example Output**:
```
user_id | message_count | percentile_rank | engagement_level
--------|---------------|-----------------|----------------
123456  | 234           | 98.5           | power_user
789012  | 198           | 95.2           | power_user
345678  | 156           | 89.7           | active
901234  | 89            | 76.3           | active
567890  | 45            | 52.1           | regular
234567  | 12            | 23.4           | lurker
```

### 2.3 User Time Patterns

**Purpose**: Analyzes when individual users are active and how consistent their participation is.

**Insights**:
- Identifies user time zones and availability
- Shows user commitment and consistency
- Helps predict user churn

```sql
-- User consistency (posting regularity)
WITH user_daily AS (
    SELECT 
        user_id,
        DATE(timestamp) as date,
        COUNT(*) as daily_messages
    FROM messages
    WHERE chat_id = ?
    GROUP BY user_id, DATE(timestamp)
),
user_consistency AS (
    SELECT 
        user_id,
        COUNT(DISTINCT date) as active_days,
        AVG(daily_messages) as avg_daily_messages,
        MIN(date) as first_active,
        MAX(date) as last_active,
        julianday(MAX(date)) - julianday(MIN(date)) + 1 as total_span_days
    FROM user_daily
    GROUP BY user_id
)
SELECT 
    user_id,
    active_days,
    total_span_days,
    ROUND(active_days * 100.0 / total_span_days, 2) as consistency_rate,
    ROUND(avg_daily_messages, 2) as avg_daily_messages,
    CASE 
        WHEN active_days * 1.0 / total_span_days > 0.5 THEN 'highly_consistent'
        WHEN active_days * 1.0 / total_span_days > 0.25 THEN 'regular'
        WHEN active_days * 1.0 / total_span_days > 0.1 THEN 'sporadic'
        ELSE 'rare'
    END as consistency_level
FROM user_consistency
WHERE total_span_days > 7  -- Active for at least a week
ORDER BY consistency_rate DESC;
```

**Example Output**:
```
user_id | active_days | total_span_days | consistency_rate | avg_daily_messages | consistency_level
--------|-------------|-----------------|------------------|-------------------|------------------
123456  | 89          | 120             | 74.17           | 3.4               | highly_consistent
789012  | 67          | 180             | 37.22           | 2.1               | regular
345678  | 23          | 200             | 11.50           | 5.6               | sporadic
```

### 2.4 User Content Patterns

**Purpose**: Analyzes the type of content users contribute - questions, conversations starters, or replies.

**Insights**:
- Identifies knowledge seekers vs knowledge providers
- Shows conversation leaders
- Helps understand user roles in the community

```sql
-- Users who ask questions vs provide answers
SELECT 
    user_id,
    COUNT(*) as total_messages,
    COUNT(CASE WHEN message_text LIKE '%?%' THEN 1 END) as questions,
    COUNT(CASE WHEN reply_to_id IS NULL THEN 1 END) as conversation_starters,
    COUNT(CASE WHEN reply_to_id IS NOT NULL THEN 1 END) as replies,
    ROUND(COUNT(CASE WHEN message_text LIKE '%?%' THEN 1 END) * 100.0 / COUNT(*), 2) as question_rate,
    ROUND(COUNT(CASE WHEN reply_to_id IS NULL THEN 1 END) * 100.0 / COUNT(*), 2) as starter_rate
FROM messages
WHERE chat_id = ?
GROUP BY user_id
HAVING total_messages > 10  -- Filter users with enough messages
ORDER BY total_messages DESC;
```

**Example Output**:
```
user_id | total_messages | questions | conversation_starters | replies | question_rate | starter_rate
--------|----------------|-----------|----------------------|---------|---------------|-------------
123456  | 234           | 67        | 145                  | 89      | 28.63%       | 61.97%
789012  | 198           | 12        | 45                   | 153     | 6.06%        | 22.73%
345678  | 156           | 89        | 67                   | 89      | 57.05%       | 42.95%
```

---

## 3. Chat Analytics (Summary Layer)

### 3.1 Overall Chat Health

**Purpose**: Provides high-level metrics about the chat's overall health, activity span, and member participation.

**Insights**:
- Chat duration (days between first and last message) shows the time span of captured conversations
- Participation rate shows what percentage of members are active
- Average messages per day indicates overall chat velocity
- Message distribution metrics reveal if few users dominate conversation

```sql
-- Chat health summary with duration metrics
WITH chat_stats AS (
    SELECT 
        c.chat_id,
        c.title,
        c.chat_type,
        c.participants_count as total_members,
        COUNT(DISTINCT m.user_id) as active_users,
        COUNT(m.message_id) as total_messages,
        MIN(m.timestamp) as first_message,
        MAX(m.timestamp) as last_message,
        CAST(julianday(MAX(m.timestamp)) - julianday(MIN(m.timestamp)) AS INTEGER) as chat_duration_days
    FROM chats c
    LEFT JOIN messages m ON c.chat_id = m.chat_id
    WHERE c.chat_id = ?
    GROUP BY c.chat_id, c.title, c.chat_type, c.participants_count
)
SELECT 
    chat_id,
    title,
    chat_type,
    total_members,
    active_users,
    total_messages,
    first_message,
    last_message,
    chat_duration_days,
    ROUND(active_users * 100.0 / NULLIF(total_members, 0), 2) as participation_rate,
    ROUND(total_messages * 1.0 / NULLIF(active_users, 0), 2) as avg_messages_per_user,
    ROUND(total_messages * 1.0 / NULLIF(chat_duration_days, 0), 2) as avg_messages_per_day
FROM chat_stats;
```

**Example Output**:
```
chat_id     | title         | chat_type | total_members | active_users | total_messages | first_message       | last_message        | chat_duration_days | participation_rate | avg_messages_per_user | avg_messages_per_day
------------|---------------|-----------|---------------|--------------|----------------|--------------------|--------------------|-------------------|-------------------|--------------------|--------------------
1670178185  | Bitcoinsensus | channel   | 2987          | 234          | 2169           | 2024-01-15 09:23:45 | 2024-08-18 14:32:10 | 216               | 7.83%            | 9.27               | 10.04
```

### 3.2 Chat Activity Trends

**Purpose**: Shows how chat activity changes over time, including growth rates and user retention.

**Insights**:
- Growth trends show if community is expanding or declining
- Retention rates indicate user satisfaction
- Weekly patterns reveal activity cycles

```sql
-- Weekly activity trend with growth
WITH weekly_stats AS (
    SELECT 
        strftime('%Y-W%W', timestamp) as week,
        COUNT(*) as messages,
        COUNT(DISTINCT user_id) as active_users,
        COUNT(DISTINCT CASE WHEN reply_to_id IS NOT NULL THEN message_id END) as replies
    FROM messages
    WHERE chat_id = ?
    GROUP BY week
)
SELECT 
    week,
    messages,
    active_users,
    replies,
    LAG(messages) OVER (ORDER BY week) as prev_week_messages,
    ROUND((messages - LAG(messages) OVER (ORDER BY week)) * 100.0 / 
          NULLIF(LAG(messages) OVER (ORDER BY week), 0), 2) as growth_rate
FROM weekly_stats
ORDER BY week DESC
LIMIT 10;
```

**Example Output**:
```
week     | messages | active_users | replies | prev_week_messages | growth_rate
---------|----------|--------------|---------|-------------------|------------
2024-W33 | 456      | 89           | 123     | 398               | 14.57%
2024-W32 | 398      | 76           | 98      | 412               | -3.40%
2024-W31 | 412      | 82           | 134     | 367               | 12.26%
2024-W30 | 367      | 71           | 89      | 389               | -5.66%
```

### 3.3 Conversation Dynamics

**Purpose**: Analyzes how conversations flow, including response times and discussion depth.

**Insights**:
- Fast response times indicate engaged real-time discussions
- Deep threads show substantive conversations
- Response patterns reveal community responsiveness

```sql
-- Response time analysis
WITH message_pairs AS (
    SELECT 
        m1.message_id,
        m1.user_id as responder,
        m1.timestamp as response_time,
        m2.timestamp as original_time,
        (julianday(m1.timestamp) - julianday(m2.timestamp)) * 24 * 60 as response_minutes
    FROM messages m1
    JOIN messages m2 ON m1.reply_to_id = m2.message_id
    WHERE m1.chat_id = ? AND m2.chat_id = ?
        AND (julianday(m1.timestamp) - julianday(m2.timestamp)) * 24 < 24  -- Within 24 hours
)
SELECT 
    COUNT(*) as total_replies,
    ROUND(AVG(response_minutes), 2) as avg_response_minutes,
    ROUND(MIN(response_minutes), 2) as fastest_response,
    ROUND(MAX(response_minutes), 2) as slowest_response,
    COUNT(CASE WHEN response_minutes < 5 THEN 1 END) as quick_replies_under_5min,
    COUNT(CASE WHEN response_minutes < 60 THEN 1 END) as replies_within_hour
FROM message_pairs;
```

**Example Output**:
```
total_replies | avg_response_minutes | fastest_response | slowest_response | quick_replies_under_5min | replies_within_hour
--------------|---------------------|------------------|------------------|-------------------------|-------------------
713           | 127.45              | 0.23             | 1439.87          | 234                     | 456
```

---

## Implementation Notes

### SQLite vs BigQuery Differences

| SQLite | BigQuery |
|--------|----------|
| `strftime('%Y-%m-%d', timestamp)` | `FORMAT_TIMESTAMP('%Y-%m-%d', timestamp)` |
| `julianday(date1) - julianday(date2)` | `DATE_DIFF(date1, date2, DAY)` |
| `CAST(x AS INTEGER)` | `CAST(x AS INT64)` |
| `STDEV()` | `STDDEV()` |

### Performance Optimization

1. **Indexing**: Create indexes on `chat_id`, `user_id`, `timestamp`, and `reply_to_id`
2. **Partitioning**: In BigQuery, partition by `timestamp` for better performance
3. **Materialized Views**: Pre-calculate common aggregations for faster queries
4. **Batch Processing**: Run heavy analytics during off-peak hours

### Usage Guidelines

1. **Sequential Execution**: Run message analytics first, then user, then chat
2. **Parameter Substitution**: Replace `?` with actual chat_id values
3. **Result Caching**: Cache results for frequently accessed metrics
4. **Incremental Updates**: Design queries to work with date ranges for incremental processing