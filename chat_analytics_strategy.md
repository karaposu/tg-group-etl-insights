# Chat Analytics Strategy

## The Challenge

Designing chat analytics tables is complex because:

1. **Data Overlap**: Some metrics could logically belong in multiple places
   - `first_message_date` - belongs in `chats` table? or `chat_analytics`?
   - `total_messages` - can be computed from `messages` table, so why store it?
   - `total_participants` - already in `chats.participants_count`

2. **Temporal Complexity**: Chats have multiple time dimensions
   - Point-in-time snapshots (what's happening now)
   - Historical aggregates (what happened overall)
   - Time-series trends (how things change over time)
   - Periodic summaries (daily/weekly/monthly rollups)

3. **Computation vs Storage Trade-offs**
   - Should we store `avg_messages_per_day` or compute it on-demand?
   - When do we update aggregate metrics?
   - How do we handle incremental updates efficiently?

4. **Analysis Types**
   - Descriptive: What happened? (counts, averages)
   - Diagnostic: Why did it happen? (patterns, correlations)
   - Predictive: What will happen? (trends, forecasts)
   - Prescriptive: What should we do? (recommendations)

## Final Table Structure

### Core Tables (What We Have)
```
chats table (static metadata):
- chat_id
- platform
- title
- username
- chat_type
- created_at (when we first saw it)
# Note: removed participants_count since it changes over time

messages table (raw data):
- message_id
- chat_id
- user_id
- message_text
- timestamp
- reply_to_id

message_analytics table (per-message analysis):
- message_id
- [62 columns of analysis]
- Includes both generic and LLM analysis
```

### The Only Additional Table We Need

```
chat_daily table:
- chat_id (PK)
- date (PK)
- total_member_count (snapshot of members on that day)
- total_message_count (messages sent that day)
- total_active_member_count (members who sent messages that day)
- daily_summary (LLM-generated summary of the day)
- llm_model (which model generated the summary)
- llm_cost (cost of generating summary)
- created_at (when this record was created)
```

That's it. Nothing more needed.

## Design Principles

1. **No Redundant Storage**: Don't store what can be computed from raw data
2. **Store Only Non-Deterministic Results**: LLM outputs, external API results
3. **Compute On-Demand**: Leverage SQL for real-time analytics
4. **Cache Strategically**: Only cache if performance requires it

## Revised Strategy: Minimal Storage, Maximum Computation

### What We Can Compute On-Demand (Don't Store)

Since every message has a timestamp and we keep all data, we can compute:

#### Basic Metrics
```sql
-- All basic chat metrics from messages table
SELECT 
    COUNT(*) as total_messages,
    COUNT(DISTINCT user_id) as active_users,
    MIN(timestamp) as first_message,
    MAX(timestamp) as last_message,
    COUNT(DISTINCT DATE(timestamp)) as active_days
FROM messages
WHERE chat_id = ?
```

#### Time-Series Analytics
```sql
-- Daily statistics computed on-demand
SELECT 
    DATE(timestamp) as date,
    COUNT(*) as messages_count,
    COUNT(DISTINCT user_id) as active_users,
    AVG(LENGTH(message_text)) as avg_message_length,
    SUM(CASE WHEN message_text LIKE '%?%' THEN 1 ELSE 0 END) as questions
FROM messages
WHERE chat_id = ?
GROUP BY DATE(timestamp)
ORDER BY date DESC
```

#### User Participation
```sql
-- User engagement levels
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
    COUNT(CASE WHEN percentile >= 0.8 THEN 1 END) as power_users,
    COUNT(CASE WHEN percentile < 0.2 THEN 1 END) as lurkers
FROM user_stats
```

#### Trend Analysis
```sql
-- Week-over-week comparison
WITH weekly AS (
    SELECT 
        strftime('%Y-W%W', timestamp) as week,
        COUNT(*) as messages,
        COUNT(DISTINCT user_id) as users
    FROM messages
    WHERE chat_id = ?
    GROUP BY week
)
SELECT 
    week,
    messages,
    messages - LAG(messages) OVER (ORDER BY week) as change,
    ROUND((messages - LAG(messages) OVER (ORDER BY week)) * 100.0 / 
          LAG(messages) OVER (ORDER BY week), 2) as growth_rate
FROM weekly
```

### Why We Need chat_daily Table

1. **Member count changes**: We need historical snapshots of membership
2. **LLM summaries**: Can't be recomputed (non-deterministic and expensive)
3. **Daily aggregates**: Convenient for dashboards and trends

### What We DON'T Need to Store

Everything else can be computed on-demand from messages and message_analytics tables:
- Hourly distributions
- User engagement levels  
- Response times
- Topic distributions
- Sentiment trends
- Weekly/monthly aggregates (compute from chat_daily)

All these can be calculated with SQL when needed.

### Performance Optimization (Only If Needed)

If computing on-demand becomes slow (unlikely for most use cases), create materialized views:

```sql
-- Create materialized view for expensive queries
CREATE MATERIALIZED VIEW chat_daily_stats AS
SELECT 
    chat_id,
    DATE(timestamp) as date,
    COUNT(*) as messages,
    COUNT(DISTINCT user_id) as active_users
FROM messages
GROUP BY chat_id, DATE(timestamp);

-- Refresh periodically
REFRESH MATERIALIZED VIEW chat_daily_stats;
```

But **don't do this preemptively** - wait until you have actual performance issues.

## Implementation Plan

### Phase 1: Pure SQL Analytics (Recommended Starting Point)
1. Create SQL views for common queries
2. Build dashboard using on-demand computations
3. Test performance with real data

```sql
-- Example view for current chat status
CREATE VIEW chat_current_stats AS
SELECT 
    c.chat_id,
    c.title,
    COUNT(m.message_id) as total_messages,
    COUNT(DISTINCT m.user_id) as active_users,
    MAX(m.timestamp) as last_activity,
    COUNT(DISTINCT DATE(m.timestamp)) as active_days
FROM chats c
LEFT JOIN messages m ON c.chat_id = m.chat_id
GROUP BY c.chat_id;
```

### Phase 2: Add LLM Summaries
1. Create `llm_chat_summaries` table
2. Build Prefect flow for daily/weekly summary generation
3. Store only the LLM outputs

### Phase 3: Performance Optimization (Only If Needed)
1. Identify slow queries through monitoring
2. Create indexes on (chat_id, timestamp)
3. Consider materialized views for specific slow queries
4. Add caching layer if still needed

## Query Examples

All of these work without any additional analytics tables:

### Chat Overview
```sql
-- Get complete chat analytics
WITH chat_stats AS (
    SELECT 
        COUNT(*) as total_messages,
        COUNT(DISTINCT user_id) as active_users,
        MIN(timestamp) as first_message,
        MAX(timestamp) as last_message,
        AVG(LENGTH(message_text)) as avg_message_length
    FROM messages
    WHERE chat_id = ?
),
hourly_distribution AS (
    SELECT 
        strftime('%H', timestamp) as hour,
        COUNT(*) as count
    FROM messages
    WHERE chat_id = ?
    GROUP BY hour
    ORDER BY count DESC
    LIMIT 1
)
SELECT 
    s.*,
    h.hour as peak_hour,
    julianday(s.last_message) - julianday(s.first_message) as duration_days,
    s.total_messages / NULLIF(julianday(s.last_message) - julianday(s.first_message), 0) as avg_messages_per_day
FROM chat_stats s, hourly_distribution h;
```

### Time Series
```sql
-- Get daily trend for last 30 days
SELECT 
    DATE(timestamp) as date,
    COUNT(*) as messages,
    COUNT(DISTINCT user_id) as users,
    COUNT(CASE WHEN reply_to_id IS NOT NULL THEN 1 END) as replies
FROM messages
WHERE chat_id = ? 
    AND timestamp >= datetime('now', '-30 days')
GROUP BY DATE(timestamp)
ORDER BY date;
```

### User Analytics
```sql
-- Get user participation breakdown
WITH user_messages AS (
    SELECT 
        user_id,
        COUNT(*) as msg_count,
        NTILE(5) OVER (ORDER BY COUNT(*)) as quintile
    FROM messages
    WHERE chat_id = ?
    GROUP BY user_id
)
SELECT 
    CASE quintile
        WHEN 5 THEN 'Top 20% (Power Users)'
        WHEN 4 THEN '60-80%'
        WHEN 3 THEN '40-60%'
        WHEN 2 THEN '20-40%'
        WHEN 1 THEN 'Bottom 20%'
    END as user_segment,
    COUNT(*) as user_count,
    SUM(msg_count) as total_messages
FROM user_messages
GROUP BY quintile
ORDER BY quintile DESC;
```

## Benefits of This Approach

1. **Simplicity**: Minimal schema, maximum flexibility
2. **Accuracy**: Always showing real-time data
3. **Cost-Effective**: Store only what's necessary
4. **Maintainable**: No complex ETL for derived metrics
5. **Flexible**: Easy to add new metrics without schema changes

## When to Store Computed Metrics

Only consider storing computed metrics when:

1. **Performance**: Query takes >2 seconds and runs frequently
2. **Complexity**: Computation requires multiple passes or complex joins
3. **External Dependencies**: Results depend on external APIs
4. **Audit Requirements**: Need to track what metrics looked like at a specific time
5. **Data Loss Risk**: Source data might be deleted/modified

For most chat analytics use cases, none of these apply, so compute on-demand!

## Conclusion

The final architecture is extremely simple:

### Tables:
1. **chats** - Static metadata (no participant count)
2. **messages** - Raw message data
3. **message_analytics** - Per-message analysis (62 columns)
4. **chat_daily** - Daily snapshots with member counts and LLM summaries

### Why This Works:
- **Minimal redundancy** - Only store what changes (member count) or can't be recomputed (LLM summaries)
- **Maximum flexibility** - Compute everything else on-demand with SQL
- **Simple to maintain** - Just one daily job to populate chat_daily
- **Cost effective** - Store only essential data

### Daily Job (Prefect Flow):
1. Count members for the day → `total_member_count`
2. Count messages for the day → `total_message_count` 
3. Count active users → `total_active_member_count`
4. Generate LLM summary → `daily_summary`
5. Insert into `chat_daily`

Everything else is computed on-demand when needed.