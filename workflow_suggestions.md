# Telegram ETL Workflow Suggestions

## Overview
Based on your requirements and philosophy, here are 5 production-grade workflow approaches using Prefect for orchestrating the Telegram group data ETL pipeline. Each approach balances simplicity, scalability, and maintainability while avoiding over-engineering.

## Critical Consideration: Historical vs New Data Processing

### Historical Data (Initial Backfill)
Historical data extraction is a **one-time bulk operation** that needs special handling:

#### Challenges
- **Volume**: Groups can have millions of historical messages
- **API Limits**: Telegram has strict rate limits (e.g., 30 requests/second)
- **Time**: Full extraction can take hours or days
- **Memory**: Loading all messages at once can cause OOM errors

#### Strategy
```python
@flow(name="historical_backfill")
async def backfill_historical_data(group_id: int, batch_size: int = 1000):
    """One-time historical data extraction using tgdata with built-in batch processing"""
    
    # Initialize tgdata
    tg = TgData("config.ini")
    
    # Get total message count
    total_messages = await tg.get_message_count(group_id=group_id)
    logger.info(f"Total messages to process: {total_messages:,}")
    
    messages_processed = 0
    last_message_id = 0
    
    # Define batch callback for processing chunks
    async def process_batch(batch_df, batch_info):
        nonlocal messages_processed, last_message_id
        
        # Store batch to BigQuery
        await store_to_bigquery(batch_df, group_id)
        
        # Update tracking
        messages_processed = batch_info['total_processed']
        last_message_id = int(batch_df['MessageId'].max())
        
        # Save checkpoint for recovery
        save_checkpoint(group_id, last_message_id, messages_processed)
        
        # Log progress
        percent = (messages_processed / total_messages * 100) if total_messages > 0 else 0
        logger.info(f"Batch {batch_info['batch_num']}: {messages_processed:,}/{total_messages:,} ({percent:.1f}%)")
    
    # Use tgdata's built-in batch processing with rate limiting
    all_messages = await tg.get_messages(
        group_id=group_id,
        after_id=0,  # Start from beginning
        limit=total_messages,  # Get all messages
        batch_size=batch_size,  # Process in chunks
        batch_callback=process_batch,  # Process each batch
        batch_delay=1.0,  # Built-in rate limit delay between batches
        rate_limit_strategy='exponential',  # Handle rate limits automatically
        with_progress=True  # Show progress
    )
    
    await tg.close()
    return len(all_messages)
```

### New Data (Continuous Processing)
New data extraction is a **continuous incremental process** using message ID tracking:

#### Key Concepts
- **Last Message ID**: Query the highest message_id directly from BigQuery (no separate state needed)
- **Polling Strategy**: Check for new messages at regular intervals
- **Delta Processing**: Only process messages newer than the highest stored ID

#### Implementation Patterns

**Pattern 1: Simple Polling with Last Message ID**
```python
@flow(name="incremental_message_sync")
async def sync_new_messages(group_id: int):
    """Continuous new message extraction using tgdata with built-in features"""
    
    # Initialize tgdata
    tg = TgData("config.ini")
    
    # Get last processed message ID directly from BigQuery
    query = f"""
    SELECT MAX(message_id) as last_id 
    FROM `project.dataset.messages` 
    WHERE group_id = {group_id}
    """
    last_message_id = run_bigquery_query(query).iloc[0]['last_id'] or 0
    
    # Use tgdata with batch processing for new messages
    new_messages_df = await tg.get_messages(
        group_id=group_id,
        after_id=last_message_id,  # Only messages after this ID
        limit=5000,  # Get up to 5000 new messages
        batch_size=500,  # Process in smaller chunks
        batch_delay=0.5,  # Built-in delay between batches
        rate_limit_strategy='wait',  # Wait on rate limits
        with_progress=False  # No progress bar for incremental
    )
    
    if not new_messages_df.empty:
        # Process all new messages at once (already batched internally)
        await process_messages_to_bigquery(new_messages_df)
        logger.info(f"Processed {len(new_messages_df)} new messages")
    
    await tg.close()
    return len(new_messages_df)
```
<!--  this one is not the best practice. 
**Pattern 2: Continuous Polling with tgdata**
```python
@flow(name="continuous_polling")
async def poll_for_new_messages(group_id: int, interval_seconds: int = 60):
    """Continuous polling using tgdata's poll_for_messages"""
    
    tg = TgData("config.ini")
    
    # Get last processed message ID from BigQuery
    client = bigquery.Client()
    result = client.query(f"""
        SELECT IFNULL(MAX(message_id), 0) as last_id 
        FROM `project.dataset.messages` 
        WHERE group_id = {group_id}
    """).result()
    last_message_id = list(result)[0].last_id
    
    # Define callback for new messages
    async def process_new_messages(messages_df):
        if not messages_df.empty:
            # Store to BigQuery (this automatically updates our max ID)
            process_messages_to_bigquery(messages_df)
            logger.info(f"Processed {len(messages_df)} new messages")
    
    # Poll for messages (runs for max_iterations)
    await tg.poll_for_messages(
        group_id=group_id,
        interval=interval_seconds,
        after_id=last_message_id,
        callback=process_new_messages,
        max_iterations=60  # Poll for 1 hour if interval=60s
    )
    
    await tg.close()
``` -->

**Pattern 3: Real-time Event Handler**
```python
@flow(name="realtime_message_handler")
async def setup_realtime_handler(group_id: int):
    """Real-time message handling using tgdata's event system"""
    
    tg = TgData("config.ini")
    
    # Register event handler for new messages
    @tg.on_new_message(group_id=group_id)
    async def handle_new_message(event):
        # Convert event to DataFrame format
        message_data = {
            'MessageId': event.message.id,
            'SenderId': event.sender_id,
            'Message': event.message.text,
            'Date': event.message.date
        }
        
        # Process immediately
        process_single_message_to_bigquery(message_data)
        update_last_processed_message_id(group_id, event.message.id)
        
        logger.info(f"Real-time: processed message {event.message.id}")
    
    # Run event loop
    await tg.run_with_event_loop()
```

### State Management Simplified

#### No Separate State Table Needed!
Instead of maintaining a separate state table, we can:
1. Query the max message_id directly from the messages table
2. Use BigQuery's built-in metadata for tracking

```sql
-- Just query the messages table directly
SELECT MAX(message_id) as last_processed_id
FROM `project.dataset.messages`
WHERE group_id = @group_id;

-- Optional: Processing history for debugging/monitoring
CREATE TABLE etl_runs (
    run_id STRING PRIMARY KEY,
    group_id INT64 NOT NULL,
    start_message_id INT64,
    end_message_id INT64,
    messages_processed INT64,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    status STRING,
    error_message STRING
);
```

#### Benefits of This Approach
- **Single Source of Truth**: The messages table IS the state
- **No Sync Issues**: Can't have mismatch between state and actual data
- **Simpler Recovery**: Just query max ID and continue
- **Less Infrastructure**: One less table to manage

#### Handling Edge Cases
```python
@task(retries=3, retry_delay_seconds=60)
async def verify_message_completeness(group_id: int, lookback_hours: int = 24):
    """Verify we haven't missed any messages in recent period"""
    
    tg = TgData("config.ini")
    
    # Get messages from last N hours to ensure completeness
    # Message IDs are GLOBAL in Telegram, not per-group sequential
    from datetime import datetime, timedelta
    since_date = datetime.now() - timedelta(hours=lookback_hours)
    
    # Fetch messages by date range
    recent_messages = await tg.get_messages(
        group_id=group_id,
        start_date=since_date,
        batch_size=1000,
        batch_delay=0.5,
        rate_limit_strategy='wait'
    )
    
    if not recent_messages.empty:
        # Get existing message IDs from database for this period
        existing_ids = get_existing_message_ids(group_id, since_date)
        telegram_ids = set(recent_messages['MessageId'].values)
        
        # Find messages we don't have
        missing_ids = telegram_ids - existing_ids
        
        if missing_ids:
            # Store only the missing messages
            missing_messages = recent_messages[recent_messages['MessageId'].isin(missing_ids)]
            await process_messages_to_bigquery(missing_messages)
            logger.info(f"Found and stored {len(missing_ids)} missing messages")
    
    await tg.close()

@task
async def ensure_data_freshness(group_id: int):
    """Ensure we have the latest messages"""
    
    tg = TgData("config.ini")
    
    # Get the latest message from Telegram
    latest_messages = await tg.get_messages(
        group_id=group_id,
        limit=1  # Just get the latest
    )
    
    if not latest_messages.empty:
        latest_id = int(latest_messages['MessageId'].max())
        stored_latest_id = get_last_processed_message_id(group_id)
        
        if latest_id > stored_latest_id:
            # We're behind, catch up
            logger.warning(f"Behind by {latest_id - stored_latest_id} messages")
            await sync_new_messages(group_id)
    
    await tg.close()
```

### Hybrid Approach: Initial Load + Continuous Sync

#### Recommended Implementation
```python
@flow(name="telegram_etl_master")
async def master_etl_flow(group_id: int, mode: str = "auto"):
    """Master flow that handles both historical and incremental"""
    
    if mode == "auto":
        # Check if this is first run
        last_id = get_last_processed_message_id(group_id)
        mode = "historical" if last_id is None else "incremental"
    
    if mode == "historical":
        # Run historical backfill
        with tags("historical", "backfill"):
            messages_processed = await backfill_historical_data(group_id)
            logger.info(f"Historical backfill complete: {messages_processed:,} messages")
            
        # Set up for incremental
        initialize_incremental_state(group_id)
        
    elif mode == "incremental":
        # Run incremental sync
        with tags("incremental", "sync"):
            new_messages = await sync_new_messages(group_id)
            logger.info(f"Incremental sync: {new_messages} new messages")
    
    elif mode == "hybrid":
        # Run both with careful coordination
        tg = TgData("config.ini")
        
        # Get current latest message as boundary
        current_latest = await tg.get_messages(group_id=group_id, limit=1)
        boundary_id = int(current_latest['MessageId'].max()) if not current_latest.empty else 0
        
        # Start historical backfill up to boundary
        historical_task = backfill_up_to_boundary.submit(
            group_id, 
            boundary_id
        )
        
        # Start incremental from boundary
        incremental_task = poll_for_new_messages.submit(
            group_id,
            interval_seconds=60
        )
        
        # Wait for both
        await asyncio.gather(historical_task, incremental_task)
        await tg.close()
```

### Polling Strategies Comparison

| Strategy | Use Case | Pros | Cons |
|----------|----------|------|------|
| **Fixed Interval** | Low-activity groups | Simple, predictable | May miss bursts, inefficient |
| **Adaptive Interval** | Variable activity | Efficient, responsive | Complex logic |
| **Real-time Events** | Critical updates | Instant updates | Requires persistent connection |
| **Hybrid Polling** | Production systems | Balanced approach | More infrastructure |

### Advanced Pattern: Memory-Efficient Historical Backfill

```python
@flow(name="memory_efficient_backfill")
async def backfill_with_streaming(group_id: int):
    """Memory-efficient backfill using tgdata's batch callback"""
    
    tg = TgData("config.ini")
    total_messages = await tg.get_message_count(group_id=group_id)
    
    # Stream directly to BigQuery without holding all in memory
    async def stream_to_bigquery(batch_df, batch_info):
        # Minimal processing - just ensure proper data types
        batch_df['Date'] = pd.to_datetime(batch_df['Date'])
        batch_df['group_id'] = group_id  # Add group_id column
        
        # Append RAW data to BigQuery table (ELT pattern, not ETL)
        batch_df.to_gbq(
            destination_table=f'telegram.raw_messages',
            project_id='your-project',
            if_exists='append',
            progress_bar=False
        )
        
        logger.info(f"Streamed batch {batch_info['batch_num']} to BigQuery (raw)")
        
        # Track unique users for later processing
        unique_users = batch_df['SenderId'].unique()
        await store_users_for_processing(unique_users)
    
    # This will process ALL messages but never hold more than batch_size in memory
    await tg.get_messages(
        group_id=group_id,
        after_id=0,
        limit=None,  # No limit - get all
        batch_size=2000,  # Process 2000 at a time
        batch_callback=stream_to_bigquery,
        batch_delay=1.5,  # Rate limiting
        rate_limit_strategy='exponential',
        with_progress=True
    )
    
    await tg.close()
```

### Separate Analysis Pipeline (Post-Load)

```python
@flow(name="message_analysis_pipeline")
async def analyze_messages_in_bigquery(group_id: int, analysis_date: datetime = None):
    """Run analysis AFTER data is already in BigQuery"""
    
    if not analysis_date:
        analysis_date = datetime.now().date()
    
    # Step 1: Entity extraction in BigQuery
    await run_bigquery_sql(f"""
        UPDATE `project.dataset.raw_messages`
        SET 
            word_count = ARRAY_LENGTH(SPLIT(message_text, ' ')),
            character_count = LENGTH(message_text),
            hashtags = REGEXP_EXTRACT_ALL(message_text, r'#\\w+'),
            mentioned_users = REGEXP_EXTRACT_ALL(message_text, r'@\\w+'),
            urls = REGEXP_EXTRACT_ALL(message_text, r'https?://[^\\s]+')
        WHERE group_id = {group_id}
        AND DATE(date) = '{analysis_date}'
        AND word_count IS NULL
    """)
    
    # Step 2: User statistics aggregation
    await run_bigquery_sql(f"""
        MERGE `project.dataset.users` t
        USING (
            SELECT 
                user_id,
                COUNT(*) as daily_messages,
                SUM(word_count) as daily_words,
                AVG(character_count) as avg_msg_length
            FROM `project.dataset.raw_messages`
            WHERE group_id = {group_id}
            AND DATE(date) = '{analysis_date}'
            GROUP BY user_id
        ) s
        ON t.user_id = s.user_id
        WHEN MATCHED THEN UPDATE SET
            total_messages = t.total_messages + s.daily_messages,
            total_words = t.total_words + s.daily_words,
            avg_message_length = (t.avg_message_length + s.avg_msg_length) / 2,
            last_seen_at = CURRENT_TIMESTAMP()
    """)
    
    # Step 3: Batch sentiment analysis (using external service)
    messages_for_sentiment = await get_unanalyzed_messages(group_id, analysis_date)
    if not messages_for_sentiment.empty:
        # Process in chunks to avoid memory issues
        for chunk in np.array_split(messages_for_sentiment, 10):
            sentiments = await analyze_sentiment_batch(chunk['message_text'].tolist())
            await update_sentiment_scores(chunk['message_id'].tolist(), sentiments)
    
    # Step 4: LLM-based daily summary (expensive, run last)
    daily_summary = await generate_daily_summary_with_llm(group_id, analysis_date)
    await store_daily_summary(group_id, analysis_date, daily_summary)
    
    logger.info(f"Analysis complete for {group_id} on {analysis_date}")

@flow(name="user_profiling_pipeline")
async def profile_users_post_load(group_id: int):
    """Generate user insights AFTER messages are loaded"""
    
    # Run expensive user profiling in BigQuery
    user_profiles = await run_bigquery_sql(f"""
        WITH user_stats AS (
            SELECT 
                user_id,
                COUNT(*) as total_messages,
                COUNT(DISTINCT DATE(date)) as active_days,
                MIN(date) as first_message,
                MAX(date) as last_message,
                AVG(character_count) as avg_message_length,
                ARRAY_AGG(DISTINCT hashtags IGNORE NULLS) as used_hashtags,
                -- Activity pattern
                CASE 
                    WHEN EXTRACT(HOUR FROM date) BETWEEN 6 AND 12 THEN 'morning'
                    WHEN EXTRACT(HOUR FROM date) BETWEEN 12 AND 18 THEN 'afternoon'
                    WHEN EXTRACT(HOUR FROM date) BETWEEN 18 AND 24 THEN 'evening'
                    ELSE 'night'
                END as time_preference
            FROM `project.dataset.raw_messages`
            WHERE group_id = {group_id}
            GROUP BY user_id
        )
        SELECT * FROM user_stats
        WHERE total_messages > 10  -- Only profile active users
    """)
    
    # Generate LLM insights for top users
    top_users = user_profiles.nlargest(100, 'total_messages')
    for _, user in top_users.iterrows():
        user_messages = await get_user_messages(user['user_id'], limit=100)
        llm_insights = await generate_user_insights_with_llm(user_messages)
        await store_user_insights(user['user_id'], llm_insights)
```

### Prefect Deployment Configuration

```python
# deployment.py
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import IntervalSchedule
from datetime import timedelta

# Historical backfill - one time
historical_deployment = Deployment.build_from_flow(
    flow=backfill_historical_data,
    name="historical-backfill",
    work_queue_name="telegram-etl",
    tags=["historical", "one-time"],
    parameters={"batch_size": 1000}
)

# Incremental sync - continuous
incremental_deployment = Deployment.build_from_flow(
    flow=sync_new_messages,
    name="incremental-sync",
    work_queue_name="telegram-etl",
    schedule=IntervalSchedule(interval=timedelta(minutes=5)),
    tags=["incremental", "continuous"]
)

# Memory-efficient backfill for large groups
streaming_deployment = Deployment.build_from_flow(
    flow=backfill_with_streaming,
    name="streaming-backfill",
    work_queue_name="telegram-etl-heavy",
    tags=["historical", "streaming", "memory-efficient"]
)
```

---

## Workflow 1: Simple Sequential Pipeline (MVP)
**Best for: Quick proof-of-concept and initial deployment**

### Architecture
```
[Prefect Schedule] → Extract → Transform → Load → Basic Analytics
```

### Components
- **Extract Flow**: Single Prefect flow that uses tgdata to fetch messages
- **Transform Task**: In-memory pandas processing for data cleanup
- **Load Task**: Batch insert to BigQuery using pandas-gbq
- **Analytics Task**: SQL queries in BigQuery + Python aggregations

### Implementation Details
```python
@flow(name="telegram_etl_simple")
def telegram_etl():
    messages = extract_messages(group_id, since_date)
    transformed = transform_messages(messages)
    load_to_bigquery(transformed)
    generate_basic_insights()
```

### Pros
- Quick to implement and deploy
- Minimal infrastructure requirements
- Easy to debug and maintain
- Good for processing < 100k messages daily

### Cons
- Limited scalability
- No real-time capabilities
- Sequential processing bottleneck

---

## Workflow 2: Incremental Processing with State Management
**Best for: Production deployment with efficient resource usage**

### Architecture
```
[State Store] ← → [Prefect Flow]
                    ├── Check Last Run
                    ├── Extract Delta
                    ├── Transform & Validate
                    ├── Load with Dedup
                    └── Update State
```

### Components
- **State Management**: Track last processed message_id in database
- **Delta Extraction**: Only fetch new messages since last run
- **Validation Layer**: Data quality checks before loading
- **Deduplication**: Ensure no duplicate messages in BigQuery
- **User Profile Sync**: Separate flow for user data updates

### Implementation Details
```python
@flow(name="incremental_telegram_etl")
def incremental_etl():
    last_message_id = get_last_processed_id()
    new_messages = extract_new_messages(since_id=last_message_id)
    
    if new_messages:
        validated = validate_and_transform(new_messages)
        load_with_merge(validated)  # MERGE instead of INSERT
        update_last_processed_id(max(msg.id for msg in new_messages))
        
    # Run user updates separately
    update_user_profiles()
```

### Pros
- Efficient resource usage
- Handles failures gracefully
- Supports frequent runs (every 5-15 minutes)
- Maintains data consistency

### Cons
- Requires state management infrastructure
- More complex error recovery
- Need to handle message edits/deletions

---

## Workflow 3: Parallel Processing with Task Mapping
**Best for: High-volume processing with optimized performance**

### Architecture
```
[Prefect Orchestrator]
    ├── Extract (Batched)
    ├── Transform (Parallel)
    │   ├── Text Processing
    │   ├── Entity Extraction
    │   └── User Attribution
    ├── Load (Partitioned)
    └── Analytics (Async)
        ├── SQL Analytics
        └── LLM Processing
```

### Components
- **Batch Extraction**: Fetch messages in configurable chunks
- **Parallel Transform**: Use Prefect's task mapping for parallel processing
- **Partitioned Loading**: Load to date-partitioned BigQuery tables
- **Async Analytics**: Non-blocking LLM analysis tasks

### Implementation Details
```python
@flow(name="parallel_telegram_etl")
def parallel_etl():
    # Extract in batches
    message_batches = extract_in_batches(batch_size=1000)
    
    # Process batches in parallel
    transformed_batches = transform_batch.map(message_batches)
    
    # Load in parallel to partitioned tables
    load_results = load_to_partition.map(
        transformed_batches,
        partition_date=unmapped(date.today())
    )
    
    # Trigger async analytics
    trigger_llm_analysis.submit(wait_for=[load_results])
```

### Pros
- Handles millions of messages efficiently
- Optimal resource utilization
- Fault-tolerant with batch isolation
- Scalable horizontally

### Cons
- More complex monitoring
- Requires careful resource planning
- Higher infrastructure costs

---

## Workflow 4: Event-Driven with Real-Time Processing
**Best for: Near real-time insights and alerts**

### Architecture
```
[Telegram Events] → [Message Queue] → [Prefect Worker Pool]
                                         ├── Process Message
                                         ├── Update User Stats
                                         ├── Trigger Analytics
                                         └── Send Alerts
```

### Components
- **Event Listener**: tgdata's real-time event handler
- **Message Buffer**: Redis or Cloud Pub/Sub for buffering
- **Worker Pool**: Multiple Prefect agents processing events
- **Stream Processing**: Incremental stats updates
- **Alert System**: Immediate notifications for important events

### Implementation Details
```python
@flow(name="realtime_processor")
def process_message_event(message_event):
    # Quick processing path
    message = parse_event(message_event)
    
    # Immediate storage
    store_to_staging(message)
    
    # Update running statistics
    update_user_stats(message.user_id)
    
    # Check for alerts
    if check_alert_conditions(message):
        send_notification(message)
    
    # Queue for batch processing
    queue_for_batch_etl(message)

# Separate batch consolidation flow
@flow(name="batch_consolidator")
def consolidate_staging():
    messages = read_staging_table()
    process_and_load_to_warehouse(messages)
    clear_staging_table()
```

### Pros
- Real-time insights and alerts
- Low latency processing
- Supports both streaming and batch
- Good for active monitoring

### Cons
- Complex architecture
- Requires message queue infrastructure
- Higher operational overhead
- Potential for message loss if not properly configured

---

## Workflow 5: Hybrid Lakehouse Pattern
**Best for: Maximum flexibility and future-proofing**

### Architecture
```
[Raw Data Lake]
    ↓
[Bronze Layer] (Raw Messages)
    ↓
[Silver Layer] (Cleaned, Structured)
    ↓
[Gold Layer] (Analytics-Ready)
    ↓
[Insights & ML]
```

### Components
- **Raw Storage**: Store raw Telegram data in GCS/S3
- **Bronze Tables**: Direct copy of raw data in BigQuery
- **Silver Tables**: Cleaned, deduplicated, user-enriched data
- **Gold Tables**: Pre-aggregated metrics and features
- **ML Pipeline**: Feature engineering for user profiling

### Implementation Details
```python
@flow(name="lakehouse_telegram_etl")
def lakehouse_pipeline():
    # Bronze: Raw extraction
    raw_messages = extract_to_gcs()
    load_bronze_tables(raw_messages)
    
    # Silver: Transform and enrich
    with dbt_transform():
        create_silver_tables()
        enrich_user_data()
        validate_data_quality()
    
    # Gold: Analytics layer
    create_gold_aggregates()
    calculate_user_segments()
    
    # ML/Analytics
    run_llm_analysis()
    update_user_insights()
```

### Pros
- Complete data lineage
- Supports reprocessing
- Enables advanced analytics and ML
- Schema evolution friendly
- Best practices for data engineering

### Cons
- Higher initial setup time
- Requires more storage
- Need expertise in modern data stack
- May be overkill for single group

---

## Recommendation

For your immediate needs (job assessment), I recommend **Workflow 2 (Incremental Processing)** with elements of **Workflow 3 (Parallel Processing)** for the following reasons:

1. **Production-ready**: Shows understanding of real-world requirements
2. **Efficient**: Demonstrates resource optimization
3. **Scalable**: Can grow from single to multiple groups
4. **Prefect-native**: Leverages Prefect's best features
5. **Balanced complexity**: Not over-engineered but sophisticated enough

### Suggested Implementation Path
1. Start with SQLAlchemy models (users, messages, groups, user_insights)
2. Implement incremental extraction using tgdata
3. Add data validation and transformation tasks
4. Set up BigQuery loading with deduplication
5. Create basic analytics queries
6. Add LLM-based analysis as async tasks
7. Implement Prefect deployments for scheduling
8. Add monitoring and alerting

### Key Technologies
- **Orchestration**: Prefect (flows, tasks, deployments)
- **Storage**: BigQuery (partitioned tables)
- **Processing**: Pandas for transforms, SQL for analytics
- **LLM**: OpenAI/Anthropic API for contextual analysis
- **Monitoring**: Prefect UI + custom dashboards
- **State**: PostgreSQL or BigQuery for tracking