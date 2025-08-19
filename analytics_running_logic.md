# Analytics Running Logic

## Overview
Analytics pipeline using Prefect for orchestration and SQL queries for computation. This approach combines the best of both worlds: Prefect's workflow management with SQL's analytical power.

## Architecture

```
┌─────────────────┐
│  Prefect Flow   │ ← Orchestration Layer
├─────────────────┤
│   SQLAlchemy    │ ← Execution Layer  
├─────────────────┤
│  SQL Queries    │ ← Analytics Layer
├─────────────────┤
│    Database     │ ← Storage Layer
└─────────────────┘
```

## Implementation Structure

### 1. Current Implementation Structure
```
convoetl/
├── analytics/
│   ├── __init__.py
│   ├── sqlite/                   # SQLite-specific implementation
│   │   ├── __init__.py
│   │   ├── queries.py            # SQLite SQL queries
│   │   ├── executor.py           # SQLite executor
│   │   ├── results_saver.py      # Save results to database
│   │   └── analytics_tables.sql  # Table definitions
│   ├── bigquery/                  # BigQuery-specific (Version 2)
│   │   ├── __init__.py
│   │   ├── queries.py            # BigQuery SQL queries (placeholder)
│   │   └── executor.py           # BigQuery executor (placeholder)
│   └── flows/
│       ├── __init__.py
│       ├── generic_analytics_flow.py  # Database-agnostic Prefect flows
│       └── advanced_analytics_flow.py # LLM analytics (future)
```

This structure separates database-specific implementations because SQLite and BigQuery have different:
- Date/time functions (SQLite: `strftime()` vs BigQuery: `EXTRACT()`)
- String functions and syntax
- Window function capabilities
- Data types and casting

### 2. SQL Query Storage Pattern (Current Implementation)

```python
# convoetl/analytics/sqlite/queries.py
from dataclasses import dataclass
from typing import Dict, Any, Optional

@dataclass
class AnalyticsQuery:
    """Container for analytics queries"""
    name: str
    description: str
    sql: str  # SQLite-specific SQL
    parameters: Optional[Dict[str, Any]] = None
    
MESSAGE_QUERIES = {
    "individual_messages": AnalyticsQuery(
        name="Individual Message Analysis",
        description="Analyzes properties of each individual message",
        sql="""
            SELECT 
                message_id,
                user_id,
                message_text,
                LENGTH(message_text) as char_count,
                LENGTH(message_text) - LENGTH(REPLACE(message_text, ' ', '')) + 1 as word_count,
                CASE 
                    WHEN message_text LIKE '%?%' THEN 'question'
                    ELSE 'statement'
                END as message_type,
                timestamp
            FROM messages
            WHERE chat_id = :chat_id
            ORDER BY timestamp DESC
            LIMIT :limit
        """,
        parameters={"limit": 100}
    ),
    
    "hourly_distribution": AnalyticsQuery(
        name="Hourly Message Distribution",
        description="Messages per hour of day",
        sql="""
            SELECT 
                CAST(strftime('%H', timestamp) AS INTEGER) as hour,
                COUNT(*) as message_count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
            FROM messages
            WHERE chat_id = :chat_id
            GROUP BY hour
            ORDER BY hour
        """
    )
}

# Future BigQuery implementation (convoetl/analytics/bigquery/queries.py)
# Will use BigQuery-specific syntax:
# - EXTRACT(HOUR FROM timestamp) instead of strftime('%H', timestamp)
# - ARRAY_LENGTH(SPLIT(message_text, ' ')) instead of LENGTH-based word count
# - @chat_id parameter syntax instead of :chat_id
```

### 3. Query Executor Pattern (Current Implementation)

```python
# convoetl/analytics/sqlite/executor.py
import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class SQLiteAnalyticsExecutor:
    """Executes analytics queries on SQLite"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.engine = create_engine(f'sqlite:///{db_path}')
    
    def execute_query(
        self, 
        query: str, 
        parameters: Dict[str, Any] = None,
        return_type: str = 'dataframe'
    ) -> pd.DataFrame:
        """
        Execute SQL query and return results
        
        Args:
            query: SQL query string
            parameters: Query parameters
            return_type: 'dataframe', 'dict', or 'raw'
            
        Returns:
            Query results as specified type
        """
        try:
            with self.engine.connect() as conn:
                result = pd.read_sql(
                    sql=text(query),
                    con=conn,
                    params=parameters or {}
                )
                
                if return_type == 'dict':
                    return result.to_dict(orient='records')
                elif return_type == 'raw':
                    return result.values.tolist()
                else:
                    return result
                    
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    def execute_multiple(
        self,
        queries: Dict[str, tuple[str, Dict]],
        return_type: str = 'dataframe'
    ) -> Dict[str, Any]:
        """Execute multiple queries and return results"""
        results = {}
        
        for name, (query, params) in queries.items():
            results[name] = self.execute_query(query, params, return_type)
            
        return results
```

### 4. Prefect Flow Implementation (Current)

```python
# convoetl/analytics/flows/generic_analytics_flow.py
from prefect import flow, task, get_run_logger
from typing import Dict, Any, Optional
import pandas as pd

from ..sqlite.queries import MESSAGE_QUERIES, USER_QUERIES, CHAT_QUERIES
from ..sqlite.executor import SQLiteAnalyticsExecutor
from ..sqlite.results_saver import AnalyticsResultsSaver

@task(
    name="run_message_analytics",
    description="Run message-level analytics",
    retries=2,
    retry_delay_seconds=30
)
async def run_message_analytics(
    executor: SQLiteAnalyticsExecutor,
    chat_id: str,
    queries_to_run: list = None
) -> Dict[str, pd.DataFrame]:
    """Execute message analytics queries"""
    logger = get_run_logger()
    
    results = {}
    queries = queries_to_run or MESSAGE_QUERIES.keys()
    
    for query_name in queries:
        if query_name in MESSAGE_QUERIES:
            query_obj = MESSAGE_QUERIES[query_name]
            logger.info(f"Running: {query_obj.name}")
            
            params = {"chat_id": chat_id}
            if query_obj.parameters:
                params.update(query_obj.parameters)
            
            results[query_name] = executor.execute_query(
                query_obj.sql,  # SQLite-specific SQL
                params
            )
            logger.info(f"✓ Completed {query_name}: {len(results[query_name])} rows")
    
    return results

@task(
    name="run_user_analytics",
    description="Run user-level analytics",
    retries=2
)
async def run_user_analytics(
    executor: SQLiteAnalyticsExecutor,
    chat_id: str,
    message_results: Dict[str, pd.DataFrame] = None
) -> Dict[str, pd.DataFrame]:
    """Execute user analytics queries"""
    logger = get_run_logger()
    
    results = {}
    
    for query_name, query_obj in USER_QUERIES.items():
        logger.info(f"Running: {query_obj.name}")
        
        params = {"chat_id": chat_id}
        results[query_name] = executor.execute_query(
            query_obj.sql_sqlite,
            params
        )
        
    return results

@task(
    name="run_chat_analytics",
    description="Run chat-level analytics"
)
async def run_chat_analytics(
    executor: SQLiteAnalyticsExecutor,
    chat_id: str,
    user_results: Dict[str, pd.DataFrame] = None
) -> Dict[str, pd.DataFrame]:
    """Execute chat analytics queries"""
    logger = get_run_logger()
    
    results = {}
    
    for query_name, query_obj in CHAT_QUERIES.items():
        logger.info(f"Running: {query_obj.name}")
        
        params = {"chat_id": chat_id}
        results[query_name] = executor.execute_query(
            query_obj.sql_sqlite,
            params
        )
        
    return results

@task(
    name="save_analytics_results",
    description="Save analytics results to storage"
)
async def save_analytics_results(
    results: Dict[str, Dict[str, pd.DataFrame]],
    output_format: str = "parquet",
    output_path: str = "data/analytics"
) -> Dict[str, str]:
    """Save analytics results to files"""
    logger = get_run_logger()
    
    import os
    from datetime import datetime
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_path = f"{output_path}/{timestamp}"
    os.makedirs(base_path, exist_ok=True)
    
    saved_files = {}
    
    for category, category_results in results.items():
        for query_name, df in category_results.items():
            if output_format == "parquet":
                file_path = f"{base_path}/{category}_{query_name}.parquet"
                df.to_parquet(file_path)
            elif output_format == "csv":
                file_path = f"{base_path}/{category}_{query_name}.csv"
                df.to_csv(file_path, index=False)
            else:
                file_path = f"{base_path}/{category}_{query_name}.json"
                df.to_json(file_path, orient='records')
            
            saved_files[f"{category}.{query_name}"] = file_path
            logger.info(f"Saved {query_name} to {file_path}")
    
    return saved_files

@flow(
    name="generic_analytics_pipeline",
    description="Run complete generic analytics pipeline"
)
async def generic_analytics_flow(
    chat_id: str,
    db_path: str = "data/telegram.db",
    output_format: str = "parquet",
    save_results: bool = True
) -> Dict[str, Any]:
    """
    Main analytics flow - runs analytics in sequence
    
    Args:
        chat_id: Chat to analyze
        db_path: Path to database
        output_format: Output format (parquet, csv, json)
        save_results: Whether to save results to files
        
    Returns:
        Dictionary with all analytics results
    """
    logger = get_run_logger()
    logger.info(f"Starting analytics pipeline for chat {chat_id}")
    
    # Initialize executor
    executor = SQLiteAnalyticsExecutor(db_path)
    
    # Sequential execution: Messages → Users → Chat
    logger.info("Phase 1: Message Analytics")
    message_results = await run_message_analytics(executor, chat_id)
    
    logger.info("Phase 2: User Analytics")
    user_results = await run_user_analytics(executor, chat_id, message_results)
    
    logger.info("Phase 3: Chat Analytics")
    chat_results = await run_chat_analytics(executor, chat_id, user_results)
    
    # Combine results
    all_results = {
        "messages": message_results,
        "users": user_results,
        "chat": chat_results
    }
    
    # Save results if requested
    if save_results:
        saved_files = await save_analytics_results(
            all_results,
            output_format=output_format
        )
        logger.info(f"Analytics saved to {len(saved_files)} files")
    
    logger.info("Analytics pipeline completed successfully")
    return all_results
```

### 5. Usage Examples

```python
# Run analytics as standalone
import asyncio
from convoetl.analytics.flows.generic_analytics_flow import generic_analytics_flow

async def run_analytics():
    results = await generic_analytics_flow(
        chat_id="1670178185",
        db_path="data/telegram.db",
        output_format="parquet",
        save_results=True
    )
    
    # Access specific results
    hourly_dist = results["messages"]["hourly_distribution"]
    print(f"Peak hour: {hourly_dist.iloc[0]['hour']}")
    
asyncio.run(run_analytics())
```

### 6. Scheduling with Prefect

```python
# Schedule analytics to run daily
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

deployment = Deployment.build_from_flow(
    flow=generic_analytics_flow,
    name="daily-analytics",
    parameters={
        "chat_id": "1670178185",
        "db_path": "data/telegram.db",
        "save_results": True
    },
    schedule=CronSchedule(cron="0 2 * * *"),  # Run at 2 AM daily
)

deployment.apply()
```

### 7. Integration with Main Pipeline

```python
# convoetl/core/pipeline.py extension
async def run_with_analytics(
    self,
    source_id: str,
    run_analytics: bool = True
) -> Dict[str, Any]:
    """Run extraction and analytics together"""
    
    # Run extraction
    extraction_result = await self.run(source_id)
    
    if run_analytics:
        # Run analytics after extraction
        from ..analytics.flows.generic_analytics_flow import generic_analytics_flow
        
        analytics_result = await generic_analytics_flow(
            chat_id=source_id,
            db_path=self.storage_config["db_path"]
        )
        
        return {
            "extraction": extraction_result,
            "analytics": analytics_result
        }
    
    return extraction_result
```

## Benefits of This Approach

1. **Separation of Concerns**
   - SQL queries are isolated and reusable
   - Execution logic is separate from queries
   - Orchestration is handled by Prefect

2. **Database Portability**
   - Same flow works with SQLite and BigQuery
   - Easy to add new database backends
   - Query differences are encapsulated

3. **Monitoring & Observability**
   - Prefect UI shows flow execution
   - Task-level retries and error handling
   - Performance metrics for each query

4. **Scalability**
   - Can parallelize independent queries
   - Easy to add new analytics queries
   - Results caching with Prefect

5. **Testability**
   - Queries can be tested independently
   - Mocked executors for unit tests
   - Flow testing with Prefect

## Configuration

```yaml
# analytics_config.yaml
analytics:
  default_db: sqlite
  sqlite:
    path: data/telegram.db
  bigquery:
    project: your-project
    dataset: telegram_analytics
  
  output:
    format: parquet
    path: data/analytics
    
  scheduling:
    enabled: true
    cron: "0 2 * * *"
    
  queries:
    message_limit: 10000
    user_limit: 1000
```

## Error Handling

```python
@task(
    retries=3,
    retry_delay_seconds=[30, 60, 120],
    on_failure=[send_alert]
)
async def robust_query_execution(executor, query, params):
    """Query execution with robust error handling"""
    try:
        return executor.execute_query(query, params)
    except sqlite3.OperationalError as e:
        if "database is locked" in str(e):
            # Wait and retry
            await asyncio.sleep(5)
            raise
        else:
            # Log and fail
            logger.error(f"Database error: {e}")
            raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        # Could save partial results or send notification
        raise
```

## Performance Optimization

1. **Query Optimization**
   - Create indexes before running analytics
   - Use EXPLAIN QUERY PLAN to optimize
   - Batch queries when possible

2. **Caching Results**
   ```python
   from prefect.tasks import task_input_hash
   from datetime import timedelta
   
   @task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
   async def cached_analytics(executor, query, params):
       return executor.execute_query(query, params)
   ```

3. **Parallel Execution**
   ```python
   from prefect import flow
   from prefect.task_runners import ConcurrentTaskRunner
   
   @flow(task_runner=ConcurrentTaskRunner())
   async def parallel_analytics_flow(chat_id: str):
       # Independent queries run in parallel
       async with asyncio.TaskGroup() as tg:
           hourly_task = tg.create_task(get_hourly_distribution(chat_id))
           daily_task = tg.create_task(get_daily_distribution(chat_id))
           user_task = tg.create_task(get_user_stats(chat_id))
   ```

## Next Steps

1. **Implement Query Builder**
   - Dynamic query generation
   - Query optimization hints
   - Query validation

2. **Add Result Visualization**
   - Generate charts from results
   - Export to dashboard tools
   - Email reports

3. **Implement Advanced Analytics**
   - LLM integration for text analysis
   - Sentiment analysis pipeline
   - Topic modeling workflow