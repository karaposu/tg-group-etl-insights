"""
Prefect flow for running generic SQL analytics
"""

from prefect import flow, task, get_run_logger
from typing import Dict, Any, Optional, List
import pandas as pd
from datetime import datetime
import time
import os
from pathlib import Path

from ..sqlite.executor import SQLiteAnalyticsExecutor
from ..sqlite.queries import MESSAGE_QUERIES, USER_QUERIES, CHAT_QUERIES
from ..sqlite.message_analytics_saver import MessageAnalyticsSaver


@task(
    name="run_message_analytics",
    description="Run message-level analytics",
    retries=2,
    retry_delay_seconds=30
)
async def run_message_analytics(
    executor: SQLiteAnalyticsExecutor,
    chat_id: str,
    queries_to_run: Optional[List[str]] = None
) -> Dict[str, pd.DataFrame]:
    """
    Execute message analytics queries
    
    Args:
        executor: Database executor
        chat_id: Chat to analyze
        queries_to_run: Specific queries to run (None = all)
        
    Returns:
        Dictionary of query results
    """
    logger = get_run_logger()
    logger.info(f"Running message analytics for chat {chat_id}")
    
    results = {}
    queries = queries_to_run or MESSAGE_QUERIES.keys()
    
    for query_name in queries:
        if query_name in MESSAGE_QUERIES:
            query_obj = MESSAGE_QUERIES[query_name]
            logger.info(f"Running: {query_obj.name}")
            
            # Prepare parameters
            params = {"chat_id": chat_id}
            if query_obj.parameters:
                params.update(query_obj.parameters)
            
            # Execute query
            try:
                results[query_name] = executor.execute_query(
                    query_obj.sql,
                    params
                )
                logger.info(f"✓ Completed {query_name}: {len(results[query_name])} rows")
            except Exception as e:
                logger.error(f"Failed {query_name}: {e}")
                results[query_name] = pd.DataFrame()
    
    return results


@task(
    name="run_user_analytics",
    description="Run user-level analytics",
    retries=2
)
async def run_user_analytics(
    executor: SQLiteAnalyticsExecutor,
    chat_id: str,
    message_results: Optional[Dict[str, pd.DataFrame]] = None,
    queries_to_run: Optional[List[str]] = None
) -> Dict[str, pd.DataFrame]:
    """
    Execute user analytics queries
    
    Args:
        executor: Database executor
        chat_id: Chat to analyze
        message_results: Results from message analytics (for dependencies)
        queries_to_run: Specific queries to run
        
    Returns:
        Dictionary of query results
    """
    logger = get_run_logger()
    logger.info(f"Running user analytics for chat {chat_id}")
    
    results = {}
    queries = queries_to_run or USER_QUERIES.keys()
    
    for query_name in queries:
        if query_name in USER_QUERIES:
            query_obj = USER_QUERIES[query_name]
            logger.info(f"Running: {query_obj.name}")
            
            params = {"chat_id": chat_id}
            if query_obj.parameters:
                params.update(query_obj.parameters)
            
            try:
                results[query_name] = executor.execute_query(
                    query_obj.sql,
                    params
                )
                logger.info(f"✓ Completed {query_name}: {len(results[query_name])} rows")
            except Exception as e:
                logger.error(f"Failed {query_name}: {e}")
                results[query_name] = pd.DataFrame()
        
    return results


@task(
    name="run_chat_analytics",
    description="Run chat-level analytics"
)
async def run_chat_analytics(
    executor: SQLiteAnalyticsExecutor,
    chat_id: str,
    user_results: Optional[Dict[str, pd.DataFrame]] = None,
    queries_to_run: Optional[List[str]] = None
) -> Dict[str, pd.DataFrame]:
    """
    Execute chat analytics queries
    
    Args:
        executor: Database executor
        chat_id: Chat to analyze
        user_results: Results from user analytics (for dependencies)
        queries_to_run: Specific queries to run
        
    Returns:
        Dictionary of query results
    """
    logger = get_run_logger()
    logger.info(f"Running chat analytics for chat {chat_id}")
    
    results = {}
    queries = queries_to_run or CHAT_QUERIES.keys()
    
    for query_name in queries:
        if query_name in CHAT_QUERIES:
            query_obj = CHAT_QUERIES[query_name]
            logger.info(f"Running: {query_obj.name}")
            
            params = {"chat_id": chat_id}
            if query_obj.parameters:
                params.update(query_obj.parameters)
            
            try:
                results[query_name] = executor.execute_query(
                    query_obj.sql,
                    params
                )
                logger.info(f"✓ Completed {query_name}: {len(results[query_name])} rows")
            except Exception as e:
                logger.error(f"Failed {query_name}: {e}")
                results[query_name] = pd.DataFrame()
        
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
    """
    Save analytics results to files
    
    Args:
        results: Analytics results to save
        output_format: Format for output files (parquet, csv, json)
        output_path: Base path for output files
        
    Returns:
        Dictionary of saved file paths
    """
    logger = get_run_logger()
    
    # Create output directory with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_path = Path(output_path) / timestamp
    base_path.mkdir(parents=True, exist_ok=True)
    
    saved_files = {}
    
    for category, category_results in results.items():
        for query_name, df in category_results.items():
            if df.empty:
                logger.warning(f"Skipping empty result: {category}.{query_name}")
                continue
                
            # Generate file path
            if output_format == "parquet":
                file_path = base_path / f"{category}_{query_name}.parquet"
                df.to_parquet(file_path)
            elif output_format == "csv":
                file_path = base_path / f"{category}_{query_name}.csv"
                df.to_csv(file_path, index=False)
            else:  # json
                file_path = base_path / f"{category}_{query_name}.json"
                df.to_json(file_path, orient='records', date_format='iso')
            
            saved_files[f"{category}.{query_name}"] = str(file_path)
            logger.info(f"Saved {query_name} to {file_path}")
    
    # Save metadata
    metadata_path = base_path / "metadata.json"
    import json
    metadata = {
        "timestamp": timestamp,
        "files": saved_files,
        "format": output_format,
        "total_queries": len(saved_files)
    }
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    logger.info(f"Saved {len(saved_files)} results to {base_path}")
    return saved_files


@task(
    name="print_summary",
    description="Print analytics summary"
)
async def print_analytics_summary(
    results: Dict[str, Dict[str, pd.DataFrame]]
) -> None:
    """
    Print a summary of analytics results
    
    Args:
        results: Analytics results to summarize
    """
    logger = get_run_logger()
    
    print("\n" + "="*60)
    print("ANALYTICS SUMMARY")
    print("="*60)
    
    for category, category_results in results.items():
        print(f"\n{category.upper()} ANALYTICS:")
        print("-"*40)
        
        for query_name, df in category_results.items():
            if df.empty:
                print(f"  {query_name}: No data")
                continue
                
            print(f"  {query_name}: {len(df)} rows")
            
            # Print sample for certain queries
            if query_name == "chat_health" and not df.empty:
                row = df.iloc[0]
                print(f"    - Total messages: {row.get('total_messages', 'N/A')}")
                print(f"    - Active users: {row.get('active_users', 'N/A')}")
                print(f"    - Participation rate: {row.get('participation_rate', 'N/A')}%")
                print(f"    - Duration: {row.get('chat_duration_days', 'N/A')} days")
            
            elif query_name == "user_activity" and len(df) > 0:
                print(f"    - Top user: {df.iloc[0].get('username', 'N/A')} ({df.iloc[0].get('total_messages', 0)} messages)")
            
            elif query_name == "hourly_distribution" and len(df) > 0:
                peak_hour = df.loc[df['message_count'].idxmax()]
                print(f"    - Peak hour: {int(peak_hour['hour']):02d}:00 ({peak_hour['message_count']} messages)")
    
    print("\n" + "="*60)


@flow(
    name="generic_analytics_pipeline",
    description="Run complete generic analytics pipeline"
)
async def generic_analytics_flow(
    chat_id: str,
    db_path: str = "data/telegram.db",
    db_type: str = "sqlite",
    output_format: str = "parquet",
    save_results: bool = True,
    save_to_db: bool = True,
    print_summary: bool = True,
    optimize_db: bool = False
) -> Dict[str, Any]:
    """
    Main analytics flow - runs analytics in sequence
    
    Args:
        chat_id: Chat to analyze
        db_path: Path to database
        db_type: Database type (sqlite or bigquery)
        output_format: Output format (parquet, csv, json)
        save_results: Whether to save results to files
        save_to_db: Whether to save results to database tables
        print_summary: Whether to print summary
        optimize_db: Whether to optimize database before running
        
    Returns:
        Dictionary with all analytics results
    """
    logger = get_run_logger()
    logger.info(f"Starting analytics pipeline for chat {chat_id}")
    logger.info(f"Database: {db_type} at {db_path}")
    
    # Track timing
    start_time = time.time()
    
    # Initialize executor and results saver based on database type
    if db_type == "sqlite":
        executor = SQLiteAnalyticsExecutor(db_path)
        results_saver = AnalyticsResultsSaver(db_path) if save_to_db else None
        
        # Optimize database if requested
        if optimize_db:
            logger.info("Optimizing database for analytics...")
            executor.optimize_for_analytics()
    else:
        raise NotImplementedError(f"Database type {db_type} not yet implemented")
    
    # Start analytics run if saving to DB
    run_id = None
    if results_saver:
        run_id = results_saver.start_run(chat_id, run_type="full")
        logger.info(f"Started analytics run: {run_id}")
    
    try:
        # Sequential execution: Messages → Users → Chat
        logger.info("="*50)
        logger.info("Phase 1: Message Analytics")
        logger.info("-"*50)
        message_results = await run_message_analytics(executor, chat_id)
        
        logger.info("="*50)
        logger.info("Phase 2: User Analytics")
        logger.info("-"*50)
        user_results = await run_user_analytics(executor, chat_id, message_results)
        
        logger.info("="*50)
        logger.info("Phase 3: Chat Analytics")
        logger.info("-"*50)
        chat_results = await run_chat_analytics(executor, chat_id, user_results)
        
        # Combine results
        all_results = {
            "messages": message_results,
            "users": user_results,
            "chat": chat_results
        }
        
        # Save to database if requested
        if results_saver and run_id:
            logger.info("Saving analytics results to database...")
            
            # Count total messages and users analyzed
            total_messages = 0
            total_users = 0
            
            if "individual_messages" in message_results:
                total_messages = len(message_results["individual_messages"])
            if "user_activity" in user_results:
                total_users = len(user_results["user_activity"])
            
            # Save results to database
            results_saver.save_message_analytics(run_id, chat_id, message_results)
            results_saver.save_user_analytics(run_id, chat_id, user_results)
            results_saver.save_chat_analytics(run_id, chat_id, chat_results)
            
            # Complete the run
            duration = time.time() - start_time
            results_saver.complete_run(
                run_id,
                total_messages=total_messages,
                total_users=total_users,
                duration_seconds=duration,
                metadata={"output_format": output_format}
            )
            logger.info(f"Analytics results saved to database (run_id: {run_id})")
        
        # Print summary if requested
        if print_summary:
            await print_analytics_summary(all_results)
        
        # Save results if requested
        saved_files = {}
        if save_results:
            saved_files = await save_analytics_results(
                all_results,
                output_format=output_format
            )
            logger.info(f"Analytics saved to {len(saved_files)} files")
        
        logger.info("="*50)
        logger.info("Analytics pipeline completed successfully")
        logger.info("="*50)
        
        return {
            "status": "success",
            "chat_id": chat_id,
            "run_id": run_id,
            "results": all_results,
            "saved_files": saved_files,
            "duration_seconds": time.time() - start_time,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Analytics pipeline failed: {e}")
        
        # Mark run as failed if we started one
        if results_saver and run_id:
            results_saver.fail_run(run_id, str(e))
        
        raise
        
    finally:
        # Clean up
        if 'executor' in locals():
            executor.close()
        if 'results_saver' in locals() and results_saver:
            results_saver.close()