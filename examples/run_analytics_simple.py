"""
Example script to run simplified analytics on extracted data
"""

import asyncio
import argparse
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from convoetl.analytics import message_analytics_flow


async def main():
    """Run simplified analytics pipeline"""
    
    parser = argparse.ArgumentParser(description="Run analytics on extracted chat data")
    parser.add_argument(
        "--chat-id",
        type=str,
        default="1670178185",
        help="Chat ID to analyze"
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="data/telegram.db",
        help="Path to SQLite database"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Maximum messages to analyze"
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Don't save to message_analytics table"
    )
    parser.add_argument(
        "--aggregates",
        action="store_true",
        help="Run aggregate queries for reports"
    )
    
    args = parser.parse_args()
    
    print(f"\n{'='*60}")
    print(f"Running analytics for chat {args.chat_id}")
    print(f"Database: {args.db_path}")
    print(f"Limit: {args.limit} messages")
    print(f"{'='*60}\n")
    
    try:
        # Run analytics pipeline
        results = await message_analytics_flow(
            chat_id=args.chat_id,
            db_path=args.db_path,
            limit=args.limit,
            save_to_db=not args.no_save,
            run_aggregates=args.aggregates
        )
        
        if results["status"] == "success":
            print(f"\n✓ Analytics completed successfully!")
            print(f"  Messages analyzed: {results['messages_analyzed']}")
            print(f"  Messages saved: {results['messages_saved']}")
            print(f"  Duration: {results['duration_seconds']:.2f} seconds")
            
            if args.aggregates and results.get('aggregate_results'):
                print(f"\n{'='*60}")
                print("AGGREGATE RESULTS")
                print(f"{'='*60}")
                
                for category, queries in results['aggregate_results'].items():
                    print(f"\n{category.upper()}:")
                    for query_name, df in queries.items():
                        if not df.empty:
                            print(f"  {query_name}: {len(df)} rows")
        else:
            print(f"\n✗ Analytics failed")
            
    except Exception as e:
        print(f"\n✗ Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)