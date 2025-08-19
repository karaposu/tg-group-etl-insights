"""
Example script to run analytics on extracted data
"""

import asyncio
import argparse
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from convoetl.analytics import generic_analytics_flow


async def main():
    """Run analytics pipeline"""
    
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
        "--output-format",
        type=str,
        choices=["parquet", "csv", "json"],
        default="parquet",
        help="Output format for results"
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Don't save results to files"
    )
    parser.add_argument(
        "--no-summary",
        action="store_true",
        help="Don't print summary"
    )
    parser.add_argument(
        "--optimize",
        action="store_true",
        help="Optimize database before running analytics"
    )
    
    args = parser.parse_args()
    
    print(f"\n{'='*60}")
    print(f"Running analytics for chat {args.chat_id}")
    print(f"Database: {args.db_path}")
    print(f"Output format: {args.output_format}")
    print(f"{'='*60}\n")
    
    try:
        # Run analytics pipeline
        results = await generic_analytics_flow(
            chat_id=args.chat_id,
            db_path=args.db_path,
            output_format=args.output_format,
            save_results=not args.no_save,
            print_summary=not args.no_summary,
            optimize_db=args.optimize
        )
        
        if results["status"] == "success":
            print(f"\n✓ Analytics completed successfully!")
            if results.get("saved_files"):
                print(f"✓ Results saved to data/analytics/")
        else:
            print(f"\n✗ Analytics failed")
            
    except Exception as e:
        print(f"\n✗ Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)