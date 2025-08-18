"""
Test tgdata directly to debug the issue
"""

import asyncio
import sys
from pathlib import Path

# Add tgdata to path
sys.path.insert(0, str(Path.home() / "Desktop/projects/telegram-group-scraper"))

from tgdata import TgData


async def test_direct():
    """Test tgdata directly"""
    
    print("Testing tgdata directly...")
    print("-" * 60)
    
    # Use absolute path for config
    config_path = Path.cwd() / "config.ini"
    print(f"Config path: {config_path}")
    print(f"Config exists: {config_path.exists()}")
    
    # Initialize TgData
    tg = TgData(str(config_path))
    print("TgData initialized")
    
    # Test with Bitcoinsensus channel
    group_id = 1670178185
    print(f"\nTesting with group ID: {group_id}")
    
    try:
        # Get just 1 message to test
        print("Fetching 1 message...")
        df = await tg.get_messages(group_id=group_id, limit=1)
        print(f"✓ Success! Got {len(df)} messages")
        
        if not df.empty:
            print(f"Message ID: {df.iloc[0]['MessageId']}")
            print(f"Date: {df.iloc[0]['Date']}")
            
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await tg.close()
        print("\nTgData closed")


if __name__ == "__main__":
    asyncio.run(test_direct())