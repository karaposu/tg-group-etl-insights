"""
List groups to see the correct ID format
"""

import asyncio
import sys
from pathlib import Path

# Add tgdata to path
sys.path.insert(0, str(Path.home() / "Desktop/projects/telegram-group-scraper"))

from tgdata import TgData


async def test_list():
    """List groups to find Bitcoinsensus"""
    
    print("Listing groups to find correct ID format...")
    print("-" * 60)
    
    # Use absolute path for config
    config_path = Path.cwd() / "config.ini"
    
    # Initialize TgData
    tg = TgData(str(config_path))
    print("TgData initialized")
    
    try:
        # List groups
        groups = await tg.list_groups()
        print(f"Found {len(groups)} groups")
        
        # Find Bitcoinsensus
        bitcoin_groups = groups[groups['Title'].str.contains('Bitcoin', case=False, na=False)]
        
        if not bitcoin_groups.empty:
            print("\nBitcoin-related groups:")
            for idx, group in bitcoin_groups.iterrows():
                print(f"  Title: {group['Title']}")
                print(f"  GroupID: {group['GroupID']}")
                print(f"  Username: {group.get('Username', 'N/A')}")
                print(f"  Is Channel: {group.get('IsChannel', False)}")
                print()
        else:
            print("\nNo Bitcoin groups found. Showing first 5 groups:")
            for idx, group in groups.head(5).iterrows():
                print(f"  Title: {group['Title']}")
                print(f"  GroupID: {group['GroupID']}")
                print()
                
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await tg.close()
        print("\nTgData closed")


if __name__ == "__main__":
    asyncio.run(test_list())