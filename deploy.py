"""
Deploy ConvoETL with scheduling
"""

import asyncio
import click
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

from convoetl.scheduler import Scheduler
from convoetl import Pipeline


@click.command()
@click.option('--platform', default='telegram', help='Platform to extract from')
@click.option('--source-id', required=True, help='Source ID (group/channel ID)')
@click.option('--interval', default=5, help='Polling interval in minutes')
@click.option('--method', default='prefect', type=click.Choice(['prefect', 'cron', 'systemd']))
@click.option('--storage', default='sqlite', help='Storage backend')
@click.option('--config-path', default='config.ini', help='Path to config file')
def deploy(platform, source_id, interval, method, storage, config_path):
    """Deploy ConvoETL with scheduling"""
    
    print(f"\n🚀 Deploying ConvoETL for {platform}/{source_id}")
    print(f"   Method: {method}")
    print(f"   Interval: {interval} minutes")
    print(f"   Storage: {storage}")
    
    # Prepare configurations
    extractor_config = {"config_path": config_path}
    storage_config = {"db_path": f"data/{platform}_{source_id}.db"} if storage == 'sqlite' else {}
    
    if method == 'prefect':
        # Deploy with Prefect
        async def deploy_prefect():
            deployment_id = await Scheduler.deploy_polling(
                platform=platform,
                source_id=source_id,
                extractor_config=extractor_config,
                storage_config=storage_config,
                interval_minutes=interval
            )
            return deployment_id
        
        deployment_id = asyncio.run(deploy_prefect())
        print(f"\n✅ Deployment created with ID: {deployment_id}")
        
    elif method == 'cron':
        # Generate cron job
        cron_job = Scheduler.generate_cron_job(
            platform=platform,
            source_id=source_id,
            interval_minutes=interval
        )
        
        print("\n📝 Add this line to your crontab (crontab -e):")
        print(f"\n{cron_job}\n")
        
        # Also create the sync script
        script_content = f"""#!/usr/bin/env python3
import asyncio
from convoetl import Pipeline

async def sync():
    pipeline = Pipeline(
        platform="{platform}",
        storage_type="{storage}",
        extractor_config={extractor_config},
        storage_config={storage_config}
    )
    result = await pipeline.sync(source_id="{source_id}")
    print(f"Synced {{result['new_messages']}} messages")

if __name__ == "__main__":
    asyncio.run(sync())
"""
        
        script_path = f"convoetl_sync_{platform}_{source_id}.py"
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        print(f"✅ Sync script created: {script_path}")
        
    elif method == 'systemd':
        # Generate systemd files
        service_content, timer_content = Scheduler.generate_systemd_service(
            platform=platform,
            source_id=source_id,
            interval_seconds=interval * 60
        )
        
        service_name = f"convoetl-{platform}-{source_id}"
        
        print(f"\n📝 Create these systemd files:")
        print(f"\n1. Service file: /etc/systemd/system/{service_name}.service")
        print("-" * 60)
        print(service_content)
        
        print(f"\n2. Timer file: /etc/systemd/system/{service_name}.timer")
        print("-" * 60)
        print(timer_content)
    
    # Show setup instructions
    Scheduler.print_setup_instructions(platform, source_id, method)


if __name__ == '__main__':
    deploy()