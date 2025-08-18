"""
Scheduler for ConvoETL using Prefect deployments
"""

from datetime import timedelta
from typing import Dict, Any, Optional
import logging

# Note: Prefect 2.14+ uses flow.serve() and flow.deploy() instead of Deployment class
# This scheduler provides utility functions for scheduling

logger = logging.getLogger(__name__)


class Scheduler:
    """
    Schedule ConvoETL flows using Prefect deployments
    """
    
    @staticmethod
    def create_polling_deployment(
        name: str,
        platform: str,
        source_id: str,
        extractor_config: Dict[str, Any],
        storage_config: Dict[str, Any],
        interval_minutes: int = 5,
        work_queue: str = "default"
    ) -> Deployment:
        """
        Create a Prefect deployment for continuous polling
        
        Args:
            name: Deployment name
            platform: Platform name
            source_id: Source identifier
            extractor_config: Extractor configuration
            storage_config: Storage configuration
            interval_minutes: Polling interval in minutes
            work_queue: Prefect work queue name
            
        Returns:
            Prefect Deployment object
        """
        deployment = Deployment.build_from_flow(
            flow=incremental_flow,
            name=name,
            parameters={
                "platform": platform,
                "source_id": source_id,
                "extractor_config": extractor_config,
                "storage_config": storage_config,
                "limit": 5000
            },
            schedule=IntervalSchedule(interval=timedelta(minutes=interval_minutes)),
            work_queue_name=work_queue,
            tags=[platform, "polling", source_id]
        )
        
        logger.info(f"Created polling deployment '{name}' with {interval_minutes} minute interval")
        return deployment
    
    @staticmethod
    def create_daily_sync_deployment(
        name: str,
        platform: str,
        source_id: str,
        extractor_config: Dict[str, Any],
        storage_config: Dict[str, Any],
        hour: int = 2,
        minute: int = 0,
        work_queue: str = "default"
    ) -> Deployment:
        """
        Create a daily sync deployment
        
        Args:
            name: Deployment name
            platform: Platform name
            source_id: Source identifier
            extractor_config: Extractor configuration
            storage_config: Storage configuration
            hour: Hour to run (0-23)
            minute: Minute to run (0-59)
            work_queue: Work queue name
            
        Returns:
            Prefect Deployment
        """
        # Cron expression for daily run
        cron_expression = f"{minute} {hour} * * *"
        
        deployment = Deployment.build_from_flow(
            flow=convoetl_flow,
            name=name,
            parameters={
                "platform": platform,
                "source_id": source_id,
                "extractor_config": extractor_config,
                "storage_config": storage_config,
                "mode": "incremental",
                "analyze": True
            },
            schedule=CronSchedule(cron=cron_expression),
            work_queue_name=work_queue,
            tags=[platform, "daily", source_id]
        )
        
        logger.info(f"Created daily sync deployment '{name}' at {hour:02d}:{minute:02d}")
        return deployment
    
    @staticmethod
    async def deploy_polling(
        platform: str,
        source_id: str,
        extractor_config: Dict[str, Any],
        storage_config: Dict[str, Any],
        interval_minutes: int = 5,
        deployment_name: Optional[str] = None
    ):
        """
        Deploy a polling schedule for a source
        
        Args:
            platform: Platform name
            source_id: Source identifier
            extractor_config: Extractor configuration
            storage_config: Storage configuration
            interval_minutes: Polling interval
            deployment_name: Optional custom deployment name
        """
        name = deployment_name or f"{platform}-{source_id}-polling"
        
        deployment = Scheduler.create_polling_deployment(
            name=name,
            platform=platform,
            source_id=source_id,
            extractor_config=extractor_config,
            storage_config=storage_config,
            interval_minutes=interval_minutes
        )
        
        # Apply the deployment
        deployment_id = await deployment.apply()
        
        logger.info(f"Deployed polling schedule with ID: {deployment_id}")
        print(f"✅ Polling deployment created: {name}")
        print(f"   Interval: {interval_minutes} minutes")
        print(f"   Source: {platform}/{source_id}")
        print(f"\nTo start a Prefect agent:")
        print(f"   prefect agent start -q default")
        
        return deployment_id
    
    @staticmethod
    def generate_cron_job(
        platform: str,
        source_id: str,
        interval_minutes: int = 5,
        python_path: str = "python",
        script_path: str = None
    ) -> str:
        """
        Generate a cron job entry for system crontab
        
        Args:
            platform: Platform name
            source_id: Source identifier
            interval_minutes: Interval in minutes
            python_path: Path to Python executable
            script_path: Path to the sync script
            
        Returns:
            Cron job string
        """
        if not script_path:
            script_path = "convoetl_sync.py"
        
        # Generate cron expression
        if interval_minutes < 60:
            cron_time = f"*/{interval_minutes} * * * *"
        else:
            hours = interval_minutes // 60
            cron_time = f"0 */{hours} * * *"
        
        cron_job = (
            f"{cron_time} {python_path} {script_path} "
            f"--platform {platform} --source-id {source_id} >> "
            f"/var/log/convoetl_{platform}_{source_id}.log 2>&1"
        )
        
        return cron_job
    
    @staticmethod
    def generate_systemd_service(
        platform: str,
        source_id: str,
        interval_seconds: int = 300,
        python_path: str = "/usr/bin/python3",
        working_dir: str = "/opt/convoetl"
    ) -> tuple[str, str]:
        """
        Generate systemd service and timer files
        
        Args:
            platform: Platform name
            source_id: Source identifier
            interval_seconds: Interval in seconds
            python_path: Python executable path
            working_dir: Working directory
            
        Returns:
            Tuple of (service_content, timer_content)
        """
        service_name = f"convoetl-{platform}-{source_id}"
        
        # Service file
        service_content = f"""[Unit]
Description=ConvoETL Sync for {platform}/{source_id}
After=network.target

[Service]
Type=oneshot
WorkingDirectory={working_dir}
ExecStart={python_path} -m convoetl sync --platform {platform} --source-id {source_id}
User=convoetl
Group=convoetl

[Install]
WantedBy=multi-user.target
"""
        
        # Timer file
        timer_content = f"""[Unit]
Description=ConvoETL Timer for {platform}/{source_id}
Requires={service_name}.service

[Timer]
OnBootSec=60
OnUnitActiveSec={interval_seconds}

[Install]
WantedBy=timers.target
"""
        
        return service_content, timer_content
    
    @staticmethod
    def print_setup_instructions(
        platform: str,
        source_id: str,
        method: str = "prefect"
    ):
        """
        Print setup instructions for scheduling
        
        Args:
            platform: Platform name
            source_id: Source identifier
            method: Scheduling method (prefect, cron, systemd)
        """
        print("\n" + "=" * 60)
        print(f"📅 Scheduling Setup Instructions for {platform}/{source_id}")
        print("=" * 60)
        
        if method == "prefect":
            print("\n### Using Prefect (Recommended):")
            print("1. Start Prefect server:")
            print("   prefect server start")
            print("\n2. In another terminal, start an agent:")
            print("   prefect agent start -q default")
            print("\n3. Create deployment:")
            print(f"   python -m convoetl deploy --platform {platform} --source-id {source_id}")
            
        elif method == "cron":
            cron_job = Scheduler.generate_cron_job(platform, source_id)
            print("\n### Using Cron:")
            print("1. Open crontab:")
            print("   crontab -e")
            print("\n2. Add this line:")
            print(f"   {cron_job}")
            print("\n3. Save and exit")
            
        elif method == "systemd":
            service_name = f"convoetl-{platform}-{source_id}"
            print("\n### Using systemd:")
            print("1. Create service file:")
            print(f"   sudo nano /etc/systemd/system/{service_name}.service")
            print("\n2. Create timer file:")
            print(f"   sudo nano /etc/systemd/system/{service_name}.timer")
            print("\n3. Enable and start timer:")
            print(f"   sudo systemctl enable {service_name}.timer")
            print(f"   sudo systemctl start {service_name}.timer")
            print("\n4. Check status:")
            print(f"   sudo systemctl status {service_name}.timer")
        
        print("\n" + "=" * 60)