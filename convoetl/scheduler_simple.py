"""
Simple scheduler for ConvoETL
"""

from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class Scheduler:
    """
    Simple scheduler for ConvoETL flows
    """
    
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
            script_path = "run_convoetl.py"
        
        # Generate cron expression
        if interval_minutes < 60:
            cron_time = f"*/{interval_minutes} * * * *"
        else:
            hours = interval_minutes // 60
            cron_time = f"0 */{hours} * * *"
        
        cron_job = (
            f"{cron_time} cd /path/to/project && {python_path} {script_path} "
            f"--platform {platform} --source-id {source_id} >> "
            f"logs/convoetl_{platform}_{source_id}.log 2>&1"
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
ExecStart={python_path} run_convoetl.py --platform {platform} --source-id {source_id}
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
        method: str = "cron"
    ):
        """
        Print setup instructions for scheduling
        
        Args:
            platform: Platform name
            source_id: Source identifier
            method: Scheduling method (cron, systemd, manual)
        """
        print("\n" + "=" * 60)
        print(f"📅 Scheduling Setup Instructions for {platform}/{source_id}")
        print("=" * 60)
        
        if method == "manual":
            print("\n### Manual Polling:")
            print("Run this command to start continuous polling:")
            print(f"   python run_convoetl.py --poll --platform {platform} --source-id {source_id}")
            
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