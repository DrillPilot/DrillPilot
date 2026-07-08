"""
Background poller for Tab 1 — "what's the current state of everything".

Every POLL_INTERVAL seconds, checks each target in config.yaml (app_servers
+ databases) with a lightweight `systemctl is-active <service>` over the
same SSH connection the drill engine uses, and keeps the latest snapshot
in memory for the dashboard to read or subscribe to.

This is intentionally separate from drill_engine.py: status checks run
continuously regardless of whether a drill is in progress, and a drill
in progress should never be blocked waiting on a status poll.
"""

import asyncio
import time
from typing import Callable

import yaml

from connectors.ssh_service_connector import check_status

POLL_INTERVAL_SECONDS = 5


class StatusStore:
    def __init__(self, config_path: str, on_update: Callable):
        self.config_path = config_path
        self.on_update = on_update
        self.latest: dict[str, dict] = {}

    def _targets(self):
        with open(self.config_path) as f:
            cfg = yaml.safe_load(f)
        # app_servers and databases are both just "a systemd service on a host"
        # to this poller, exactly like the drill engine treats them
        return cfg.get("app_servers", []) + cfg.get("databases", [])

    async def poll_once(self):
        for target in self._targets():
            name = target["name"]
            try:
                is_active = await asyncio.to_thread(check_status, name)
                status_data = {
                    "name": name,
                    "site": "dr" if name.endswith("-dr") else "dc",
                    "service": target.get("service") or target.get("type"),
                    "status": "running" if is_active else "stopped",
                    "checked_at": time.strftime("%H:%M:%S"),
                }
                # For databases, include the role from config
                if target.get("type") == "mysql":
                    status_data["role"] = target.get("role", "secondary")
                self.latest[name] = status_data
            except Exception as exc:
                error_data = {
                    "name": name,
                    "site": "dr" if name.endswith("-dr") else "dc",
                    "service": target.get("service") or target.get("type"),
                    "status": "unreachable",
                    "error": str(exc),
                    "checked_at": time.strftime("%H:%M:%S"),
                }
                # For databases, include the role from config even in error case
                if target.get("type") == "mysql":
                    error_data["role"] = target.get("role", "secondary")
                self.latest[name] = error_data
        await self.on_update(list(self.latest.values()))

    async def run_forever(self):
        while True:
            await self.poll_once()
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
