"""
Custom DR drill engine — replaces Temporal.

Runs a workflow (list of steps with dependencies) sequentially, calling the
existing connector functions (ssh_service_connector.stop/start), and gives
us three things Temporal gave us for free, but in plain, demoable Python:

  1. Live progress   -> self.steps[i]["status"] + on_update callback
  2. Live logs        -> self.log(...) + on_update callback
  3. Pause on error / resume from checkpoint -> asyncio.Event per run

No external workflow engine, no separate UI to explain to a customer —
this *is* the thing the dashboard talks to directly.
"""

import asyncio
import time
import uuid
from enum import Enum
from typing import Callable, Optional

import yaml

from connectors.ssh_service_connector import service_action


class StepStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    PAUSED = "paused"          # the whole run is paused at this step
    SKIPPED = "skipped"


class DrillRun:
    """One in-progress or completed execution of a workflow file."""

    def __init__(self, run_id: str, workflow: dict, on_update: Callable):
        self.run_id = run_id
        self.name = workflow.get("drill", "Unnamed drill")
        self.steps = [
            {**step, "status": StepStatus.PENDING, "error": None, "duration_s": None}
            for step in workflow["steps"]
        ]
        self.on_update = on_update          # async callback(run) -> broadcasts to websockets
        self.logs: list[str] = []
        self.status = "running"             # running | paused | completed | failed
        self._resume_event = asyncio.Event()
        self._resume_event.set()            # not paused initially

    def log(self, message: str):
        line = f"{time.strftime('%H:%M:%S')} {message}"
        self.logs.append(line)
        self._emit()

    def _emit(self):
        # fire-and-forget update to any connected dashboard clients
        asyncio.create_task(self.on_update(self))

    def to_dict(self):
        return {
            "run_id": self.run_id,
            "name": self.name,
            "status": self.status,
            "steps": [
                {k: v for k, v in s.items() if k != "action_fn"} for s in self.steps
            ],
            "logs": self.logs[-200:],       # cap payload size
        }

    async def resume(self):
        if self.status != "paused":
            return
        self.status = "running"
        self.log("Resume requested — continuing from the paused step")
        self._resume_event.set()


class DrillEngine:
    """Holds all runs in memory for the demo. Swap the dict for a DB-backed
    store later without changing the run() logic."""

    def __init__(self):
        self.runs: dict[str, DrillRun] = {}

    def load_workflow(self, path: str) -> dict:
        with open(path) as f:
            return yaml.safe_load(f)

    async def start(self, workflow_path: str, broadcast: Callable) -> str:
        workflow = self.load_workflow(workflow_path)
        run_id = str(uuid.uuid4())[:8]
        run = DrillRun(run_id, workflow, on_update=broadcast)
        self.runs[run_id] = run
        asyncio.create_task(self._execute(run))
        return run_id

    async def resume(self, run_id: str):
        run = self.runs.get(run_id)
        if run:
            await run.resume()

    async def _execute(self, run: DrillRun):
        run.log(f"Starting drill: {run.name}")
        for step in run.steps:
            # wait here if a previous failure paused the run
            await run._resume_event.wait()

            step["status"] = StepStatus.RUNNING
            run._emit()
            run.log(f"Running step '{step['id']}' -> {step['action']} on {step['target']}")

            start = time.monotonic()
            action = step["action"]

            if action.startswith("stop"):
                verb = "stop"

            elif action.startswith("start"):
                verb = "start"

            elif action == "check_replication":
                verb = "check_replication"

            elif action == "promote":
                verb = "promote"

            elif action == "demote":
                verb = "demote"

            elif action == "reverse_sync":
                verb = "reverse_sync"

            else:
                raise RuntimeError(f"Unknown workflow action: {action}")

            try:
                await asyncio.to_thread(service_action, step["target"], verb)
                step["status"] = StepStatus.DONE
                step["duration_s"] = round(time.monotonic() - start, 1)
                run.log(f"Step '{step['id']}' completed in {step['duration_s']}s")

            except Exception as exc:
                step["status"] = StepStatus.FAILED
                step["error"] = str(exc)
                run.status = "paused"
                run._resume_event.clear()
                run.log(f"Step '{step['id']}' FAILED: {exc} — drill paused")
                run._emit()

                await run._resume_event.wait()

                run.log(f"Retrying step '{step['id']}' after resume")

                try:
                    await asyncio.to_thread(service_action, step["target"], verb)
                    step["status"] = StepStatus.DONE
                    step["error"] = None
                    run.log(f"Step '{step['id']}' succeeded on retry")

                except Exception as exc2:
                    step["status"] = StepStatus.FAILED
                    step["error"] = str(exc2)
                    run.status = "failed"
                    run.log(f"Step '{step['id']}' failed again: {exc2} — stopping drill")
                    run._emit()
                    return

            run._emit()
