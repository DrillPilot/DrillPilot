"""
DrillPilot orchestration API — custom engine, no Temporal.

Two websocket channels, one per dashboard tab:
  /ws/status         -> Tab 1: live server/service status grid
  /ws/drills/{run_id} -> Tab 2: live step progress + log lines for one drill run

Two REST actions:
  POST /api/drills/start          -> kick off a workflow file, returns run_id
  POST /api/drills/{run_id}/resume -> unpause a drill sitting on a failed step
"""

import asyncio
import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from drill_engine import DrillEngine
from status_poller import StatusStore

app = FastAPI(title="DrillPilot Orchestrator")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

engine = DrillEngine()

# --- simple in-memory websocket registries -------------------------------
status_sockets: set[WebSocket] = set()
drill_sockets: dict[str, set[WebSocket]] = {}


async def broadcast_status(snapshot: list[dict]):
    dead = set()
    for ws in status_sockets:
        try:
            await ws.send_json({"type": "status", "data": snapshot})
        except Exception:
            dead.add(ws)
    status_sockets.difference_update(dead)


async def broadcast_drill(run):
    sockets = drill_sockets.get(run.run_id, set())
    dead = set()
    for ws in sockets:
        try:
            await ws.send_json({"type": "drill", "data": run.to_dict()})
        except Exception:
            dead.add(ws)
    sockets.difference_update(dead)


status_store = StatusStore(config_path="config.yaml", on_update=broadcast_status)


@app.on_event("startup")
async def startup():
    asyncio.create_task(status_store.run_forever())


# --- Tab 1: status ---------------------------------------------------------

@app.get("/api/status")
async def get_status():
    return list(status_store.latest.values())

@app.get("/api/status/{target}")
async def get_status_one(target: str):
    for item in status_store.latest.values():
        if item.get("name") == target:
            return item

    return {
        "name": target,
        "status": "Unknown"
    }

@app.get("/api/workflows")
async def list_workflows():
    """Feeds the dropdown on Tab 2 — every .yaml file in workflows/."""
    folder = "workflows"
    if not os.path.isdir(folder):
        return []
    return [f"{folder}/{f}" for f in sorted(os.listdir(folder)) if f.endswith((".yaml", ".yml"))]


@app.websocket("/ws/status")
async def ws_status(ws: WebSocket):
    await ws.accept()
    status_sockets.add(ws)
    await ws.send_json({"type": "status", "data": list(status_store.latest.values())})
    try:
        while True:
            await ws.receive_text()   # keep-alive; client doesn't need to send anything meaningful
    except WebSocketDisconnect:
        status_sockets.discard(ws)


# --- Tab 2: drill control ---------------------------------------------------

class StartDrillRequest(BaseModel):
    workflow_file: str


@app.post("/api/drills/start")
async def start_drill(req: StartDrillRequest):
    run_id = await engine.start(req.workflow_file, broadcast=broadcast_drill)
    return {"run_id": run_id}


@app.post("/api/drills/{run_id}/resume")
async def resume_drill(run_id: str):
    await engine.resume(run_id)
    return {"status": "resume signal sent"}


@app.get("/api/drills/{run_id}")
async def get_drill(run_id: str):
    run = engine.runs.get(run_id)
    return run.to_dict() if run else {"error": "not found"}


@app.websocket("/ws/drills/{run_id}")
async def ws_drill(ws: WebSocket, run_id: str):
    await ws.accept()
    drill_sockets.setdefault(run_id, set()).add(ws)
    run = engine.runs.get(run_id)
    if run:
        await ws.send_json({"type": "drill", "data": run.to_dict()})
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        drill_sockets[run_id].discard(ws)


# --- Dashboard (must be mounted last so it doesn't shadow /api and /ws) -----
# app.mount("/", StaticFiles(directory="static", html=True), name="static")

from fastapi.responses import FileResponse

@app.get("/")
async def home():
    return FileResponse("static/login.html")

app.mount("/static", StaticFiles(directory="static"), name="static")

from fastapi.responses import RedirectResponse

@app.get("/{full_path:path}", include_in_schema=False)
async def catch_all(full_path: str):
    return RedirectResponse(url="/")
