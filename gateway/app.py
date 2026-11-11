"""Gateway: Kafka -> WebSocket for the 3D view, plus the static page.

  GET /            the 3D viewer (static/index.html)
  GET /api/track   centre line of the circuit, from one clean lap of OpenF1 location data
  GET /api/drivers drivers of the session with team colours
  WS  /ws          live stream: {"topic": ..., "data": {...}} for every topic below

Every browser gets its own consumer position (the gateway reads with a fresh
group id and "latest"), plus the last known state of each driver on connect.

Env: KAFKA_BROKERS, SESSION_KEY (latest, must match the producer)
"""
import asyncio
import logging
import os
import threading
import uuid
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

from confluent_kafka import Consumer
from fastapi import FastAPI, HTTPException, WebSocket
from fastapi.staticfiles import StaticFiles

from openf1 import OpenF1

log = logging.getLogger("gateway")

TOPICS = ["telemetry.raw", "car.position", "race.laps", "race.events", "telemetry.predictions"]
SESSION_KEY = os.getenv("SESSION_KEY", "latest")
BATCH_S = float(os.getenv("WS_BATCH_MS", "50")) / 1000  # WebSocket frames carry ~50 ms of messages
MAX_BATCH = 500

clients = set()    # one asyncio.Queue per connected browser
state = {}         # (topic, key) -> last message text, replayed to new browsers
recent_events = deque(maxlen=15)
api = OpenF1()
cache = {}


def publish(topic, key, value):
    """Runs on the event loop: remember the message and hand it to every browser."""
    text = '{"topic":"%s","data":%s}' % (topic, value)
    if topic == "race.events":
        recent_events.append(text)
    else:
        state[(topic, key)] = text
    for q in clients:
        if q.full():
            q.get_nowait()  # slow browser: drop its oldest message
        q.put_nowait(text)


def consume(loop):
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BROKERS", "localhost:9092"),
        "group.id": f"f1-gateway-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(TOPICS)
    while True:
        m = consumer.poll(1.0)
        if m is None or m.error():
            continue
        loop.call_soon_threadsafe(publish, m.topic(), (m.key() or b"").decode(), m.value().decode())


@asynccontextmanager
async def lifespan(app):
    threading.Thread(target=consume, args=(asyncio.get_running_loop(),), daemon=True).start()
    yield


app = FastAPI(lifespan=lifespan)


def session():
    if "session" not in cache:
        cache["session"] = api.get("sessions", session_key=SESSION_KEY)[-1]
    return cache["session"]


@app.get("/api/drivers")
def drivers():
    if "drivers" not in cache:
        cache["drivers"] = [{
            "number": d["driver_number"], "code": d["name_acronym"], "name": d["full_name"],
            "team": d["team_name"], "colour": "#" + (d.get("team_colour") or "888888"),
        } for d in api.get("drivers", session_key=session()["session_key"])]
    return cache["drivers"]


@app.get("/api/track")
def track():
    """One clean, fast lap of one driver: its x/y/z samples are the circuit centre line."""
    if "track" in cache:
        return cache["track"]
    s = session()
    laps = [l for l in api.get("laps", session_key=s["session_key"])
            if l.get("date_start") and l.get("lap_duration") and not l.get("is_pit_out_lap")
            and l["lap_number"] >= 2]
    if not laps:
        raise HTTPException(503, "no laps available for this session yet")
    lap = min(laps, key=lambda l: l["lap_duration"])
    start = datetime.fromisoformat(lap["date_start"])
    end = start + timedelta(seconds=lap["lap_duration"])
    pts = api.get("location", session_key=s["session_key"], driver_number=lap["driver_number"],
                  **{"date>": start.isoformat(), "date<": end.isoformat()})
    pts = [[p["x"], p["y"], p["z"]] for p in pts]
    if len(pts) < 50:
        raise HTTPException(503, "not enough position data to draw the circuit")
    cache["track"] = {"points": pts, "circuit": s["location"], "session": s["session_name"],
                      "date": s["date_start"][:10]}
    return cache["track"]


@app.websocket("/ws")
async def ws(sock: WebSocket):
    await sock.accept()
    q = asyncio.Queue(maxsize=3000)
    for text in list(recent_events) + list(state.values()):
        q.put_nowait(text)
    clients.add(q)

    async def drain():  # notice when the browser leaves
        try:
            while True:
                await sock.receive_text()
        except Exception:
            pass

    watcher = asyncio.create_task(drain())
    try:
        while not watcher.done():
            try:
                batch = [await asyncio.wait_for(q.get(), 1.0)]
            except asyncio.TimeoutError:
                continue
            await asyncio.sleep(BATCH_S)  # let messages pile up, then send them in one frame
            while not q.empty() and len(batch) < MAX_BATCH:
                batch.append(q.get_nowait())
            await sock.send_text("[" + ",".join(batch) + "]")
    except Exception:
        pass
    finally:
        watcher.cancel()
        clients.discard(q)


app.mount("/", StaticFiles(directory="static", html=True), name="static")
