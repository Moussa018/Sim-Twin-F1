"""F1 producer: real data from the OpenF1 API -> several Kafka topics.

Topics written:
  telemetry.raw     car data per driver (key = driver, 6 partitions, 1 day retention)
  telemetry.latest  same samples, log-compacted: last known state per driver
  race.laps         one message per completed lap (key = driver)
  race.events       race control messages: flags, safety car, penalties (key = "race")

A cursor moves through the session at SPEED x real time (replay of a finished
session, or following a live one). Samples are stamped with the current time.
Laps and race events are published when the cursor reaches their timestamp, so
all topics stay in sync.

Env: KAFKA_BROKERS, SESSION_KEY (latest), DRIVERS (VER,NOR,...), SPEED (1),
     WINDOW_S (5), START_MIN (0), LOOP (1), METRICS_PORT (8000)
"""
import bisect
import json
import logging
import os
import queue
import threading
import time
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, start_http_server

from openf1 import OpenF1

log = logging.getLogger("producer")

SENT = Counter("f1_samples_sent_total", "Messages written to Kafka.", ["topic", "driver"])
FAILED = Counter("f1_send_errors_total", "Messages that failed to reach Kafka.", ["topic"])
SPEED_G = Gauge("f1_last_speed_kmh", "Last speed per driver.", ["driver"])


def parse(ts):
    return datetime.fromisoformat(ts)


def iso(dt):
    return dt.astimezone(timezone.utc).isoformat()


class TyreModel:
    """Tyre wear estimated from OpenF1 stints + laps (the API has no wear data)."""

    WEAR_PER_LAP = 2.0  # % per lap, rough estimate

    def __init__(self, laps, stints):
        by_driver = {}
        for lap in laps:
            if lap.get("date_start"):
                by_driver.setdefault(lap["driver_number"], []).append((parse(lap["date_start"]), lap["lap_number"]))
        self.laps = {}
        for d, items in by_driver.items():
            items.sort()
            self.laps[d] = ([t for t, _ in items], [n for _, n in items])
        self.stints = {}
        for st in stints:
            self.stints.setdefault(st["driver_number"], []).append(st)

    def wear(self, driver, t):
        starts, numbers = self.laps.get(driver, ([], []))
        i = bisect.bisect_right(starts, t) - 1
        lap = numbers[i] if i >= 0 else 1
        for st in self.stints.get(driver, []):
            if st["lap_start"] <= lap <= (st["lap_end"] or 10**6):
                age = (st["tyre_age_at_start"] or 0) + (lap - st["lap_start"])
                return min(100.0, round(age * self.WEAR_PER_LAP, 2))
        return None


def build_events(laps, race_control, names):
    """Laps and race-control messages as a time-sorted list of (time, topic, key, payload)."""
    events = []
    for lap in laps:
        driver = names.get(lap["driver_number"])
        if driver and lap.get("date_start") and lap.get("lap_duration"):
            end = parse(lap["date_start"]) + timedelta(seconds=lap["lap_duration"])
            events.append((end, "race.laps", driver, {
                "driver": driver, "lap_number": lap["lap_number"], "lap_duration": lap["lap_duration"],
                "sector1": lap.get("duration_sector_1"), "sector2": lap.get("duration_sector_2"),
                "sector3": lap.get("duration_sector_3"), "is_pit_out": bool(lap.get("is_pit_out_lap")),
            }))
    for rc in race_control:
        events.append((parse(rc["date"]), "race.events", "race", {
            "category": rc.get("category"), "flag": rc.get("flag"), "message": rc.get("message"),
            "lap_number": rc.get("lap_number"), "driver": names.get(rc.get("driver_number")),
            "scope": rc.get("scope"),
        }))
    events.sort(key=lambda e: e[0])
    return events


def fetcher(api, cfg, session, out):
    """Background thread: fetches consecutive time windows of car_data."""
    start = parse(session["date_start"]) + timedelta(minutes=cfg["start_min"])
    end = parse(session["date_end"])
    window = timedelta(seconds=cfg["window_s"])
    cursor = start
    while True:
        stop = cursor + window
        ahead = (stop - datetime.now(timezone.utc)).total_seconds()
        if ahead > 0:  # live: do not ask for data that does not exist yet
            time.sleep(ahead)
        try:
            rows = api.get("car_data", session_key=session["session_key"],
                           **{"date>": iso(cursor), "date<=": iso(stop)})
        except Exception as e:  # keep going on a bad window
            log.warning("car_data %s: %s", iso(cursor), e)
            rows = []
        out.put(rows)
        cursor = stop
        if cursor > end:
            if not cfg["loop"]:
                out.put(None)
                return
            log.info("session finished, restarting")
            cursor = start
            out.put("RESET")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    cfg = {
        "brokers": os.getenv("KAFKA_BROKERS", "localhost:9092"),
        "session": os.getenv("SESSION_KEY", "latest"),
        "drivers": {d.strip().upper() for d in os.getenv("DRIVERS", "").split(",") if d.strip()},
        "speed": float(os.getenv("SPEED", "1")),
        "window_s": float(os.getenv("WINDOW_S", "5")),
        "start_min": float(os.getenv("START_MIN", "0")),
        "loop": os.getenv("LOOP", "1") == "1",
    }
    start_http_server(int(os.getenv("METRICS_PORT", "8000")))

    api = OpenF1()
    session = api.get("sessions", session_key=cfg["session"])[-1]
    sk = session["session_key"]
    names = {d["driver_number"]: d["name_acronym"] for d in api.get("drivers", session_key=sk)}
    if cfg["drivers"]:
        names = {n: a for n, a in names.items() if a in cfg["drivers"]}
    if not names:
        raise SystemExit("no matching drivers in this session")
    laps = api.get("laps", session_key=sk)
    tyres = TyreModel(laps, api.get("stints", session_key=sk))
    events = build_events(laps, api.get("race_control", session_key=sk), names)
    first = parse(session["date_start"]) + timedelta(minutes=cfg["start_min"])
    events = [e for e in events if e[0] >= first]  # skip what happened before the replay start
    log.info("session %s: %s %s (%s), %d drivers, %d laps/events",
             sk, session["location"], session["session_name"], session["date_start"][:10],
             len(names), len(events))

    # Reliable producer: no duplicates on retry, wait for all in-sync replicas.
    producer = Producer({
        "bootstrap.servers": cfg["brokers"],
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 50,
    })
    headers = [("source", b"openf1"), ("session_key", str(sk).encode())]

    def delivered(err, msg):
        if err:
            FAILED.labels(msg.topic()).inc()
            log.warning("kafka %s: %s", msg.topic(), err)
        else:
            SENT.labels(msg.topic(), msg.key().decode()).inc()

    def send(topic, key, payload):
        while True:
            try:
                producer.produce(topic, key=key, value=json.dumps(payload), headers=headers, on_delivery=delivered)
                break
            except BufferError:  # local queue full: let it drain
                producer.poll(0.5)
        producer.poll(0)

    batches = queue.Queue(maxsize=3)
    threading.Thread(target=fetcher, args=(api, cfg, session, batches), daemon=True).start()

    base_data = base_wall = None
    next_event = 0
    try:
        while True:
            batch = batches.get()
            if batch is None:
                break
            if batch == "RESET":
                base_data, next_event = None, 0
                continue
            for row in batch:
                driver = names.get(row["driver_number"])
                if driver is None:
                    continue
                t = parse(row["date"])
                if base_data is None:
                    base_data, base_wall = t, time.monotonic()
                delay = base_wall + (t - base_data).total_seconds() / cfg["speed"] - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                elif delay < -2:  # fell behind (slow API): resync instead of bursting
                    base_data, base_wall = t, time.monotonic()

                now = iso(datetime.now(timezone.utc))
                while next_event < len(events) and events[next_event][0] <= t:
                    _, topic, key, payload = events[next_event]
                    send(topic, key, {"time": now, **payload})
                    next_event += 1

                sample = {
                    "time": now, "driver": driver,
                    "speed": row["speed"], "throttle": row["throttle"], "brake": row["brake"],
                    "gear": row["n_gear"], "rpm": row["rpm"],
                    "tyre_temp": None,  # not provided by OpenF1
                    "tyre_wear": tyres.wear(row["driver_number"], t),
                }
                send("telemetry.raw", driver, sample)
                send("telemetry.latest", driver, sample)  # compacted: last state per driver
                SPEED_G.labels(driver).set(row["speed"])
    except KeyboardInterrupt:
        pass
    finally:
        log.info("flushing")
        producer.flush(10)


if __name__ == "__main__":
    main()
