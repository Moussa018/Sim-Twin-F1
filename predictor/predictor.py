"""Predictor: reads telemetry.raw and race.laps, publishes telemetry.predictions.

For every completed lap of a driver it publishes three predictions (key = driver):
  next_lap_time     forecast of the next lap in seconds (linear trend of recent clean laps)
  tyre_laps_left    laps until tyre wear reaches TYRE_LIMIT, from the recent wear rate
  pit_recommended   1 when the tyres are within PIT_WINDOW laps of the limit, else 0

A Monte Carlo strategy simulation (strategy.py) adds mc_best_pit_lap, mc_gain_s and
mc_win_prob once the race length is known (race.laps carries total_laps, or set TOTAL_LAPS).

Each message also carries a human readable "message" used by the 3D view.
It is a simple statistical model on purpose: replace predict() with a trained
model later without touching the Kafka plumbing.

Env: KAFKA_BROKERS, TYRE_LIMIT (80), PIT_WINDOW (5), METRICS_PORT (8002),
     TOTAL_LAPS (0 = from the producer), PIT_LOSS (22 s), WEAR_PER_LAP (2.0), MC_RUNS (1000)
"""
import json
import logging
import os
import statistics
from collections import deque
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer
from prometheus_client import Counter, Gauge, start_http_server

import strategy

log = logging.getLogger("predictor")

TYRE_LIMIT = float(os.getenv("TYRE_LIMIT", "80"))
PIT_WINDOW = float(os.getenv("PIT_WINDOW", "5"))
OUT_TOPIC = "telemetry.predictions"
TOTAL_LAPS = int(os.getenv("TOTAL_LAPS", "0"))          # race length, for live sessions
PIT_LOSS = float(os.getenv("PIT_LOSS", "22"))           # seconds a stop costs at this circuit
WEAR_PER_LAP = float(os.getenv("WEAR_PER_LAP", "2.0"))  # must match the producer's tyre model
MC_RUNS = int(os.getenv("MC_RUNS", "1000"))             # simulated races per driver and lap

PUBLISHED = Counter("f1_predictions_total", "Predictions published.", ["metric"])
MC_GAIN = Gauge("f1_mc_gain_seconds", "Expected seconds saved by the best stop (Monte Carlo).", ["driver"])
LAPS_LEFT = Gauge("f1_tyre_laps_left", "Predicted laps until the tyre limit.", ["driver"])
NEXT_LAP = Gauge("f1_next_lap_time_seconds", "Predicted next lap time.", ["driver"])


class Driver:
    def __init__(self):
        self.wear = None
        self.age = None               # laps on the current tyres
        self.laps = deque(maxlen=8)   # (lap_number, duration) of clean laps
        self.wears = deque(maxlen=4)  # (lap_number, wear) at the end of recent laps


class Race:
    """What the whole field shares: laps run, total laps, safety cars."""

    def __init__(self):
        self.lap = 0                  # furthest lap seen
        self.total = TOTAL_LAPS or None
        self.sc_deployed = 0
        self.sc_active = False

    def event(self, data):
        msg = (data.get("message") or "").upper()
        if "SAFETY CAR" in msg and "VIRTUAL" not in msg:
            if "DEPLOYED" in msg:
                self.sc_deployed += 1
                self.sc_active = True
            elif "IN THIS LAP" in msg:
                self.sc_active = False
        elif "TRACK CLEAR" in msg or "GREEN LIGHT" in msg:
            self.sc_active = False


def monte_carlo(driver, st, race, lap):
    """Run the strategy simulation after a lap; returns a list of (metric, value, message, extra)."""
    if not race.total or st.age is None or st.wear is None:
        return []
    fit = strategy.fit_pace(st.laps)
    if not fit:
        return []
    pace, slope, se, next_lap = fit
    remaining = race.total - next_lap + 1
    if remaining < 3:
        return []
    p_sc = strategy.sc_rate(race.sc_deployed, max(race.lap, 1))
    r = strategy.simulate(pace=pace, slope=slope, slope_se=se, age=st.age, remaining=remaining,
                          next_lap=next_lap, pit_loss=PIT_LOSS, p_sc=p_sc, sc_now=race.sc_active,
                          age_limit=TYRE_LIMIT / WEAR_PER_LAP, runs=MC_RUNS,
                          seed=strategy.seed_for(driver, lap))
    if not r:
        return []
    if r["best_lap"]:
        msg = (f"Monte Carlo, {MC_RUNS} runs: stop on lap {r['best_lap']} saves {r['gain_s']:.1f} s "
               f"over staying out ({r['win_prob'] * 100:.0f}% of runs). Safety car risk {p_sc * 100:.1f}% per lap.")
    else:
        msg = f"Monte Carlo, {MC_RUNS} runs: staying out is the cheapest option. Safety car risk {p_sc * 100:.1f}% per lap."
    curve = [[n, round(g, 2), round(p, 3)] for n, g, p in r["curve"]]
    return [("mc_best_pit_lap", r["best_lap"], msg, {"curve": curve, "total_laps": race.total}),
            ("mc_gain_s", round(r["gain_s"], 2), None, {}),
            ("mc_win_prob", round(r["win_prob"], 3), None, {})]


def fmt_time(seconds):
    m, s = divmod(seconds, 60)
    return f"{int(m)}:{s:06.3f}"


def forecast_lap(laps):
    """Linear trend over recent laps, ignoring slow outliers (safety car, traffic)."""
    if len(laps) < 3:
        return None
    med = statistics.median(d for _, d in laps)
    clean = [(n, d) for n, d in laps if d <= med * 1.12]
    if len(clean) < 3:
        return None
    xs, ys = [n for n, _ in clean], [d for _, d in clean]
    mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
    var = sum((x - mx) ** 2 for x in xs)
    slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / var if var else 0.0
    next_n = max(xs) + 1
    return my + slope * (next_n - mx), slope, next_n


def tyre_rate(wears):
    """Wear per lap over the recent laps; None if unknown or the tyres were just changed."""
    if len(wears) < 2:
        return None
    (n0, w0), (n1, w1) = wears[0], wears[-1]
    if n1 <= n0 or w1 <= w0:
        return None
    return (w1 - w0) / (n1 - n0)


def predict(driver, state, lap):
    """Return a list of (metric, value, message) for one driver after a completed lap."""
    out = []
    f = forecast_lap(state.laps)
    if f:
        t, slope, n = f
        out.append(("next_lap_time", round(t, 3), f"Lap {n} forecast {fmt_time(t)} ({slope:+.2f} s per lap)."))
    rate = tyre_rate(state.wears)
    if rate and state.wear is not None:
        left = max(0.0, (TYRE_LIMIT - state.wear) / rate)
        out.append(("tyre_laps_left", round(left, 1),
                    f"Tyre wear {state.wear:.0f}%. About {left:.1f} laps until the {TYRE_LIMIT:.0f}% limit."))
        pit = 1 if left <= PIT_WINDOW else 0
        out.append(("pit_recommended", pit,
                    f"Pit window open. Recommend boxing within {left:.0f} laps." if pit
                    else "No stop needed yet."))
    return out


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    brokers = os.getenv("KAFKA_BROKERS", "localhost:9092")
    start_http_server(int(os.getenv("METRICS_PORT", "8002")))

    consumer = Consumer({"bootstrap.servers": brokers, "group.id": "f1-predictor",
                         "auto.offset.reset": "latest", "enable.auto.commit": True})
    consumer.subscribe(["telemetry.raw", "race.laps", "race.events"])
    producer = Producer({"bootstrap.servers": brokers, "enable.idempotence": True, "acks": "all"})
    drivers = {}
    log.info("predicting from telemetry.raw + race.laps -> %s", OUT_TOPIC)

    race = Race()
    try:
        while True:
            m = consumer.poll(1.0)
            producer.poll(0)
            if m is None or m.error():
                continue
            try:
                data = json.loads(m.value())
                if m.topic() == "race.events":
                    race.event(data)
                    continue
                driver = data["driver"]
            except (ValueError, KeyError):
                continue
            st = drivers.setdefault(driver, Driver())

            if m.topic() == "telemetry.raw":
                st.wear = data.get("tyre_wear", st.wear)
                continue

            lap, dur = data.get("lap_number"), data.get("lap_duration")
            if lap is None:
                continue
            race.lap = max(race.lap, lap)
            race.total = data.get("total_laps") or race.total
            new_tyres = False
            if st.wear is not None:
                if st.wears and st.wear < st.wears[-1][1] - 5:  # wear dropped: new tyres
                    st.wears.clear()
                    new_tyres = True
                st.wears.append((lap, st.wear))
            if data.get("is_pit_out") or new_tyres:
                st.age = 0
            elif st.age is None:
                st.age = round((st.wear or 0) / WEAR_PER_LAP)  # joined mid-stint: guess from wear
            else:
                st.age += 1
            if dur and not data.get("is_pit_out"):
                st.laps.append((lap, dur))

            now = datetime.now(timezone.utc).isoformat()
            results = [(*p, {}) for p in predict(driver, st, lap)]
            results += monte_carlo(driver, st, race, lap)
            for metric, value, message, extra in results:
                producer.produce(OUT_TOPIC, key=driver, value=json.dumps({
                    "time": now, "driver": driver, "metric": metric, "value": value,
                    "lap_number": lap, "message": message, **extra}))
                PUBLISHED.labels(metric).inc()
                if metric == "tyre_laps_left":
                    LAPS_LEFT.labels(driver).set(value)
                elif metric == "next_lap_time":
                    NEXT_LAP.labels(driver).set(value)
                elif metric == "mc_gain_s":
                    MC_GAIN.labels(driver).set(value)
            producer.poll(0)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.flush(5)


if __name__ == "__main__":
    main()
