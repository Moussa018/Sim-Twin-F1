"""Train the lap-time model on real races from OpenF1.

  python train.py --year 2024 --year 2025 --max-sessions 20

Builds one sample per clean lap (features from the laps before it, target = how much
the lap differs from the recent pace), holds out whole races for validation, compares
the model with the linear-trend baseline the predictor used before, and saves it to
predictor/model/lap_model.joblib. Needs internet (OpenF1) and scikit-learn.
"""
import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone

import numpy as np
import requests

import ml

log = logging.getLogger("train")
BASE = "https://api.openf1.org/v1"


def get(endpoint, **params):
    for attempt in range(6):
        r = requests.get(f"{BASE}/{endpoint}", params=params, timeout=60)
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(2 * (attempt + 1))
            continue
        if r.status_code == 404:
            return []
        r.raise_for_status()
        time.sleep(0.4)
        return r.json()
    raise RuntimeError(f"OpenF1 {endpoint}: too many retries")


def session_samples(laps, stints):
    """Samples of one race: (features, target, baseline, actual, linear_forecast)."""
    total = max((l["lap_number"] for l in laps), default=0)
    stint_of = {}
    for st in stints:
        for n in range(st["lap_start"], (st["lap_end"] or total) + 1):
            stint_of[(st["driver_number"], n)] = (
                st.get("compound"), (st.get("tyre_age_at_start") or 0) + n - st["lap_start"], st["stint_number"])
    by_driver = {}
    for l in laps:
        by_driver.setdefault(l["driver_number"], []).append(l)
    out = []
    for driver, items in by_driver.items():
        items.sort(key=lambda l: l["lap_number"])
        history, cur = [], None  # clean laps of the current stint: (lap_number, seconds)
        for l in items:
            info = stint_of.get((driver, l["lap_number"]))
            if not info or not l.get("lap_duration"):
                continue
            compound, age, stint = info
            if stint != cur:
                history, cur = [], stint
            if l.get("is_pit_out_lap"):
                continue
            recent = [d for _, d in history]
            if len(recent) >= 2 and age >= 1:
                if l["lap_duration"] in ml.clean(recent + [l["lap_duration"]]):  # skip slow target laps
                    r = ml.row(age, l["lap_number"], total, ml.compound_code(compound), ml.clean(recent))
                    lin = ml.forecast_lap(history)
                    if r and lin:
                        f, base = r
                        out.append((f, l["lap_duration"] - base, base, l["lap_duration"], lin[0]))
            history.append((l["lap_number"], l["lap_duration"]))
            history = history[-8:]
    return out


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, action="append", help="season to train on (repeatable)")
    ap.add_argument("--max-sessions", type=int, default=20)
    ap.add_argument("--holdout", type=float, default=0.25, help="share of races kept for validation")
    ap.add_argument("--out", default=ml.MODEL_PATH)
    a = ap.parse_args()
    years = a.year or [datetime.now(timezone.utc).year - 1]

    now = datetime.now(timezone.utc).isoformat()
    sessions = []
    for y in years:
        sessions += [s for s in get("sessions", year=y, session_name="Race") if s["date_end"] < now]
    sessions = sessions[-a.max_sessions:]
    if not sessions:
        sys.exit("no finished races found")

    data = []  # (session_key, samples)
    for s in sessions:
        sk = s["session_key"]
        try:
            samples = session_samples(get("laps", session_key=sk), get("stints", session_key=sk))
        except Exception as e:
            log.warning("session %s skipped: %s", sk, e)
            continue
        log.info("%s %s: %d samples", s["location"], s["year"], len(samples))
        if samples:
            data.append((sk, samples))
    if len(data) < 4:
        sys.exit("need at least 4 usable races")

    order = np.random.default_rng(0).permutation(len(data))
    n_val = max(1, int(len(data) * a.holdout))
    val = [data[i] for i in order[:n_val]]
    train = [data[i] for i in order[n_val:]]

    def flat(part):
        return [x for _, s in part for x in s]

    tr, va = flat(train), flat(val)
    X, y = np.array([s[0] for s in tr], float), np.array([s[1] for s in tr])
    Xv, yv = np.array([s[0] for s in va], float), np.array([s[1] for s in va])

    from sklearn.ensemble import HistGradientBoostingRegressor
    est = HistGradientBoostingRegressor(loss="absolute_error", max_depth=4, learning_rate=0.05, max_iter=300, l2_regularization=1.0,
                                        categorical_features=[ml.FEATURES.index("compound")], random_state=0)
    est.fit(X, y)
    mae = float(np.abs(est.predict(Xv) - yv).mean())
    naive = float(np.abs(yv).mean())                                    # "same as recent pace"
    linear = float(np.abs(np.array([s[4] - s[3] for s in va])).mean())  # old linear-trend forecast
    log.info("validation on %d unseen races, %d laps", len(val), len(va))
    log.info("MAE  ML model %.3f s | linear trend %.3f s | recent pace %.3f s", mae, linear, naive)

    est.fit(np.vstack([X, Xv]), np.concatenate([y, yv]))  # final model uses every race
    import joblib
    meta = {"mae": mae, "linear_mae": linear, "naive_mae": naive, "n_train": len(tr) + len(va),
            "sessions": [sk for sk, _ in data], "trained_at": now}
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    joblib.dump({"estimator": est, "meta": meta}, a.out)
    log.info("saved %s", a.out)


if __name__ == "__main__":
    main()
