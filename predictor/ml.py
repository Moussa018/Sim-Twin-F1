"""Machine learning lap-time model, shared by train.py and predictor.py.

The model predicts how much slower or faster the NEXT lap is than the driver's recent
pace (the mean of the last 3 clean laps of the stint). Predicting a delta instead of a
raw lap time makes the model circuit independent, so one model trained on many races
works on any track.

Features (see FEATURES):
  tyre_age   laps on the tyres the lap will be driven on
  frac_race  lap number / race length (a proxy for fuel load, 0.5 when unknown)
  compound   0 soft, 1 medium, 2 hard (categorical)
  trend      last clean lap minus the mean of the last 3 (momentum)
"""
import os
import statistics

import numpy as np

FEATURES = ["tyre_age", "frac_race", "compound", "trend"]
COMPOUNDS = {"SOFT": 0, "MEDIUM": 1, "HARD": 2}
HISTORY = 3              # clean laps used for the driver's recent pace
SLOW_FACTOR = 1.06       # laps slower than this times the median are not clean
MODEL_PATH = os.getenv("MODEL_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "model", "lap_model.joblib"))


def compound_code(name):
    """OpenF1 compound name to a number; unknown compounds count as medium."""
    return COMPOUNDS.get((name or "").upper(), 1)


def row(tyre_age, lap_number, total_laps, compound, recent):
    """Feature row and baseline pace from the clean laps `recent` (oldest first).

    Returns (features, baseline) or None with fewer than 2 laps of history.
    """
    if len(recent) < 2:
        return None
    last = recent[-HISTORY:]
    base = sum(last) / len(last)
    frac = lap_number / total_laps if total_laps else 0.5
    return [tyre_age, frac, compound, recent[-1] - base], base


def clean(durations):
    """Drop slow outliers (safety car, traffic) relative to the median."""
    if len(durations) < 3:
        return list(durations)
    med = statistics.median(durations)
    return [d for d in durations if d <= med * SLOW_FACTOR]


def forecast_lap(laps):
    """Baseline without ML: linear trend over recent laps. laps = [(lap_number, seconds)]."""
    if len(laps) < 3:
        return None
    med = statistics.median(d for _, d in laps)
    c = [(n, d) for n, d in laps if d <= med * SLOW_FACTOR]
    if len(c) < 3:
        return None
    xs, ys = [n for n, _ in c], [d for _, d in c]
    mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
    var = sum((x - mx) ** 2 for x in xs)
    slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / var if var else 0.0
    next_n = max(xs) + 1
    return my + slope * (next_n - mx), slope, next_n


class LapModel:
    """A trained gradient boosting model plus what we know about its accuracy."""

    def __init__(self, estimator, meta):
        self.est = estimator
        self.meta = meta  # mae, linear_mae, naive_mae, n_train, sessions, trained_at

    @classmethod
    def load(cls, path=MODEL_PATH):
        """The saved model, or None when it has not been trained yet."""
        try:
            import joblib
            blob = joblib.load(path)
            return cls(blob["estimator"], blob["meta"])
        except Exception:
            return None

    def delta(self, features):
        return float(self.est.predict(np.array([features], dtype=float))[0])

    def next_lap(self, tyre_age, lap_number, total_laps, compound, recent):
        """Forecast of the next lap in seconds, or None without enough history."""
        r = row(tyre_age, lap_number, total_laps, compound, recent)
        if not r:
            return None
        f, base = r
        return base + self.delta(f)

    def slope(self, tyre_age, lap_number, total_laps, compound):
        """Seconds per lap the car changes at this tyre age, with no momentum.

        The recent pace is centred about 2 laps behind the lap we predict, so the
        per-lap change is half the predicted delta.
        """
        frac = lap_number / total_laps if total_laps else 0.5
        return self.delta([tyre_age, frac, compound, 0.0]) / 2
