"""Checks on the ML lap-time model. Run with: python -m pytest predictor/test_ml.py"""
import numpy as np
from sklearn.ensemble import HistGradientBoostingRegressor

import ml


def test_row_uses_the_last_three_laps_as_baseline():
    f, base = ml.row(5, 10, 50, 1, [91.0, 90.0, 90.5, 90.1])
    assert abs(base - (90.0 + 90.5 + 90.1) / 3) < 1e-9
    assert f[:3] == [5, 0.2, 1] and abs(f[3] - (90.1 - base)) < 1e-9


def test_row_needs_two_laps():
    assert ml.row(3, 4, 50, 1, [90.0]) is None


def test_clean_drops_a_safety_car_lap():
    assert ml.clean([90.0, 90.2, 89.9, 130.0]) == [90.0, 90.2, 89.9]


def test_compound_code_defaults_to_medium():
    assert ml.compound_code("SOFT") == 0 and ml.compound_code("hard") == 2 and ml.compound_code(None) == 1


def test_model_learns_that_old_tyres_are_slower():
    rng = np.random.default_rng(0)
    age = rng.integers(1, 35, 4000)
    X = np.column_stack([age, rng.random(4000), rng.integers(0, 3, 4000), rng.normal(0, 0.2, 4000)])
    y = 0.08 * age + rng.normal(0, 0.1, 4000)  # each lap of tyre age costs 0.08 s
    est = HistGradientBoostingRegressor(random_state=0, categorical_features=[2]).fit(X, y)
    m = ml.LapModel(est, {"mae": 0.1})
    assert m.slope(30, 20, 50, 1) > m.slope(3, 20, 50, 1)
    assert abs(m.next_lap(10, 20, 50, 1, [90.0, 90.0, 90.0]) - (90.0 + 0.8)) < 0.3


def test_load_without_a_model_file_returns_none(tmp_path):
    assert ml.LapModel.load(str(tmp_path / "missing.joblib")) is None
