"""Checks on the strategy model. Run with: python -m pytest predictor/test_strategy.py"""
import strategy as s

BASE = dict(pace=90.0, slope=0.05, slope_se=0.01, age=10, remaining=30, next_lap=21,
            pit_loss=22.0, p_sc=0.03, seed=1)


def test_fit_pace_recovers_a_clean_trend():
    laps = [(n, 90 + 0.1 * n) for n in range(1, 9)]
    pace, slope, se, nxt = s.fit_pace(laps)
    assert abs(slope - 0.1) < 1e-9 and nxt == 9 and abs(pace - 90.9) < 1e-9 and se < 1e-9


def test_fit_pace_ignores_a_safety_car_lap():
    laps = [(n, 90 + 0.1 * n) for n in range(1, 8)] + [(8, 130.0)]
    _, slope, _, nxt = s.fit_pace(laps)
    assert abs(slope - 0.1) < 1e-9 and nxt == 8


def test_fit_pace_needs_four_clean_laps():
    assert s.fit_pace([(1, 90), (2, 91), (3, 92)]) is None


def test_worn_tyres_and_long_race_recommend_a_stop():
    r = s.simulate(**{**BASE, "slope": 0.25, "age": 25, "remaining": 35})
    assert r["best_lap"] > 0 and r["gain_s"] > 0 and r["win_prob"] > 0.5


def test_no_degradation_means_staying_out():
    r = s.simulate(**{**BASE, "slope": -s.FUEL_GAIN, "slope_se": 0.0, "age": 2, "remaining": 10})
    assert r["best_lap"] == 0 and r["gain_s"] == 0


def test_same_seed_same_answer():
    assert s.simulate(**BASE) == s.simulate(**BASE)


def test_curve_values_are_sane():
    r = s.simulate(**BASE)
    assert all(0 <= p <= 1 for _, _, p in r["curve"])
    laps = [c[0] for c in r["curve"]]
    assert laps == sorted(laps) and laps[0] == BASE["next_lap"]


def test_cheaper_stops_under_safety_cars():
    calm = s.simulate(**{**BASE, "p_sc": 0.0})
    busy = s.simulate(**{**BASE, "p_sc": 0.25})
    # with safety cars around, stopping is on average less costly
    assert max(c[1] for c in busy["curve"]) > max(c[1] for c in calm["curve"])


def test_too_few_laps_left():
    assert s.simulate(**{**BASE, "remaining": 1}) is None
