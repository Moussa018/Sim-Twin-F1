"""Monte Carlo pit-stop strategy.

For one driver it simulates the rest of the race thousands of times and compares
"stay out" with "pit at the end of lap N" for many values of N. All strategies
are run on the same random draws (same safety cars, same tyre degradation), so
the comparison is paired and a small expected gain is still measurable.

What is random in each run:
  tyre degradation  drawn around the fitted trend, wide when the fit is uncertain
  safety cars       each lap may start one (rate learned from the session), it
                    lasts SC_LAPS laps, slows everyone and makes a stop cheaper

Model per lap, with i = laps from now and age = laps on the current tyres:
  lap time = pace - FUEL_GAIN * i + deg * (age_i - age_1) + cliff(age_i)
A stop adds the pit loss and resets age_i to 0 for the laps that follow.

Limits: only the driver's own race time is modelled. Traffic, gaps to other
cars and the two-compound rule are not, so read the result as "which lap
costs the least time", not as a finishing position.
"""
import math
import zlib

import numpy as np

FUEL_GAIN = 0.035     # s per lap the car gets faster as fuel burns
SC_LAPS = 3           # laps a safety car period lasts
SC_SLOWDOWN = 0.35    # lap time is 35 % longer under a safety car
SC_PIT_FACTOR = 0.55  # a stop under a safety car costs about half
CLIFF = 0.02          # s per lap^2 once the tyre is past its limit
PRIOR_SC_RATE = 0.03  # safety cars per lap before the session tells us anything
PRIOR_WEIGHT = 30     # how many laps that prior is worth


def fit_pace(laps):
    """Fit lap time against lap number on clean laps.

    laps: iterable of (lap_number, seconds). Slow outliers (traffic, safety car)
    are dropped like in the predictor. Returns (pace_next, slope, slope_se, next_lap)
    or None when there are fewer than 4 clean laps.
    """
    laps = list(laps)
    if len(laps) < 4:
        return None
    med = float(np.median([d for _, d in laps]))
    clean = [(n, d) for n, d in laps if d <= med * 1.12]
    if len(clean) < 4:
        return None
    x = np.array([n for n, _ in clean], dtype=float)
    y = np.array([d for _, d in clean], dtype=float)
    mx, my = x.mean(), y.mean()
    sxx = float(((x - mx) ** 2).sum())
    if sxx == 0:
        return None
    slope = float(((x - mx) * (y - my)).sum() / sxx)
    resid = y - (my + slope * (x - mx))
    sigma = math.sqrt(float((resid ** 2).sum()) / (len(x) - 2))
    next_lap = int(x.max()) + 1
    return my + slope * (next_lap - mx), slope, sigma / math.sqrt(sxx), next_lap


def sc_rate(deployments, laps_elapsed):
    """Safety cars per lap: the session's own count blended with a prior."""
    return (deployments + PRIOR_SC_RATE * PRIOR_WEIGHT) / (laps_elapsed + PRIOR_WEIGHT)


def simulate(*, pace, slope, slope_se, age, remaining, next_lap, pit_loss, p_sc,
             sc_now=False, age_limit=40.0, runs=1000, max_candidates=14, seed=None):
    """Compare stopping at different laps over `remaining` laps starting at `next_lap`.

    Returns a dict:
      best_lap   lap number of the cheapest stop, 0 when staying out is cheapest
      gain_s     expected seconds saved by the best choice over staying out
      win_prob   share of runs where the best stop beats staying out
      curve      [[lap, expected seconds saved vs staying out, win share], ...]
      deg, sc_rate  the degradation (s/lap) and safety car rate that were used
    """
    rng = np.random.default_rng(seed)
    L = int(remaining)
    deg_mean = max(slope + FUEL_GAIN, 0.01)  # the observed slope hides the fuel effect
    deg = np.maximum(rng.normal(deg_mean, max(slope_se, 0.1 * deg_mean), runs), 0.005)

    starts = rng.random((runs, L)) < p_sc
    sc = starts.copy()
    for k in range(1, SC_LAPS):
        sc[:, k:] |= starts[:, :-k]
    if sc_now:
        sc[:, :2] = True

    i = np.arange(L, dtype=float)
    last = L - 2  # a stop on the last lap can only lose time
    if last < 0:
        return None
    if last + 1 <= max_candidates:
        stops = list(range(last + 1))
    else:
        stops = sorted({int(round(v)) for v in np.linspace(0, last, max_candidates)})

    def total(j):
        """Race time of every run when stopping at the end of lap index j (None = never)."""
        pit = L if j is None else j
        ages = np.where(i <= pit, age + 1 + i, i - pit)
        wear = deg[:, None] * (ages - (age + 1))[None, :] + CLIFF * np.maximum(0.0, ages - age_limit) ** 2
        lap = (pace - FUEL_GAIN * i)[None, :] + wear
        lap = lap * (1 + SC_SLOWDOWN * sc)
        if j is not None:
            lap[:, j] += pit_loss * np.where(sc[:, j], SC_PIT_FACTOR, 1.0)
        return lap.sum(axis=1)

    stay = total(None)
    results = [(j, total(j)) for j in stops]
    curve = []
    for j, t in results:
        curve.append([next_lap + j, float((stay - t).mean()), float((t < stay).mean())])
    best = max(range(len(curve)), key=lambda k: curve[k][1])
    if curve[best][1] <= 0:
        return {"best_lap": 0, "gain_s": 0.0, "win_prob": 0.0, "curve": curve,
                "deg": float(deg_mean), "sc_rate": p_sc}
    return {"best_lap": curve[best][0], "gain_s": curve[best][1], "win_prob": curve[best][2],
            "curve": curve, "deg": float(deg_mean), "sc_rate": p_sc}


def seed_for(driver, lap):
    """Same driver and lap always give the same simulation, so results are reproducible."""
    return zlib.crc32(f"{driver}:{lap}".encode())
