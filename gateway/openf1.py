"""Small client for the OpenF1 REST API (https://openf1.org)."""
import time

import requests

BASE = "https://api.openf1.org/v1"
MIN_INTERVAL = 0.4  # seconds between calls, the free tier allows ~3 requests/s


class OpenF1:
    def __init__(self):
        self.http = requests.Session()
        self._last = 0.0

    def get(self, endpoint, **params):
        """GET /endpoint and return the JSON list. Retries on 429 / 5xx."""
        for attempt in range(5):
            wait = MIN_INTERVAL - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()
            r = self.http.get(f"{BASE}/{endpoint}", params=params, timeout=30)
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(2 * (attempt + 1))
                continue
            if r.status_code == 404:  # OpenF1 answers 404 when nothing matches
                return []
            r.raise_for_status()
            return r.json()
        raise RuntimeError(f"OpenF1 {endpoint}: too many retries")
