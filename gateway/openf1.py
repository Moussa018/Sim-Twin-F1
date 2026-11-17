import time

import requests

BASE = "https://api.openf1.org/v1"
MIN_INTERVAL = 0.4 


class OpenF1:
    def __init__(self):
        self.http = requests.Session()
        self._last = 0.0

    def get(self, endpoint, **params):
        for attempt in range(5):
            wait = MIN_INTERVAL - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()
            r = self.http.get(f"{BASE}/{endpoint}", params=params, timeout=30)
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(2 * (attempt + 1))
                continue
            if r.status_code == 404:  
                return []
            r.raise_for_status()
            return r.json()
        raise RuntimeError(f"OpenF1 {endpoint}: too many retries")
