# Parallel Processing Utilities

import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

logger = logging.getLogger(__name__)

class RateLimiter:
    """Simple rate limiter using token bucket algorithm."""

    def __init__(self, calls_per_second: float = 5.0):
        self.min_interval = 1.0 / calls_per_second
        self.last_call = 0.0
        self.lock = Lock()

    def wait(self):
        """Block until rate limit allows next call"""
        with self.lock:
            now = time.time()
            elapsed = now - self.last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_call = time.time()

def parallel_map(func, items, max_workers: int = 5, rate_limit: float = 5.0):
    """Execute func on each item in parallel with rate limiting.
    
    Args:
        func: Function to call on each item
        items: Iterable of items to process
        max_workers: Number of parallel threads
        rate_limit: Max calls per second
    
    Returns:
        List of (item, result) tuples"""
    limiter = RateLimiter(rate_limit)
    results = []

    def rate_limited_call(item):
        limiter.wait()
        return item, func(item)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(rate_limited_call, item): item for item in items}
            
        for future in as_completed(futures):
            item, result = future.result()
            results.append((item, result))
    
    return results