"""
Unit tests for rate limiter.
"""

import time

import pytest

from credit_markets.ingestion.rate_limiter import RateLimiter


@pytest.mark.unit
class TestRateLimiter:
    """Tests for RateLimiter class."""
    
    def test_allows_burst_requests(self):
        """Should allow burst of requests up to bucket capacity."""
        limiter = RateLimiter("test", rate=1.0, burst=5)
        
        # Should be able to make 5 requests immediately
        for _ in range(5):
            assert limiter.acquire(blocking=False) is True
        
        # 6th request should fail (non-blocking)
        assert limiter.acquire(blocking=False) is False
    
    def test_refills_tokens_over_time(self):
        """Tokens should refill based on rate."""
        limiter = RateLimiter("test", rate=10.0, burst=5)  # 10 tokens/sec
        
        # Consume all tokens
        for _ in range(5):
            limiter.acquire(blocking=False)
        
        # Wait for refill (0.5 sec = 5 tokens at 10/sec)
        time.sleep(0.5)
        
        # Should have tokens again
        assert limiter.acquire(blocking=False) is True
    
    def test_blocking_acquire_waits(self):
        """Blocking acquire should wait for tokens."""
        limiter = RateLimiter("test", rate=10.0, burst=1)  # Fast for testing
        
        # Consume the only token
        limiter.acquire(blocking=False)
        
        # Blocking acquire should wait and succeed
        start = time.time()
        limiter.acquire(blocking=True)
        elapsed = time.time() - start
        
        # Should have waited ~0.1 seconds (1 token at 10/sec)
        assert elapsed >= 0.05  # Allow some margin
    
    def test_decorator_rate_limits(self):
        """Decorator should apply rate limiting."""
        limiter = RateLimiter("test", rate=100.0, burst=2)  # Fast
        
        call_count = 0
        
        @limiter
        def tracked_func():
            nonlocal call_count
            call_count += 1
            return call_count
        
        # First 2 calls should be immediate
        results = [tracked_func() for _ in range(2)]
        assert results == [1, 2]
    
    def test_respects_burst_capacity(self):
        """Should not exceed burst capacity."""
        limiter = RateLimiter("test", rate=100.0, burst=3)
        
        # Fill beyond capacity (wait for refill)
        time.sleep(0.1)
        
        # Should only have burst capacity (3), not more
        for _ in range(3):
            assert limiter.acquire(blocking=False) is True
        
        # 4th should fail
        assert limiter.acquire(blocking=False) is False
