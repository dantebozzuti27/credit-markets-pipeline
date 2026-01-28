"""
Unit tests for circuit breaker pattern.
"""

import time

import pytest

from credit_markets.ingestion.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerError,
    CircuitState,
)


@pytest.mark.unit
class TestCircuitBreaker:
    """Tests for CircuitBreaker class."""
    
    def test_starts_closed(self):
        """Circuit should start in CLOSED state."""
        breaker = CircuitBreaker("test", failure_threshold=3, recovery_timeout=1)
        assert breaker.state == CircuitState.CLOSED
        assert breaker.is_closed is True
    
    def test_opens_after_threshold_failures(self):
        """Circuit should open after N consecutive failures."""
        breaker = CircuitBreaker("test", failure_threshold=3, recovery_timeout=1)
        
        @breaker
        def failing_func():
            raise RuntimeError("Simulated failure")
        
        # First 2 failures - still closed
        for _ in range(2):
            with pytest.raises(RuntimeError):
                failing_func()
        assert breaker.state == CircuitState.CLOSED
        
        # Third failure - opens circuit
        with pytest.raises(RuntimeError):
            failing_func()
        assert breaker.state == CircuitState.OPEN
    
    def test_rejects_requests_when_open(self):
        """Open circuit should reject requests immediately."""
        breaker = CircuitBreaker("test", failure_threshold=1, recovery_timeout=10)
        
        @breaker
        def failing_func():
            raise RuntimeError("Simulated failure")
        
        # Open the circuit
        with pytest.raises(RuntimeError):
            failing_func()
        
        # Subsequent calls should fail fast
        with pytest.raises(CircuitBreakerError) as exc_info:
            failing_func()
        
        assert "is OPEN" in str(exc_info.value)
        assert exc_info.value.time_remaining > 0
    
    def test_half_opens_after_recovery_timeout(self):
        """Circuit should half-open after recovery timeout."""
        breaker = CircuitBreaker("test", failure_threshold=1, recovery_timeout=1)
        
        call_count = 0
        
        @breaker
        def tracked_func():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("First call fails")
            return "success"
        
        # Open the circuit
        with pytest.raises(RuntimeError):
            tracked_func()
        assert breaker.state == CircuitState.OPEN
        
        # Wait for recovery timeout
        time.sleep(1.1)
        
        # Next call should succeed (half-open allows one request)
        result = tracked_func()
        assert result == "success"
        assert breaker.state == CircuitState.CLOSED
    
    def test_closes_on_successful_half_open_request(self):
        """Successful request in half-open state should close circuit."""
        breaker = CircuitBreaker("test", failure_threshold=1, recovery_timeout=0)
        
        # Manually set to half-open
        breaker._state = CircuitState.HALF_OPEN
        
        @breaker
        def success_func():
            return "success"
        
        result = success_func()
        assert result == "success"
        assert breaker.state == CircuitState.CLOSED
    
    def test_reopens_on_failed_half_open_request(self):
        """Failed request in half-open state should reopen circuit."""
        breaker = CircuitBreaker("test", failure_threshold=1, recovery_timeout=10)
        
        # Manually set to half-open
        breaker._state = CircuitState.HALF_OPEN
        
        @breaker
        def failing_func():
            raise RuntimeError("Still failing")
        
        with pytest.raises(RuntimeError):
            failing_func()
        
        assert breaker.state == CircuitState.OPEN
    
    def test_manual_reset(self):
        """reset() should force circuit to closed state."""
        breaker = CircuitBreaker("test", failure_threshold=1, recovery_timeout=100)
        
        @breaker
        def failing_func():
            raise RuntimeError("Fail")
        
        # Open the circuit
        with pytest.raises(RuntimeError):
            failing_func()
        assert breaker.state == CircuitState.OPEN
        
        # Manually reset
        breaker.reset()
        assert breaker.state == CircuitState.CLOSED
    
    def test_success_resets_failure_count(self):
        """Successful request should reset failure counter."""
        breaker = CircuitBreaker("test", failure_threshold=3, recovery_timeout=1)
        
        call_count = 0
        
        @breaker
        def intermittent_func():
            nonlocal call_count
            call_count += 1
            if call_count % 2 == 1:
                raise RuntimeError("Odd call fails")
            return "success"
        
        # Fail, succeed, fail, succeed - should not open
        with pytest.raises(RuntimeError):
            intermittent_func()
        intermittent_func()  # success resets counter
        with pytest.raises(RuntimeError):
            intermittent_func()
        intermittent_func()
        
        assert breaker.state == CircuitState.CLOSED
