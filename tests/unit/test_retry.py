#Unit Tests

import pytest
from credit_markets.utils.retry import retry


def test_succeeds_on_first_try():
    """Function succeeds immediately, no retry needed"""

    @retry(max_attempts=3)
    def always_works():
        return "success"
    
    result = always_works()
    assert result == "success"


def test_retries_then_succeeds():
    """Function fails twice, then succeeds on third try"""

    call_count = 0

    @retry(max_attempts=3, base_delay=0.01)
    def fails_twice():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ValueError("temporary failure")
        return "success"

    result = fails_twice()
    assert result == "success"
    assert call_count == 3


def test_raises_after_max_attempts():
    """Function fails all attempts, raise exception"""

    call_count = 0

    @retry(max_attempts=3, base_delay=0.01)
    def always_fails():
        nonlocal call_count
        call_count +=1
        raise ValueError("permanent failure")
    
    with pytest.raises(ValueError):
        always_fails()

    assert call_count == 3


def test_only_catches_specified_exceptions():
    """Only retries on specified exception types"""

    call_count = 0

    @retry(max_attempts=3, base_delay=0.01, exceptions=(ValueError,))
    def raise_type_error():
        nonlocal call_count
        call_count +=1
        raise TypeError("different error")

    with pytest.raises(TypeError):
        raise_type_error()

    assert call_count == 1