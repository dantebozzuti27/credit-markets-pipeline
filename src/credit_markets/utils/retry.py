#Retry Decorator with Exponential Backoff

import time
import logging
from functools import wraps

logger = logging.getLogger(__name__)


def retry(max_attempts: int = 3, base_delay: float = 1.0, exceptions: tuple = (Exception,)):
    """
    Retry decorator with exponential backoff.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        delay = base_delay * (2 ** attempt)
                        logger.warning(f"{func.__name__} failed (attempt {attempt + 1}/{max_attempts}): {e}. Retrying in {delay}s")
                        time.sleep(delay)
                    else:
                        logger.error(f"{func.__name__} failed after {max_attempts} attempts: {e}")

            raise last_exception
        
        return wrapper

    return decorator