# from functools import wraps
import functools
import logging
from typing import Callable, Any
import time

logger = logging.getLogger(__name__)


def retry(retries: int = 3, delay: float = 1) -> Callable:
    """
    Attempt to call a function, if it fails, try again with a specified delay.
    :param retries: The max amount of retries you want for the function call
    :param delay: The delay (in seconds) between each function retry
    :return:
    """

    # Don't let the user use this decorator if they are high
    if retries < 1 or delay <= 0:
        raise ValueError("Invalid retry configuration: retries must be >= 1 and delay must be > 0")

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            for i in range(
                1, retries + 1
            ):  # 1 to retries + 1 since upper bound is exclusive
                try:
                    logger.info(f"Running ({i}): {func.__name__}()")
                    return func(*args, **kwargs)
                except Exception as e:
                    # Break out of the loop if the max amount of retries is exceeded
                    if i == retries:
                        logger.error(f"Error: {repr(e)}.")
                        logger.error(
                            f'"{func.__name__}()" failed after {retries} retries.'
                        )
                        break
                    else:
                        logger.info(f"Error: {repr(e)} -> Retrying...")
                        time.sleep(
                            delay
                        )  # Add a delay before running the next iteration

        return wrapper

    return decorator
