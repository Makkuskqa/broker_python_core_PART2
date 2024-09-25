import logging

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        filename='ib_bot.log'
    )

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.ERROR)  # Set to ERROR to match your previous intention
    return logger




logger = get_logger(__name__)

import time
import functools
def log_time(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.info(f">>>>>>>>>>>>Starting {func.__name__}...<<<<<<<<<<<<<<<<")
        result = func(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"<< Finished Function: {func.__name__} in {duration:.4f} seconds. >>")
        return result
    return wrapper
