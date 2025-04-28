from utils.logger_config import get_sga_logger
logger = get_sga_logger()

def log_exceptions(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            logger.error(f"Error in {func.__name__}: {exc}", exc_info=True)
            raise
    return wrapper



