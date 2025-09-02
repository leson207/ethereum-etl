import logging
import sys
from datetime import datetime

from loguru import logger as loguru_logger

from src.configs.environment import env

logger_name = "App"
timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
log_file = env._local_log_folder / f"{logger_name}-{timestamp}.log"


def create_logging_logger():
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG if env.DEBUG_MODE else logging.INFO)

    file_handler = logging.FileHandler(log_file)
    stdout_handler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter(
        "[%(asctime)s - %(name)s - %(levelname)s] - %(message)s"
    )

    file_handler.setFormatter(formatter)
    stdout_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stdout_handler)
    logger.info(f"{logger_name} logger created")

    return logger


def create_loguru_logger():
    # Remove default handler
    loguru_logger.remove()

    # File handler
    loguru_logger.add(
        log_file,
        format="[<green>{time:YYYY-MM-DD HH:mm:ss}</green> - <cyan>{name}</cyan> - <level>{level}</level>] - {message}",
        level="DEBUG" if env.DEBUG_MODE else "INFO",
        enqueue=False,
        rotation="10 MB",
        retention="10 days",
        backtrace=False,
        diagnose=False,
    )

    # Stdout handler
    loguru_logger.add(
        sink=sys.stdout,
        format="[<green>{time:HH:mm:ss}</green> - <cyan>{name}</cyan> - <level>{level}</level>] - {message}",
        level="DEBUG" if env.DEBUG_MODE else "INFO",
    )

    loguru_logger.info(f"{logger_name} logger created")
    return loguru_logger


logger = create_loguru_logger()
