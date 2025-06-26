# log_config.py
import logging
import os
from datetime import datetime


def get_logger(name: str):
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return logger

    logger.setLevel(logging.INFO)

    # Create logs directory if it doesn't exist
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    # Date-based log file
    date_str = datetime.now().strftime("%Y-%m-%d")
    file_handler = logging.FileHandler(os.path.join(log_dir, f"{date_str}.log"), mode='a')
    file_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler.setFormatter(file_formatter)

    # Console handler
    stream_handler = logging.StreamHandler()
    stream_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    stream_handler.setFormatter(stream_formatter)

    # Add both handlers
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger
