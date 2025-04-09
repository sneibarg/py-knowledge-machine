import logging
from datetime import datetime
import os
from config import LOG_DIR


def setup_logging(log_type, debug=False):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(LOG_DIR, f"{log_type}_{timestamp}.log")

    handlers = [logging.FileHandler(log_file)]
    if debug:
        handlers.append(logging.StreamHandler())

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=handlers
    )
    return logging.getLogger()


def setup_batch_logger(item_type, process_id, timestamp, debug=False):
    batch_log = os.path.join(LOG_DIR, f"{item_type}_batch_{process_id}_{timestamp}.log")
    logger = logging.getLogger(f"{item_type}_{process_id}")
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(batch_log)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.handlers = [handler]
    if debug:
        logger.handlers.append(logging.StreamHandler())
    return logger
