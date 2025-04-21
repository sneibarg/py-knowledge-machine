import os
import logging
import requests
import json
from datetime import datetime

BASE_DIR = os.getcwd()
OWL_FILE = os.path.join(BASE_DIR, "opencyc-owl/opencyc-2012-05-10.owl")
FIXED_OWL_FILE = os.path.join(BASE_DIR, "opencyc-owl/opencyc-2012-05-10_fixed.owl")
LOG_DIR = os.path.join(BASE_DIR, "logs")
KM_SERVER_URL = "http://localhost:8080/km"
os.makedirs(LOG_DIR, exist_ok=True)


def setup_logging(log_type, debug=False, pid=None):
    """Configure logging with optional PID for process-specific logs."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    logger_name = f"{log_type}_{os.getpid()}" if pid else log_type
    log_file = os.path.join(LOG_DIR, f"{logger_name}_{timestamp}.log")

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO if not debug else logging.DEBUG)

    handlers = [logging.FileHandler(log_file)]
    if debug:
        handlers.append(logging.StreamHandler())

    formatter = logging.Formatter(f"%(asctime)s [PID {os.getpid()}] [%(levelname)s] %(message)s")
    for handler in handlers:
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def setup_batch_logger(item_type, process_id, timestamp, debug=False):
    """Setup logger for batch processing with PID."""
    pid = os.getpid()
    logger_name = f"{item_type}_{process_id}_{pid}"
    batch_log = os.path.join(LOG_DIR, f"{logger_name}_{timestamp}.log")
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO if not debug else logging.DEBUG)
    handler = logging.FileHandler(batch_log)
    formatter = logging.Formatter(f"%(asctime)s [PID {pid}] [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    logger.handlers = [handler]
    if debug:
        logger.handlers.append(logging.StreamHandler())
    return logger


def send_to_km(expr, fail_mode="fail", dry_run=False):
    """Send a KM expression to the server."""
    logger = logging.getLogger(__name__)
    logger.info(f"[PID {os.getpid()}] Preparing to send expression: {expr[:100]}...")
    if dry_run:
        logger.info(f"[PID {os.getpid()}] Dry-run mode: Skipped sending '{expr[:100]}...'")
        return {"success": True, "message": "Dry-run: Skipped sending to KM server."}
    payload = {"expr": expr, "fail_mode": fail_mode}
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(KM_SERVER_URL, data=json.dumps(payload), headers=headers, timeout=10)
        response.raise_for_status()
        logger.info(f"[PID {os.getpid()}] Successfully sent expression: {expr[:100]}...")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"[PID {os.getpid()}] Failed to send expression: {str(e)}")
        return {"success": False, "error": str(e)}