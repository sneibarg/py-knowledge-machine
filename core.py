import os
import logging
import sys
import requests
import json
from datetime import datetime

BASE_DIR = os.getcwd()
OWL_FILE = os.path.join(BASE_DIR, "opencyc-owl/opencyc-2012-05-10.owl")
FIXED_OWL_FILE = os.path.join(BASE_DIR, "opencyc-owl/opencyc-2012-05-10_fixed.owl")
LOG_DIR = os.path.join(BASE_DIR, "logs")
KM_SERVER_URL = "http://localhost:8080/km"
os.makedirs(LOG_DIR, exist_ok=True)


def setup_logging(debug=False):
    """Configure logging with a single file for all logs."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pid = os.getpid()
    log_file = os.path.join(LOG_DIR, f"application_{timestamp}_{pid}.log")
    logging.getLogger('').handlers = []
    logger = logging.getLogger('OWL-to-KM')
    logger.setLevel(logging.INFO if not debug else logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s [PID %(process)d] [%(levelname)s] [%(name)s] %(message)s")
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if debug:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        if sys.platform.startswith('win'):
            console_handler.stream = sys.stdout
            console_handler.stream.reconfigure(encoding='utf-8', errors='replace')
        logger.addHandler(console_handler)

    return logger


def send_to_km(expr, fail_mode="fail", dry_run=False):
    """Send a KM expression to the server."""
    logger = logging.getLogger('OWL-to-KM.rest_client')
    logger.info("Preparing to send expression: %s...", expr[:100])
    if dry_run:
        logger.info("Dry-run mode: Skipped sending '%s...'", expr[:100])
        return {"success": True, "message": "Dry-run: Skipped sending to KM server."}
    payload = {"expr": expr, "fail_mode": fail_mode}
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(KM_SERVER_URL, data=json.dumps(payload), headers=headers, timeout=10)
        response.raise_for_status()
        logger.info("Successfully sent expression: %s...", expr[:100])
        return {"success": True, "response: ": response.json()}
    except requests.exceptions.RequestException as e:
        logger.error("Failed to send expression: %s", str(e))
        return {"success": False, "error": str(e)}
