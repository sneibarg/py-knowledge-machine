import os
import logging
import re
import sys
import requests
import json
from datetime import datetime
from rdflib import URIRef

CYC_ANNOT_LABEL = URIRef("http://sw.opencyc.org/2006/07/15/cycAnnot#label")
CYC_BASES = [
    "http://sw.opencyc.org/2012/05/10/concept/",
    "http://sw.opencyc.org/concept/",
    "http://sw.cyc.com/concept/",
]

CAMEL_CASE_PATTERN = r"[A-Z][a-z]+([A-Z][a-z]*)*"
HYPHEN = r"-"
WORD_PATTERN = r"\w+"
START_ANCHOR = r"^"
END_ANCHOR = r"$"
FUNCTION_GROUP = "function"
ACTIVITY_GROUP = "activity"

PATTERN = (
    START_ANCHOR
    + r"(?P<" + FUNCTION_GROUP + r">" + CAMEL_CASE_PATTERN + r")"
    + HYPHEN
    + r"(?P<" + ACTIVITY_GROUP + r">" + WORD_PATTERN + r")"
    + END_ANCHOR
)

HAS_PATTERN = (
    START_ANCHOR
    + r"(?P<words>(\w+\s)*)"  # Zero or more words followed by a space
    + r"has"
    + END_ANCHOR
)

BASE_DIR = os.getcwd()
OWL_FILE = os.path.join(BASE_DIR, "opencyc-owl/opencyc-2012-05-10.owl")
FIXED_OWL_FILE = os.path.join(BASE_DIR, "opencyc-owl/opencyc-2012-05-10_fixed.owl")
TINY_OWL_FILE = os.path.join(BASE_DIR, "opencyc-owl/opencyc-owl-tiny.owl")
GO_OWL_FILE = os.path.join(BASE_DIR, 'opencyc-owl/go-basic.owl')
LOG_DIR = os.path.join(BASE_DIR, "logs")
KM_SERVER_URL = "http://localhost:8080/km"
os.makedirs(LOG_DIR, exist_ok=True)


def is_cyc_id(val):
    return isinstance(val, str) and re.match(r"^Mx[0-9A-Za-z\-]+$", val)


def preprocess_cyc_file(logger):
    logger.info("Starting preprocessing of OWL file: %s", OWL_FILE)
    if not os.path.exists(OWL_FILE):
        logger.error("OWL file not found at %s", OWL_FILE)
        raise FileNotFoundError(f"OWL file not found at {OWL_FILE}")

    with open(OWL_FILE, 'r', encoding='utf-8') as infile, open(FIXED_OWL_FILE, 'w', encoding='utf-8') as outfile:
        for line in infile:
            if 'rdf:datatype="http://www.w3.org/2001/XMLSchema#integer"' in line:
                match = re.search(r'>(\w+)</', line)
                if match and not match.group(1).isdigit():
                    fixed_line = line.replace(
                        'rdf:datatype="http://www.w3.org/2001/XMLSchema#integer"',
                        'rdf:datatype="http://www.w3.org/2001/XMLSchema#string"'
                    )
                    outfile.write(fixed_line)
                    logger.info("Fixed datatype mismatch: %s", fixed_line.strip())
                    continue
            outfile.write(line)
    logger.info("Preprocessing complete. Saved as %s", FIXED_OWL_FILE)


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


def lambda_match(input_str, pattern, anon_dict):
    match = re.match(pattern, input_str)
    if match:
        named_captures = {k: v for k, v in match.groupdict().items() if v is not None}
        return {**anon_dict, **named_captures}
    else:
        return anon_dict
