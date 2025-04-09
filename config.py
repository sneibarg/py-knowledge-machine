import os

BASE_DIR = "./py-knowledge-machine"

OWL_FILE = os.path.join(BASE_DIR, "opencyc-owl/opencyc-2012-05-10.owl")
FIXED_OWL_FILE = os.path.join(BASE_DIR, "opencyc-owl/opencyc-2012-05-10_fixed.owl")
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

KM_SERVER_URL = "http://localhost:8080/km"
