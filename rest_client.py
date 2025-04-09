import requests
import json
from config import KM_SERVER_URL


def send_to_km(expr, fail_mode="fail", dry_run=False):
    if dry_run:
        return "Dry-run: Skipped sending to KM server."
    url = KM_SERVER_URL
    payload = {"expr": expr, "fail_mode": fail_mode}
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(url, data=json.dumps(payload), headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return f"Error: {e}"
