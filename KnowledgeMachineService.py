import json
import logging
import requests


class KMService:
    def __init__(self, km_service):
        self.km_service = km_service

    def send_to_km(self, expr, fail_mode="fail", dry_run=False):
        """Send a KM expression to the server."""
        logger = logging.getLogger('OWL-to-KM.rest_client')
        logger.info("Preparing to send expression: %s...", expr[:100])
        if dry_run:
            logger.info("Dry-run mode: Skipped sending '%s...'", expr[:100])
            return {"success": True, "message": "Dry-run: Skipped sending to KM server."}
        payload = {"expr": expr, "fail_mode": fail_mode}
        headers = {"Content-Type": "application/json"}
        try:
            response = requests.post(self.km_service, data=json.dumps(payload), headers=headers, timeout=10)
            response.raise_for_status()
            logger.info("Successfully sent expression: %s...", expr[:100])
            return {"success": True, "response: ": response.json()}
        except requests.exceptions.RequestException as e:
            logger.error("Failed to send expression: %s", str(e))
            return {"success": False, "error": str(e)}