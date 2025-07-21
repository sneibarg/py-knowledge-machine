import json
import logging
import rdflib
import requests
from tenacity import stop_after_attempt, retry_if_exception_type, wait_exponential, retry

STANDARD_PREDICATES = {
    rdflib.RDF.type: "instance-of",
    rdflib.RDFS.subClassOf: "superclasses",
    rdflib.RDFS.label: "prettyString",
    rdflib.OWL.sameAs: "same-as",
    rdflib.OWL.disjointWith: "mustnt-be-a",
    rdflib.RDFS.comment: "comment",
    rdflib.RDFS.subPropertyOf: "subPropertyOf",
    "Mx4rvViAzpwpEbGdrcN5Y29ycA": "datatype",
    "Mx4rBVVEokNxEdaAAACgydogAg": "Quoted Isa",
    "Mx4rwLSVCpwpEbGdrcN5Y29ycA": "prettyString",
    "Mx4r8POVIYRHEdmd8gACs6hbCw": "prettyString-Canonical"
}
BUILT_IN_FRAMES = {
    "instance-of", "superclasses", "label", "Slot", "Class", "Thing", "has",
    "with", "a", "in", "where", "then", "else", "if", "forall", "oneof", "a-prototype"
}


def rdf_to_krl_name(uri) -> str:
    return str(uri).split('/')[-1]


class KMService:
    def __init__(self, km_service):
        self.km_service = km_service

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.exceptions.ConnectionError,
                                       requests.exceptions.Timeout,
                                       requests.exceptions.HTTPError))
    )
    def send_to_km(self, expr, fail_mode="fail", dry_run=False) -> dict:
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
