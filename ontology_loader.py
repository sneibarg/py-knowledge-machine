import rdflib
from config import FIXED_OWL_FILE
from logging_setup import setup_logging


def load_ontology():
    logger = setup_logging("load")
    logger.info("Loading ontology with rdflib...")

    g = rdflib.Graph()
    try:
        g.parse(FIXED_OWL_FILE, format="xml")
        logger.info("Ontology loaded successfully with rdflib.")
        return g
    except Exception as e:
        logger.error(f"Failed to parse ontology with rdflib: {e}")
        raise
