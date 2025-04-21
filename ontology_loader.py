import rdflib
import os
import re
from core import FIXED_OWL_FILE, OWL_FILE, setup_logging


def preprocess_owl_file(logger):
    """Preprocess the OWL file to fix datatype issues."""
    logger.info(f"[PID {os.getpid()}] Starting preprocessing of OWL file: {OWL_FILE}")
    if not os.path.exists(OWL_FILE):
        logger.error(f"[PID {os.getpid()}] OWL file not found at {OWL_FILE}")
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
                    logger.info(f"[PID {os.getpid()}] Fixed datatype mismatch: {fixed_line.strip()}")
                    continue
            outfile.write(line)
    logger.info(f"[PID {os.getpid()}] Preprocessing complete. Saved as {FIXED_OWL_FILE}")


def load_ontology():
    """Load the ontology, preprocessing if necessary."""
    logger = setup_logging("ontology_loader", pid=True)
    if not os.path.exists(FIXED_OWL_FILE):
        logger.info(f"[PID {os.getpid()}] Fixed OWL file not found. Triggering preprocessing.")
        preprocess_owl_file(logger)
    else:
        logger.info(f"[PID {os.getpid()}] Using existing fixed OWL file: {FIXED_OWL_FILE}")

    logger.info(f"[PID {os.getpid()}] Loading ontology with rdflib...")
    g = rdflib.Graph()
    try:
        g.parse(FIXED_OWL_FILE, format="xml")
        logger.info(f"[PID {os.getpid()}] Ontology loaded successfully with {len(g)} triples.")
        return g
    except Exception as e:
        logger.error(f"[PID {os.getpid()}] Failed to parse ontology: {str(e)}")
        raise