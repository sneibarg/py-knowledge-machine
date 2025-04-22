import rdflib
import os
import re
import time
from core import FIXED_OWL_FILE, OWL_FILE


def preprocess_owl_file(logger):
    """Preprocess the OWL file to fix datatype issues."""
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


def load_ontology(logger):
    """Load the ontology, preprocessing if necessary."""
    start_time = time.time()
    onto_logger = logger.getChild('OntologyLoader')
    if not os.path.exists(FIXED_OWL_FILE):
        onto_logger.info("Fixed OWL file not found. Triggering preprocessing.")
        preprocess_owl_file(logger)
    else:
        onto_logger.info("Using existing fixed OWL file: %s", FIXED_OWL_FILE)

    onto_logger.info("Loading ontology with rdflib.")
    g = rdflib.Graph()
    try:
        g.parse(FIXED_OWL_FILE, format="xml")
        onto_logger.info("Ontology loaded successfully with %d triples in %d.", len(g), int(time.time() - start_time))
        return g
    except Exception as e:
        onto_logger.error("Failed to parse ontology: %s", str(e))
        raise
