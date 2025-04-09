import os
import re
from config import OWL_FILE, FIXED_OWL_FILE
from logging_setup import setup_logging


def preprocess_owl_file():
    logger = setup_logging("preprocess")
    logger.info("Starting preprocessing of OWL file...")

    if not os.path.exists(OWL_FILE):
        logger.error(f"OWL file not found at {OWL_FILE}")
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
                    logger.info(f"Fixed datatype mismatch: {fixed_line.strip()}")
                    continue
            outfile.write(line)
    logger.info(f"Preprocessing complete. Saved as {FIXED_OWL_FILE}")