import os
import re
import rdflib
from rdflib import URIRef, Namespace

TYPE_PREDICATES = [
    rdflib.RDF.type,
    rdflib.URIRef("http://sw.opencyc.org/2008/06/10/concept/Mx4rBVVEokNxEdaAAACgydogAg")
]
CYC_ANNOT_LABEL = URIRef("http://sw.opencyc.org/2006/07/15/cycAnnot#label")
CYC_BASES = [
    "http://sw.opencyc.org/2012/05/10/concept/",
    "http://sw.opencyc.org/concept/",
    "http://sw.cyc.com/concept/",
]

CYC = Namespace("http://sw.opencyc.org/concept/")
CYCANNOT = Namespace("http://sw.cyc.com/CycAnnotations_v1#")
cyc_annot_label = rdflib.URIRef("http://sw.cyc.com/CycAnnotations_v1#label")


def is_cyc_id(val):
    return isinstance(val, str) and re.match(r"^Mx[0-9A-Za-z\-]+$", val)


class OpenCycService:
    def __init__(self, logger):
        self.logger = logger

    def preprocess_cyc_file(self, file, preprocessed_file):
        self.logger.info("Starting preprocessing of OWL file: %s", file)
        if not os.path.exists(file):
            self.logger.error("OWL file not found at %s", file)
            raise FileNotFoundError(f"OWL file not found at {file}")

        with open(file, 'r', encoding='utf-8') as infile, open(preprocessed_file, 'w', encoding='utf-8') as outfile:
            for line in infile:
                if 'rdf:datatype="http://www.w3.org/2001/XMLSchema#integer"' in line:
                    match = re.search(r'>(\w+)</', line)
                    if match and not match.group(1).isdigit():
                        fixed_line = line.replace(
                            'rdf:datatype="http://www.w3.org/2001/XMLSchema#integer"',
                            'rdf:datatype="http://www.w3.org/2001/XMLSchema#string"'
                        )
                        outfile.write(fixed_line)
                        self.logger.info("Fixed datatype mismatch: %s", fixed_line.strip())
                        continue
                outfile.write(line)
        self.logger.info("Preprocessing complete. Saved as %s", preprocessed_file)