import os
import re
import rdflib
from rdflib import URIRef, Namespace
from agent.CycLAgent import CycLAgent

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
BASE_DIR = os.getcwd()
OWL_FILE = os.path.join(BASE_DIR, "runtime/opencyc-owl/opencyc-2012-05-10.owl")
FIXED_OWL_FILE = os.path.join(BASE_DIR, "runtime/opencyc-owl/opencyc-2012-05-10_fixed.owl")
TINY_OWL_FILE = os.path.join(BASE_DIR, "runtime/opencyc-owl/opencyc-owl-tiny.owl")


class OpenCycService(CycLAgent):
    def __init__(self, host, logger):
        super().__init__(host)
        self.logger = logger
        self.file = OWL_FILE
        self.preprocessed_file = FIXED_OWL_FILE

    @staticmethod
    def custom_matching_function(val) -> str:
        return isinstance(val, str) and re.match(r"^Mx[0-9A-Za-z\-]+$", val)

    def preprocess(self, owl_file=None) -> None:
        if owl_file is None and not os.path.exists(self.file):
            self.logger.error("Default OWL file not found at %s", self.file)
            raise FileNotFoundError(f"Default OWL file not found at {self.file}")

        if owl_file is None:
            owl_file = self.file

        self.logger.info("Starting preprocessing of OWL file: %s", owl_file)
        with open(owl_file, 'r', encoding='utf-8') as infile, open(self.preprocessed_file, 'w', encoding='utf-8') as outfile:
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
        self.logger.info("Preprocessing complete. Saved as %s", self.preprocessed_file)
