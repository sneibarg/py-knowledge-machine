import argparse
import json
import logging
import os
import sys
import time
import rdflib

from multiprocessing import Pool
from processor.owl.OWLGraphProcessor import OWLGraphProcessor
from service.KMSyntaxGenerator import KMSyntaxGenerator
from service.LoggingService import LoggingService
from service.OpenCycService import CYC_ANNOT_LABEL, CYC_BASES, is_cyc_id, OpenCycService, FIXED_OWL_FILE, TINY_OWL_FILE

BASE_DIR = os.getcwd()
LOG_DIR = os.path.join(BASE_DIR, "runtime/logs")
logging_service = LoggingService(LOG_DIR, "OWL-to-KM")
logger = logging_service.setup_logging(False)
open_cyc_service = OpenCycService(logger)

os.makedirs(LOG_DIR, exist_ok=True)


def preprocess(graph):
    classes = list(graph.subjects(rdflib.RDF.type, rdflib.OWL.Class))
    individuals = [(s, o) for s, o in graph.subject_objects(rdflib.RDF.type)
                   if o != rdflib.OWL.Class and (o, rdflib.RDF.type, rdflib.OWL.Class) in graph]
    properties = list(graph.subjects(rdflib.RDF.type, rdflib.OWL.ObjectProperty))
    assertions = [("class", uri) for uri in classes] + \
                 [("property", uri) for uri in properties] + \
                 [("individual", (ind_uri, class_uri)) for ind_uri, class_uri in individuals]
    logger.info("Found %d classes, %d individuals, %d properties.", len(classes), len(individuals), len(properties))
    return assertions


def parse_arguments():
    parser = argparse.ArgumentParser(description="Translate OWL to KM")
    parser.add_argument("--dry-run", action="store_true", help="Skip sending requests to KM server.")
    parser.add_argument("--translate-only", action="store_true", help="Translate and log only.")
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)
    return args


def extract_labels_and_ids(graph, parent_logger):
    child_logger = parent_logger.getChild('LabelsExtractor')
    child_logger.info("Extracting labels and IDs from graph.")
    result = {}
    for subject in graph.subjects():
        label = next((str(obj) for obj in graph.objects(subject, rdflib.RDFS.label) if isinstance(obj, rdflib.Literal)),
                     None)
        external_id = next(
            (str(obj) for obj in graph.objects(subject, rdflib.OWL.sameAs) if isinstance(obj, rdflib.URIRef)), None)
        if label or external_id:
            result[subject] = {'label': label, 'external_id': external_id}
    child_logger.info("Extracted labels/IDs for %d resources.", len(result))
    return result


def main():
    args = parse_arguments()

    num_processes = 1

    if args.debug:
        logger.setLevel(logging.DEBUG)

    if not os.path.exists(FIXED_OWL_FILE):
        logger.error("Fixed OWL file not found at %s.", FIXED_OWL_FILE)
        sys.exit(1)

    if num_processes > 1:
        pool = Pool(processes=num_processes)
    else:
        pool = None

    processing_start = time.time()
    logger.info("Starting KM translation process.")
    owl_graph_processor = OWLGraphProcessor(logger, TINY_OWL_FILE, pool, open_cyc_service.preprocess_cyc_file,
                                            is_cyc_id, args)
    owl_graph_processor.set_annotation_label(CYC_ANNOT_LABEL)
    owl_graph_processor.set_bases(CYC_BASES)
    object_map = extract_labels_and_ids(owl_graph_processor.graph, logger)

    km_generator = KMSyntaxGenerator(owl_graph_processor.graph, object_map, logger)
    assertions = preprocess(owl_graph_processor.graph)
    translated_assertions = []  # Translate here instead of separate function
    for assertion in assertions:
        if assertion[0] == "class":
            expr = km_generator.class_to_km(assertion[1])
        elif assertion[0] == "property":
            expr = km_generator.property_to_km(assertion[1])
        elif assertion[0] == "individual":
            expr = km_generator.individual_to_km(assertion[1][0])
        translated_assertions.append(expr)

    logger.info(f"Translated {len(translated_assertions)} in {int(time.time() - processing_start)} seconds.")
    if args.translate_only:
        for assertion in translated_assertions:
            logger.info(json.dumps(assertion, indent=2))
        sys.exit(0)

    total_expressions = len(owl_graph_processor.successfully_sent)
    logger.info("Processed and sent %d KRL expressions in total.", total_expressions)


if __name__ == "__main__":
    main()
