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
from service.OpenCycService import CYC_ANNOT_LABEL, CYC_BASES, OpenCycService, OWL_FILE

BASE_DIR = os.getcwd()
LOG_DIR = os.path.join(BASE_DIR, "runtime/logs")
logging_service = LoggingService(LOG_DIR, "OWL-to-KM")
logger = logging_service.setup_logging(False)
open_cyc_service = OpenCycService(logger)

os.makedirs(LOG_DIR, exist_ok=True)


def generate_assertions(graph_processor, use_sparql_queries=False):
    start_time = time.time()
    if use_sparql_queries:
        logger.info("Using SPARQL queries for RDF optimization.")
        try:
            classes = graph_processor.get_classes_via_sparql()
            properties = graph_processor.get_properties_via_sparql()
            individuals = graph_processor.get_individuals_via_sparql()
            logger.info("SPARQL queries completed in %d seconds.", int(time.time() - start_time))
        except Exception as e:
            logger.warning("SPARQL queries failed: %s. Falling back to direct graph methods.", str(e))
            use_sparql_queries = False

    if not use_sparql_queries:
        classes = list(graph_processor.graph.subjects(rdflib.RDF.type, rdflib.OWL.Class))
        individuals = [(s, o) for s, o in graph_processor.graph.subject_objects(rdflib.RDF.type)
                       if o != rdflib.OWL.Class and (o, rdflib.RDF.type, rdflib.OWL.Class) in graph_processor.graph]
        properties = list(graph_processor.graph.subjects(rdflib.RDF.type, rdflib.OWL.ObjectProperty))
        logger.info("Direct graph methods completed in %d seconds.", int(time.time() - start_time))

    assertions = [("class", uri) for uri in classes] + \
                 [("property", uri) for uri in properties] + \
                 [("individual", (ind_uri, class_uri)) for ind_uri, class_uri in individuals]
    logger.info("Found %d classes, %d individuals, %d properties.", len(classes), len(individuals), len(properties))
    return assertions


def parse_arguments():
    parser = argparse.ArgumentParser(description="Translate OWL to KM")
    parser.add_argument("--generate-only", action="store_true", help="Generate assertions from the graph.")
    parser.add_argument("--translate-only", action="store_true", help="Translate and log only.")
    parser.add_argument("--dry-run", action="store_true", help="Skip sending requests to KM server.")
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--use-sparql-queries', action='store_true',
                        help='Enable RDF optimization using SPARQL queries for data fetching (experimental).')
    args = parser.parse_args()
    if str(sys.argv[0]) == "-h" or str(sys.argv[0]) == "--help":
        parser.print_help()
        sys.exit(0)
    return args


def main():
    global open_cyc_service
    args = parse_arguments()

    num_processes = 1

    if args.debug:
        logger.setLevel(logging.DEBUG)

    if not os.path.exists(OWL_FILE):
        logger.error("OWL file not found at %s.", OWL_FILE)
        sys.exit(1)

    if num_processes > 1:
        pool = Pool(processes=num_processes)
    else:
        pool = None

    processing_start = time.time()
    logger.info("Starting KM translation process.")
    owl_graph_processor = OWLGraphProcessor(logger, pool, open_cyc_service, args)
    owl_graph_processor.set_annotation_label(CYC_ANNOT_LABEL)
    owl_graph_processor.set_bases(CYC_BASES)
    object_map = owl_graph_processor.extract_labels_and_ids()

    km_generator = KMSyntaxGenerator(owl_graph_processor.graph, object_map, logger)
    assertions = generate_assertions(owl_graph_processor, args.use_sparql_queries)
    if args.generate_only:
        sys.exit(0)
    translated_assertions = []
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
