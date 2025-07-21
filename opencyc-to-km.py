import argparse
import json
import logging
import os
import sys
import time
import rdflib

from multiprocessing import Pool
from processor.owl.OWLGraphProcessor import OWLGraphProcessor
from service.KMSyntaxService import KMSyntaxGenerator
from service.LoggingService import LoggingService
from service.OpenCycService import CYC_ANNOT_LABEL, CYC_BASES, OpenCycService

host = "dragon:3602"
num_processes = int(os.cpu_count() - 1)
logging_service = LoggingService(os.path.join(os.getcwd(), "runtime/logs"), "OWL-to-KM")
logger = logging_service.setup_logging(False)

os.makedirs(os.path.join(os.getcwd(), "runtime/logs"), exist_ok=True)


def generate_assertions(graph_processor, use_sparql_queries=False):
    start_time = time.time()
    if use_sparql_queries:
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


def translate_only(args, translated_assertions):
    if args.debug:
        for assertion in translated_assertions:
            logger.debug(json.dumps(assertion, indent=2))


def main():
    args = parse_arguments()
    if args.debug:
        logger.setLevel(logging.DEBUG)

    open_cyc_service = OpenCycService(host, logger)
    if not os.path.exists(open_cyc_service.file):
        logger.error("OWL file not found at %s.", open_cyc_service.file)
        sys.exit(1)

    pool = Pool(processes=num_processes)
    processing_start = time.time()
    logger.info("Starting KM translation process.")
    owl_graph_processor = OWLGraphProcessor(logger, pool, open_cyc_service, args)
    owl_graph_processor.set_annotation_label(CYC_ANNOT_LABEL)
    owl_graph_processor.set_bases(CYC_BASES)
    object_map = owl_graph_processor.extract_labels_and_ids()
    assertions = generate_assertions(owl_graph_processor, args.use_sparql_queries)
    if args.generate_only:
        sys.exit(0)
    km_generator = KMSyntaxGenerator(owl_graph_processor.graph, object_map, logger)
    translated_assertions = []
    for assertion in assertions:
        translated_assertions.append(km_generator.translate_assertion(assertion))
    logger.info(f"Translated {len(translated_assertions)} in {int(time.time() - processing_start)} seconds.")
    if args.translate_only:
        translate_only(args, translated_assertions)
        sys.exit(0)


if __name__ == "__main__":
    main()
