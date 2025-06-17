import argparse
import json
import logging
import os
import re
import sys
import time
import rdflib
from datetime import datetime
from multiprocessing import Pool, Manager, current_process
from KMSyntaxGenerator import KMSyntaxGenerator
from OWLGraphProcessor import OWLGraphProcessor
from OpenCycService import CYC_ANNOT_LABEL, CYC_BASES, is_cyc_id, OpenCycService

logger = None
worker_logger = None
manager = None
successfully_sent = None
failed_assertions = None
BASE_DIR = os.getcwd()
OWL_FILE = os.path.join(BASE_DIR, "opencyc-owl/opencyc-2012-05-10.owl")
FIXED_OWL_FILE = os.path.join(BASE_DIR, "opencyc-owl/opencyc-2012-05-10_fixed.owl")
TINY_OWL_FILE = os.path.join(BASE_DIR, "opencyc-owl/opencyc-owl-tiny.owl")
GO_OWL_FILE = os.path.join(BASE_DIR, 'opencyc-owl/go-basic.owl')
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

CAMEL_CASE_PATTERN = r"[A-Z][a-z]+([A-Z][a-z]*)*"
HYPHEN = r"-"
WORD_PATTERN = r"\w+"
START_ANCHOR = r"^"
END_ANCHOR = r"$"
FUNCTION_GROUP = "function"
ACTIVITY_GROUP = "activity"

PATTERN = (
    START_ANCHOR
    + r"(?P<" + FUNCTION_GROUP + r">" + CAMEL_CASE_PATTERN + r")"
    + HYPHEN
    + r"(?P<" + ACTIVITY_GROUP + r">" + WORD_PATTERN + r")"
    + END_ANCHOR
)

HAS_PATTERN = (
    START_ANCHOR
    + r"(?P<words>(\w+\s)*)"  # Zero or more words followed by a space
    + r"has"
    + END_ANCHOR
)


def lambda_match(input_str, pattern, anon_dict):
    match = re.match(pattern, input_str)
    if match:
        named_captures = {k: v for k, v in match.groupdict().items() if v is not None}
        return {**anon_dict, **named_captures}
    else:
        return anon_dict


def setup_logging(debug=False):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pid = os.getpid()
    log_file = os.path.join(LOG_DIR, f"application_{timestamp}_{pid}.log")
    logging.getLogger('').handlers = []
    new_logger = logging.getLogger('OWL-to-KM')
    new_logger.setLevel(logging.INFO if not debug else logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s [PID %(process)d] [%(levelname)s] [%(name)s] %(message)s")
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    new_logger.addHandler(file_handler)

    if debug:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        if sys.platform.startswith('win'):
            console_handler.stream = sys.stdout
            console_handler.stream.reconfigure(encoding='utf-8', errors='replace')
        new_logger.addHandler(console_handler)

    return new_logger


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


def init_worker(debug):
    global logger, worker_logger
    worker_logger = logger.getChild(f'Worker.{current_process().name}')
    worker_logger.setLevel(logging.DEBUG if debug else logging.INFO)
    worker_logger.info("Initialized worker.")


def translate_assertions(assertion_list, km_generator):
    translated_assertions = []
    for assertion in assertion_list:
        translated_assertions.append(translate(assertion, km_generator))
    return translated_assertions


def translate(assertion, km_generator):
    assertion_type, uri = assertion
    if assertion_type == "class":
        expr = km_generator.class_to_km(uri)
    elif assertion_type == "property":
        expr = km_generator.property_to_km(uri)
    elif assertion_type == "individual":
        ind_uri, class_uri = uri
        expr = km_generator.individual_to_km(ind_uri)
    else:
        worker_logger.error("Unknown assertion type: %s", assertion_type)
        raise ValueError(f"Unknown type: {assertion_type}")
    return expr


def process_assertion(assertion, dry_run, km_generator, km_service):
    if assertion in successfully_sent:
        return True

    all_deps_successful = True
    for ref in km_generator.get_referenced_assertions(assertion):
        if ref not in successfully_sent and not process_assertion(ref, dry_run):
            all_deps_successful = False
            if ref not in successfully_sent and ref not in failed_assertions:
                failed_assertions[ref] = "dependency_failure"

    if not all_deps_successful:
        failed_assertions[assertion] = "dependency_failure"
        return False

    try:
        result = km_service.send_to_km(assertion, dry_run=dry_run)
        if result.get("success", False):
            successfully_sent[assertion] = assertion
            return True
        else:
            failed_assertions[assertion] = "processing_failure"
            return False
    except Exception as e:
        failed_assertions[assertion] = f"exception: {str(e)}"
        return False


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


def is_ready(assertion, generator):
    return all(ref in successfully_sent for ref in generator.get_referenced_assertions(assertion))


def parse_arguments():
    parser = argparse.ArgumentParser(description="Translate OpenCyc OWL to KM KRL.")
    parser.add_argument("--debug", action="store_true", help="Enable debug output.")
    parser.add_argument("--dry-run", action="store_true", help="Skip sending requests to KM server.")
    parser.add_argument("--num-processes", type=int, help="Number of processes to use.")
    parser.add_argument("--translate-only", action="store_true", help="Translate and log only.")
    return parser.parse_args()


def main():
    global manager, logger, successfully_sent, failed_assertions
    pool = None
    args = parse_arguments()
    logger = setup_logging(args.debug)
    if not os.path.exists(FIXED_OWL_FILE):
        logger.error("Fixed OWL file not found at %s.", FIXED_OWL_FILE)
        sys.exit(1)

    num_processes = args.num_processes if args.num_processes else 1
    if num_processes > 1:
        manager = Manager()
        failed_assertions = manager.dict()
        successfully_sent = manager.dict()
        pool = Pool(processes=num_processes, initializer=init_worker, initargs=(args.debug,))

    open_cyc_service = OpenCycService(logger)
    processing_start = time.time()
    logger.info("Starting KM translation process.")
    owl_graph_processor = OWLGraphProcessor(logger,
                                            TINY_OWL_FILE,
                                            pool,
                                            open_cyc_service.preprocess_cyc_file,
                                            is_cyc_id,
                                            args)
    owl_graph_processor.set_annotation_label(CYC_ANNOT_LABEL)
    owl_graph_processor.set_bases(CYC_BASES)
    object_map = extract_labels_and_ids(owl_graph_processor.graph, logger)

    km_generator = KMSyntaxGenerator(owl_graph_processor.graph, object_map, logger)
    assertions = preprocess(owl_graph_processor.graph)
    translated_assertions = translate_assertions(assertions, km_generator)
    logger.info(f"Translated {str(len(translated_assertions))} in {str(int(time.time() - processing_start))} seconds.")
    if args.translate_only:
        for assertion in translated_assertions:
            logger.info("-------------------------------------------------------------------------------------------------")
            logger.info("-------------------------------------------------------------------------------------------------")
            logger.info(json.dumps(assertion, indent=2))
        sys.exit(0)

    total_expressions = len(owl_graph_processor.successfully_sent)
    logger.info("Processed and sent %d KRL expressions in total.", total_expressions)


if __name__ == "__main__":
    main()
