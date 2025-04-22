import argparse
import logging
import os
import sys
import time
import rdflib
from core import setup_logging, FIXED_OWL_FILE, send_to_km
from km_syntax import KMSyntaxGenerator
from ontology_loader import load_ontology
from multiprocessing import Pool, cpu_count, Manager, current_process
from functools import partial

logger = None
worker_logger = None
km_generator = None
manager = Manager()
successfully_sent = manager.dict()
failed_assertions = manager.dict()


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
    """Initialize worker process with a logger."""
    global logger, worker_logger
    worker_logger = logger.getChild(f'Worker.{current_process().name}')
    worker_logger.setLevel(logging.DEBUG if debug else logging.INFO)
    worker_logger.info("Initialized worker.")


def translate_assertions(assertion_list):
    translated_assertions = []
    for assertion in assertion_list:
        translated_assertions.append(translate_assertion(assertion))
    return translated_assertions


def translate_assertion(assertion):
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


def process_assertion(assertion, dry_run):
    """
    Process an assertion by recursively handling its dependencies.

    Args:
        assertion: The assertion to process
        dry_run: Boolean indicating if this is a dry run

    Returns:
        bool: True if assertion and all dependencies processed successfully, False otherwise
    """
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
        result = send_to_km(assertion, dry_run=dry_run)
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
    """Extract labels and external IDs from the graph."""
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


class OWLGraphProcessor:
    def __init__(self, parent_logger, assertions, generator, pool, args):
        self.assertions = assertions
        self.km_generator = generator
        self.pool = pool
        self.args = args
        self.successfully_sent = successfully_sent
        self.logger = parent_logger.getChild('OWL-Graph-Processor')

    def run(self):
        failed_assertions = manager.dict()
        remaining_assertions = set(self.assertions)
        progress_made = True

        while remaining_assertions and progress_made:
            process_func = partial(process_assertion, dry_run=self.args.dry_run, failed_assertions=failed_assertions)
            results = self.pool.map(process_func, remaining_assertions)

            progress_made = False
            new_remaining = set()
            for assertion, success in zip(remaining_assertions, results):
                if not success:
                    new_remaining.add(assertion)
                    if assertion not in failed_assertions:
                        failed_assertions[assertion] = "unknown_failure"
                elif assertion in self.successfully_sent:
                    progress_made = True

            remaining_assertions = new_remaining

        if remaining_assertions:
            print(f"Unprocessed assertions: {len(remaining_assertions)}")
            print(f"Failure reasons: {dict(failed_assertions)}")

        return len(self.successfully_sent)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Translate OpenCyc OWL to KM KRL.")
    parser.add_argument("--debug", action="store_true", help="Enable debug output.")
    parser.add_argument("--dry-run", action="store_true", help="Skip sending requests to KM server.")
    parser.add_argument("--num-processes", type=int, help="Number of processes to use.")
    parser.add_argument("--translate-only", action="store_true", help="Translate and log only.")
    return parser.parse_args()


def main():
    global km_generator, logger
    args = parse_arguments()
    logger = setup_logging(args.debug)
    if not os.path.exists(FIXED_OWL_FILE):
        logger.error("Fixed OWL file not found at %s.", FIXED_OWL_FILE)
        sys.exit(1)

    num_processes = args.num_processes if args.num_processes else cpu_count()
    logger.info("Starting KM translation process.")
    graph = load_ontology(logger)
    object_map = extract_labels_and_ids(graph, logger)
    km_generator = KMSyntaxGenerator(graph, object_map, logger)
    pool = Pool(processes=num_processes, initializer=init_worker, initargs=(args.debug,))
    assertions = preprocess(graph)
    translate_start = time.time()
    translated_assertions = translate_assertions(assertions)
    if args.translate_only:
        logger.info(f"Translated {str(len(translated_assertions))} in {str(int(time.time() - translate_start))} seconds.")
        sys.exit(0)

    processor = OWLGraphProcessor(logger, translated_assertions, km_generator, pool, args)
    processor.run()

    total_expressions = len(processor.successfully_sent)
    logger.info("Processed and sent %d KRL expressions in total.", total_expressions)


if __name__ == "__main__":
    main()
