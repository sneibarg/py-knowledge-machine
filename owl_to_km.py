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
    Process a single assertion if all its dependencies are satisfied.

    Args:
        assertion: The assertion to process.
        dry_run: Boolean indicating if this is a test run.

    Returns:
        bool: True if successfully processed, False otherwise.
    """
    try:
        refs = km_generator.get_referenced_assertions(assertion)
        if any(ref not in successfully_sent for ref in refs):
            worker_logger.info(
                f"Assertion {assertion} has unsatisfied dependencies: {[ref for ref in refs if ref not in successfully_sent]}")
            return False

        result = send_to_km(assertion, dry_run=dry_run)
        if result.get("success", False):
            successfully_sent[assertion] = assertion
            worker_logger.info(f"Successfully sent assertion: {assertion}")
            return True
        else:
            worker_logger.error(f"Failed to send assertion {assertion}: {result}")
            return False
    except Exception as e:
        worker_logger.error(f"Error processing assertion {assertion}: {str(e)}")
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
    is_ready = None

    def __init__(self, parent_logger, assertions, generator, pool, args):
        self.assertions = assertions
        self.km_generator = generator
        self.pool = pool
        self.args = args
        self.successfully_sent = successfully_sent
        self.logger = parent_logger.getChild('OWL-Graph-Processor')

    def set_readiness_check(self, func):
        self.is_ready = func

    def run(self):
        """
        Process all assertions with retries until no further progress is possible.

        Returns:
            int: Number of successfully processed assertions.
        """
        self.logger.info("Starting processing in multi-threaded mode.")
        start_time = time.time()
        remaining_assertions = set(self.assertions)
        while remaining_assertions:
            readiness_start = time.time()
            self.logger.info("Determining assertion readiness.")
            readiness_results = self.pool.map(partial(self.is_ready, self.km_generator), remaining_assertions)
            self.logger.info(f"Determined readiness in {int(time.time() - readiness_start)} seconds.")
            ready_assertions = [
                assertion for assertion, ready in zip(remaining_assertions, readiness_results)
                if ready
            ]
            if not ready_assertions:
                self.logger.info("No more assertions can be processed. Remaining: %d", len(remaining_assertions))
                if remaining_assertions:
                    self.logger.warning(
                        "Some assertions could not be processed due to unsatisfied dependencies or errors: %s",
                        remaining_assertions)
                break

            results = self.pool.map(partial(process_assertion, ry_run=self.args.dry_run), ready_assertions)
            progress_made = False
            for assertion, success in zip(ready_assertions, results):
                if success:
                    remaining_assertions.remove(assertion)
                    progress_made = True
                else:
                    self.logger.debug(f"Assertion {assertion} not processed in this round.")

            if not progress_made:
                self.logger.warning("No progress made in this iteration. Stopping.")
                break

        successes = len(self.successfully_sent)
        self.logger.info("Processing completed in %.2fs. Sent %d assertions.", time.time() - start_time, successes)
        return successes


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
    processor.set_readiness_check(is_ready)
    processor.run()

    total_expressions = len(processor.successfully_sent)
    logger.info("Processed and sent %d KRL expressions in total.", total_expressions)


if __name__ == "__main__":
    main()
