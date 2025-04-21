import argparse
import logging
import os
import time

import rdflib
from functools import partial
from multiprocessing import Pool, cpu_count, Manager, current_process
from config import FIXED_OWL_FILE
from km_syntax import KMSyntaxGenerator
from logging_setup import setup_logging
from ontology_loader import load_ontology
from preprocess import preprocess_owl_file
from rest_client import send_to_km
from utils import extract_labels_and_ids


def process_assertion(km_generator, assertion, successfully_sent, dry_run):
    worker_logger.info(f"Processing assertion: {assertion}")
    assertion_type, uri = assertion  # Assuming assertion is a tuple
    if assertion_type == "class":
        expr = km_generator.class_to_km(uri)
    elif assertion_type == "property":
        expr = km_generator.property_to_km(uri)
    elif assertion_type == "individual":
        ind_uri, class_uri = uri
        expr = km_generator.individual_to_km(ind_uri)
    else:
        worker_logger.error(f"Unknown type: {assertion_type}")
        raise ValueError(f"Unknown type: {assertion_type}")
    result = send_to_km(expr, dry_run=dry_run)  # Assumed function
    if result.get("success", False):
        successfully_sent[assertion] = expr
        worker_logger.info(f"Successfully sent assertion: {assertion}")
        return True
    else:
        worker_logger.error(f"Failed to process {assertion}: {result}")
        return False


class OWLGraphProcessor:
    def __init__(self, graph, object_map, args, num_workers):
        self.graph = graph
        self.object_map = object_map
        self.args = args
        self.km_generator = KMSyntaxGenerator(graph, object_map)
        self.assertions = []
        self.dependencies = {}
        self.dependency_aware = not args.single_thread
        self.manager = Manager()
        self.successfully_sent = self.manager.dict()
        self.pool = Pool(
            processes=num_workers,
            initializer=self.init_worker,
            initargs=(args.debug,)
        )
        self.logger = logging.getLogger("OWLGraphProcessor")
        self.logger.setLevel(logging.INFO if not args.debug else logging.DEBUG)
        handler = logging.FileHandler("logs/main.log")
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(handler)
        if self.dependency_aware:
            start_time = time.time()
            self.logger.info(f"Starting dependency loading at start-time={start_time}.")
            self.dependencies = self.compute_dependencies(args.parallel_deps)
            elapsed_time = time.time() - start_time
            self.logger.info(f"Dependency loading completed in {elapsed_time:.2f} seconds.")
        else:
            self.dependencies = {}

    @staticmethod
    def init_worker(debug):
        global worker_logger
        process_name = current_process().name
        log_file = f"logs/{process_name}.log"
        worker_logger = logging.getLogger(process_name)
        worker_logger.setLevel(logging.INFO if not debug else logging.DEBUG)
        handler = logging.FileHandler(log_file)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        worker_logger.addHandler(handler)

    def compute_dependencies(self, parallel_deps):
        dependencies = {}
        if parallel_deps:
            self.logger.info("Parallel dependency computing enabled.")
            results = self.pool.map(self._compute_deps_worker, self.assertions)
            for assertion, deps in zip(self.assertions, results):
                dependencies[assertion] = deps
        else:
            for assertion in self.assertions:
                dependencies[assertion] = self.km_generator.get_referenced_assertions(assertion)
        return dependencies

    def _compute_deps_worker(self, assertion):
        self.logger.info(f"Computing assertion {assertion}")
        return self.km_generator.get_referenced_assertions(assertion)

    def get_ready_assertions(self):
        if self.dependency_aware:
            ready = []
            for assertion in self.assertions:
                deps = self.dependencies.get(assertion, [])
                if all(dep in self.successfully_sent for dep in deps):
                    ready.append(assertion)
            return ready
        else:
            return self.assertions  # All assertions are ready if not dependency-aware

    def process_assertion(self, assertion):
        """Process a single assertion and return the result."""
        assertion_type, uri = assertion
        if assertion_type == "class":
            expr = self.km_generator.class_to_km(uri)
        elif assertion_type == "property":
            expr = self.km_generator.property_to_km(uri)
        elif assertion_type == "individual":
            ind_uri, class_uri = uri
            expr = self.km_generator.individual_to_km(ind_uri)  # Assumes class_uri is handled internally
        else:
            self.logger.error(f"Unknown type: {assertion_type}")
            raise ValueError(f"Unknown type: {assertion_type}")

        result = send_to_km(expr, dry_run=self.args.dry_run)
        self.logger.info(f"Generated: {expr} | Result: {result}")
        if result.get("success", False):
            self.successfully_sent[assertion] = expr
            return True
        else:
            self.logger.error(f"Failed to process {assertion}: {result}")
            return False

    def run(self):
        if self.dependency_aware:
            # Dependency-aware multi-threaded processing
            self.logger.info("Running in dependency-aware multi-threaded mode.")
            while self.assertions:
                ready_assertions = self.get_ready_assertions()
                if not ready_assertions:
                    self.logger.info("No more assertions to process or dependencies unresolved.")
                    break
                process_func = partial(process_assertion, successfully_sent=self.successfully_sent,
                                   dry_run=self.args.dry_run)
                results = self.pool.map(self.process_assertion, ready_assertions)
                self.assertions = [a for a in self.assertions if a not in ready_assertions]
        else:
            self.logger.info("Running in single-threaded mode without dependencies.")
            for assertion in self.assertions:
                process_assertion(assertion)

        self.pool.close()
        self.pool.join()
        self.logger.info("Processing complete.")


def main():
    parser = argparse.ArgumentParser(description="Translate OpenCyc OWL to KM KRL.")
    parser.add_argument("--single-thread", action="store_true", help="Run in single-threaded mode without dependencies.")
    parser.add_argument("--debug", action="store_true", help="Enable debug output to console.")
    parser.add_argument("--dry-run", action="store_true", help="Skip sending requests to KM server.")
    parser.add_argument("--num-processes", type=int, help="Specify the number of processes to fork.")
    parser.add_argument("--parallel-deps", action="store_true", help="Compute dependencies in parallel.")
    args = parser.parse_args()

    logger = setup_logging("main", args.debug)
    logger.info("Starting KM translation process.")

    if not os.path.exists(FIXED_OWL_FILE):
        preprocess_owl_file()

    graph = load_ontology()
    logger.info("Extracting object labels and external IDs.")
    object_map = extract_labels_and_ids(graph)
    classes = list(graph.subjects(rdflib.RDF.type, rdflib.OWL.Class))
    individuals = [(s, o) for s, o in graph.subject_objects(rdflib.RDF.type)
                   if o != rdflib.OWL.Class and (o, rdflib.RDF.type, rdflib.OWL.Class) in graph]
    properties = list(graph.subjects(rdflib.RDF.type, rdflib.OWL.ObjectProperty))

    logger.info(f"Found {len(classes)} classes, {len(individuals)} individuals, {len(properties)} properties.")

    # Prepare assertions
    assertions = []
    for uri in classes:
        assertions.append(("class", uri))
    for uri in properties:
        assertions.append(("property", uri))
    for ind_uri, class_uri in individuals:
        assertions.append(("individual", (ind_uri, class_uri)))

    # Configure and run OWLGraphProcessor
    num_processes = args.num_processes if args.num_processes else cpu_count()
    processor = OWLGraphProcessor(graph, object_map, assertions, args, num_processes)
    processor.run()

    total_expressions = len(processor.successfully_sent)
    logger.info(f"Processed and sent {total_expressions} KRL expressions in total.")


if __name__ == "__main__":
    main()
