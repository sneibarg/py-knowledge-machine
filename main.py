import argparse
import os
import time

import rdflib
from datetime import datetime
from multiprocessing import Pool, cpu_count, Manager
from config import FIXED_OWL_FILE
from km_syntax import KMSyntaxGenerator
from logging_setup import setup_logging, setup_batch_logger
from ontology_loader import load_ontology
from preprocess import preprocess_owl_file
from rest_client import send_to_km
from utils import extract_labels_and_ids


def process_items(items, item_type, generator, process_id, timestamp, args):
    batch_logger = setup_batch_logger(item_type, process_id, timestamp, args.debug)
    batch_logger.info(f"Starting processing for {item_type} with {len(items)} items.")
    results = []

    if item_type == "class":
        for class_uri in items:
            expr = generator.class_to_km(class_uri)
            result = send_to_km(expr, dry_run=args.dry_run)
            batch_logger.info(f"Generated: {expr} | Result:\n{result}")
            results.append((expr, result))
    elif item_type == "individual":
        for ind_uri, class_uri in items:
            expr = generator.individual_to_km(ind_uri, class_uri)
            result = send_to_km(expr, dry_run=args.dry_run)
            batch_logger.info(f"Generated: {expr} | Result:\n{result}")
            results.append((expr, result))
    elif item_type == "property":
        for prop_uri in items:
            expr = generator.property_to_km(prop_uri)
            result = send_to_km(expr, dry_run=args.dry_run)
            batch_logger.info(f"Generated: {expr} | Result:\n{result}")
            results.append((expr, result))
    batch_logger.info("Processing complete.")
    return results


class OWLGraphProcessor:
    def __init__(self, graph, object_map, assertions, args, num_workers=4, parallel_deps=False):
        self.graph = graph
        self.object_map = object_map
        self.assertions = assertions
        self.args = args
        self.km_generator = KMSyntaxGenerator(graph, object_map)
        self.manager = Manager()
        self.successfully_sent = self.manager.dict()
        self.logger = setup_logging("owl_graph_processor", args.debug)
        self.logger.info("Starting dependency loading.")
        self.pool = Pool(processes=num_workers)
        start_time = time.time()
        self.dependencies = self.compute_dependencies(parallel_deps, num_workers)
        elapsed_time = time.time() - start_time
        self.logger.info(f"Dependency loading completed in {elapsed_time:.2f} seconds.")

    def compute_dependencies(self, parallel_deps, num_workers):
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
        ready = []
        for assertion in self.assertions:
            deps = self.dependencies.get(assertion, [])
            if all(dep in self.successfully_sent for dep in deps):
                ready.append(assertion)
        return ready

    def process_assertion(self, assertion):
        assertion_type, uri = assertion
        if assertion_type == "class":
            expr = self.km_generator.class_to_km(uri)
        elif assertion_type == "property":
            expr = self.km_generator.property_to_km(uri)
        elif assertion_type == "individual":
            ind_uri, class_uri = uri
            expr = self.km_generator.individual_to_km(ind_uri)
        else:
            self.logger.error(f"Unknown type: {assertion_type}")
            raise ValueError(f"Unknown type: {assertion_type}")
        result = send_to_km(expr, dry_run=self.args.dry_run)
        if result.get("success", False):
            self.successfully_sent[assertion] = expr
            return True
        else:
            self.logger.error(f"Failed to process {assertion}: {result}")
            return False

    def run(self):
        while True:
            ready_assertions = self.get_ready_assertions()
            if not ready_assertions:
                self.logger.info("No more assertions to process or dependencies unresolved.")
                break
            results = self.pool.map(self.process_assertion, ready_assertions)
            self.assertions = [a for a in self.assertions if a not in ready_assertions]
        self.pool.close()
        self.pool.join()
        self.logger.info("Processing complete.")


def main():
    parser = argparse.ArgumentParser(description="Translate OpenCyc OWL to KM KRL.")
    parser.add_argument("--single-thread", action="store_true", help="Run in single-threaded mode for testing.")
    parser.add_argument("--debug", action="store_true", help="Enable debug output to console.")
    parser.add_argument("--dry-run", action="store_true", help="Skip sending requests to KM server.")
    parser.add_argument("--num-processes", type=int, help="Specify the number of processes to fork.")
    parser.add_argument("--use-graph-processor", action="store_true",
                        help="Use the new OWLGraphProcessor for processing.")
    parser.add_argument("--parallel-deps", action="store_true",
                        help="Compute dependencies in parallel for OWLGraphProcessor.")
    args = parser.parse_args()

    logger = setup_logging("main", args.debug)
    logger.info("Starting KM translation process.")

    if not os.path.exists(FIXED_OWL_FILE):
        preprocess_owl_file()

    graph = load_ontology()
    logger.info("Extracting object labels and external IDs.")
    object_map = extract_labels_and_ids(graph)
    km_generator = KMSyntaxGenerator(graph, object_map)
    classes = list(graph.subjects(rdflib.RDF.type, rdflib.OWL.Class))
    individuals = [(s, o) for s, o in graph.subject_objects(rdflib.RDF.type)
                   if o != rdflib.OWL.Class and (o, rdflib.RDF.type, rdflib.OWL.Class) in graph]
    properties = list(graph.subjects(rdflib.RDF.type, rdflib.OWL.ObjectProperty))

    logger.info(f"Found {len(classes)} classes, {len(individuals)} individuals, {len(properties)} properties.")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if args.use_graph_processor:
        assertions = []
        for uri in classes:
            assertions.append(("class", uri))
        for uri in properties:
            assertions.append(("property", uri))
        for ind_uri, class_uri in individuals:
            assertions.append(("individual", (ind_uri, class_uri)))

        num_processes = args.num_processes if args.num_processes else cpu_count()
        processor = OWLGraphProcessor(graph, object_map, assertions, args,
                                      num_workers=num_processes, parallel_deps=args.parallel_deps)
        processor.run()
        total_expressions = len(processor.successfully_sent)
    else:
        property_results = []
        class_results = []
        individual_results = []
        if args.single_thread:
            logger.info("Running in single-threaded mode.")
            property_results = process_items(properties, "property", km_generator, 0, timestamp, args)
            class_results = process_items(classes, "class", km_generator, 0, timestamp, args)
            individual_results = process_items(individuals, "individual", km_generator, 0, timestamp, args)
        else:
            num_processes = args.num_processes if args.num_processes else cpu_count()
            logger.info(f"Running in multi-threaded mode with {num_processes} processes.")

            batch_size = max(1, len(classes) // num_processes)
            class_batches = [classes[i:i + batch_size] for i in range(0, len(classes), batch_size)]
            batch_size = max(1, len(individuals) // num_processes)
            individual_batches = [individuals[i:i + batch_size] for i in range(0, len(individuals), batch_size)]
            batch_size = max(1, len(properties) // num_processes)
            property_batches = [properties[i:i + batch_size] for i in range(0, len(properties), batch_size)]

            with Pool(processes=num_processes) as pool:
                property_tasks = [(batch, "property", km_generator, i, timestamp, args)
                                  for i, batch in enumerate(property_batches)]
                property_results = pool.starmap(process_items, property_tasks)

                class_tasks = [(batch, "class", km_generator, i, timestamp, args)
                               for i, batch in enumerate(class_batches)]
                class_results = pool.starmap(process_items, class_tasks)

                individual_tasks = [(batch, "individual", km_generator, i, timestamp, args)
                                    for i, batch in enumerate(individual_batches)]
                individual_results = pool.starmap(process_items, individual_tasks)

        total_expressions = (sum(len(batch) for batch in property_results) +
                             sum(len(batch) for batch in class_results) +
                             sum(len(batch) for batch in individual_results))

    logger.info(f"Processed and sent {total_expressions} KRL expressions in total.")


if __name__ == "__main__":
    main()
