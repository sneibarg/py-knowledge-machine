import argparse
import os
import time
from multiprocessing import Pool, cpu_count, Manager, current_process
from functools import partial
import rdflib
from core import setup_logging, send_to_km
from km_syntax import KMSyntaxGenerator
from ontology_loader import load_ontology


def init_worker(debug, parent_logger):
    """Initialize worker process with a logger."""
    global worker_logger
    worker_logger = parent_logger.getChild(f'Worker.{current_process().name}')
    worker_logger.info("Worker initialized.")


def process_assertion(km_generator, assertion, successfully_sent, dry_run):
    """Process a single assertion with dependency handling."""
    worker_logger.info("Processing assertion: %s", assertion)
    assertion_type, uri = assertion
    try:
        if assertion_type == "class":
            expr = km_generator.class_to_km(uri)
        elif assertion_type == "property":
            expr = km_generator.property_to_km(uri)
        elif assertion_type == "individual":
            ind_uri, class_uri = uri
            expr = km_generator.individual_to_km(ind_uri)
        else:
            worker_logger.error(f"[PID {os.getpid()}] Unknown assertion type: {assertion_type}")
            raise ValueError(f"Unknown type: {assertion_type}")

        refs = km_generator.get_referenced_assertions(assertion)
        worker_logger.info(f"[PID {os.getpid()}] Found {len(refs)} referenced assertions.")
        for ref in refs:
            if ref not in successfully_sent:
                worker_logger.info(f"[PID {os.getpid()}] Processing dependency: {ref}")
                process_assertion(km_generator, ref, successfully_sent, dry_run)

        result = send_to_km(expr, dry_run=dry_run)
        if result.get("success", False):
            successfully_sent[assertion] = expr
            worker_logger.info(f"[PID {os.getpid()}] Successfully sent assertion: {expr[:100]}...")
            return True
        else:
            worker_logger.error(f"[PID {os.getpid()}] Failed to send assertion: {result}")
            return False
    except Exception as e:
        worker_logger.error(f"[PID {os.getpid()}] Error processing assertion: {str(e)}")
        return False


def extract_labels_and_ids(graph):
    """Extract labels and external IDs from the graph (from utils.py)."""
    logger = setup_logging("utils", pid=True)
    logger.info(f"[PID {os.getpid()}] Extracting labels and IDs from graph...")
    result = {}
    for subject in graph.subjects():
        label = next((str(obj) for obj in graph.objects(subject, rdflib.RDFS.label) if isinstance(obj, rdflib.Literal)),
                     None)
        external_id = next(
            (str(obj) for obj in graph.objects(subject, rdflib.OWL.sameAs) if isinstance(obj, rdflib.URIRef)), None)
        if label or external_id:
            result[subject] = {'label': label, 'external_id': external_id}
    logger.info(f"[PID {os.getpid()}] Extracted labels/IDs for {len(result)} resources.")
    return result


class OWLGraphProcessor:
    def __init__(self, graph, object_map, assertions, args, num_workers):
        self.graph = graph
        self.object_map = object_map
        self.args = args
        self.assertions = assertions
        self.manager = Manager()
        self.successfully_sent = self.manager.dict()
        self.pool = Pool(processes=num_workers, initializer=init_worker, initargs=(args.debug,))
        self.logger = setup_logging("processor", debug=args.debug, pid=True)
        self.km_generator = KMSyntaxGenerator(graph, object_map, self.logger)
        self.logger.info(f"[PID {os.getpid()}] Initialized OWLGraphProcessor with {len(assertions)} assertions.")

    def run(self):
        """Run the processing with multi-processing."""
        self.logger.info("Starting processing in multi-threaded mode.")
        start_time = time.time()
        process_func = partial(process_assertion, self.km_generator, successfully_sent=self.successfully_sent,
                               dry_run=self.args.dry_run)
        results = self.pool.map(process_func, self.assertions)
        self.pool.close()
        self.pool.join()

        elapsed_time = time.time() - start_time
        successes = sum(results)
        self.logger.info(f"Processing completed in {elapsed_time:.2f}s. Sent {successes}/{len(self.assertions)} assertions.")
        return successes


def main():
    parser = argparse.ArgumentParser(description="Translate OpenCyc OWL to KM KRL.")
    parser.add_argument("--debug", action="store_true", help="Enable debug output.")
    parser.add_argument("--dry-run", action="store_true", help="Skip sending requests.")
    parser.add_argument("--num-processes", type=int, help="Number of processes.")
    args = parser.parse_args()

    logger = setup_logging(args.debug)
    logger.info("Starting KM translation process.")

    graph = load_ontology(logger)
    object_map = extract_labels_and_ids(graph, logger)

    classes = list(graph.subjects(rdflib.RDF.type, rdflib.OWL.Class))
    individuals = [(s, o) for s, o in graph.subject_objects(rdflib.RDF.type)
                   if o != rdflib.OWL.Class and (o, rdflib.RDF.type, rdflib.OWL.Class) in graph]
    properties = list(graph.subjects(rdflib.RDF.type, rdflib.OWL.ObjectProperty))
    assertions = [("class", uri) for uri in classes] + \
                 [("property", uri) for uri in properties] + \
                 [("individual", (ind_uri, class_uri)) for ind_uri, class_uri in individuals]

    logger.info("Found %d classes, %d individuals, %d properties.", len(classes), len(individuals), len(properties))

    num_processes = args.num_processes if args.num_processes else cpu_count()
    processor = OWLGraphProcessor(graph, object_map, assertions, args, num_processes, logger)
    processor.run()

    total_expressions = len(processor.successfully_sent)
    logger.info("Processed and sent %d KRL expressions in total.", total_expressions)


if __name__ == "__main__":
    main()
