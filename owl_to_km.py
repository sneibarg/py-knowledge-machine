import argparse
import os
import time
from multiprocessing import Pool, cpu_count, Manager, current_process
from functools import partial
import rdflib
from core import setup_logging, send_to_km, FIXED_OWL_FILE
from km_syntax import KMSyntaxGenerator
from ontology_loader import load_ontology

logger = None
worker_logger = None


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
    worker_logger.info("Initialized worker.")


def translate_assertions(assertion_list, km_generator):
    translated_assertions = []
    for assertion in assertion_list:
        translated_assertions.append(translate_assertion(assertion, km_generator))
    return translated_assertions


def translate_assertion(assertion, km_generator):
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


def process_assertion(km_generator, assertion, successfully_sent, dry_run):
    """Process a single assertion with dependency handling."""
    try:
        worker_logger.info(f"Getting referenced assertions for assertion: {assertion}")
        refs = km_generator.get_referenced_assertions(assertion)
        worker_logger.info(f"Found {str(len(refs))} referenced assertions for assertion: {assertion}.")
        for ref in refs:
            if ref not in successfully_sent:
                worker_logger.info("Processing dependency: %s", ref)
                process_assertion(km_generator, ref, successfully_sent, dry_run)

        result = send_to_km(assertion, dry_run=dry_run)
        if result.get("success", False):
            successfully_sent[assertion] = assertion
            worker_logger.info("Successfully sent assertion: %s...", assertion)
            return True
        else:
            worker_logger.error("Failed to send assertion: %s", result)
            return False
    except Exception as e:
        worker_logger.error("Error processing assertion: %s", str(e))
        return False


def extract_labels_and_ids(graph, logger):
    """Extract labels and external IDs from the graph."""
    child_logger = logger.getChild('LabelsExtractor')
    child_logger.info("Extracting labels and IDs from graph...")
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


class OWLGraphProcessor:
    def __init__(self, km_generator, graph, object_map, assertions, args, num_workers, logger):
        self.graph = graph
        self.object_map = object_map
        self.args = args
        self.assertions = assertions
        self.manager = Manager()
        self.successfully_sent = self.manager.dict()
        self.pool = Pool(processes=num_workers, initializer=init_worker, initargs=(args.debug,))
        self.logger = logger.getChild('OWLGraphProcessor')
        self.km_generator = km_generator
        self.logger.info("Initialized with %d assertions.", len(assertions))

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
        self.logger.info("Processing completed in %.2fs. Sent %d/%d assertions.", elapsed_time, successes,
                         len(self.assertions))
        return successes


def parse_arguments():
    parser = argparse.ArgumentParser(description="Translate OpenCyc OWL to KM KRL.")
    parser.add_argument("--debug", action="store_true", help="Enable debug output.")
    parser.add_argument("--dry-run", action="store_true", help="Skip sending requests to KM server.")
    parser.add_argument("--num-processes", type=int, help="Number of processes to use.")
    return parser.parse_args()


def main():
    global logger
    args = parse_arguments()
    num_processes = args.num_processes if args.num_processes else cpu_count()
    logger = setup_logging(args.debug)

    if not os.path.exists(FIXED_OWL_FILE):
        logger.error("Fixed OWL file not found at %s.", FIXED_OWL_FILE)
        return

    logger.info("Starting KM translation process.")
    graph = load_ontology(logger)
    object_map = extract_labels_and_ids(graph, logger)
    km_generator = KMSyntaxGenerator(graph, object_map, logger)
    assertions = preprocess(graph)
    translated_assertions = translate_assertions(assertions, km_generator)
    processor = OWLGraphProcessor(km_generator, graph, object_map, translated_assertions, args, num_processes, logger)
    processor.run()

    total_expressions = len(processor.successfully_sent)
    logger.info("Processed and sent %d KRL expressions in total.", total_expressions)


if __name__ == "__main__":
    main()
