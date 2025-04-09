import argparse
import os
import rdflib
import sexpdata
from datetime import datetime
from multiprocessing import Pool, cpu_count
from config import FIXED_OWL_FILE
from km_syntax import KMSyntaxGenerator
from logging_setup import setup_logging, setup_batch_logger
from ontology_loader import load_ontology
from preprocess import preprocess_owl_file
from rest_client import send_to_km
from utils import extract_labels_and_ids


def pretty_print_sexp(sexp, indent=0):
    """Recursively format an s-expression with proper indentation."""
    if isinstance(sexp, list):
        if not sexp:
            return '()'
        result = '(\n'
        for item in sexp:
            result += '  ' * (indent + 1) + pretty_print_sexp(item, indent + 1) + '\n'
        result += '  ' * indent + ')'
        return result
    elif isinstance(sexp, sexpdata.Symbol):
        return sexp.value()
    else:
        return str(sexp)


def pretty_print(expr):
    """Pretty-print a KM expression string, falling back to original if parsing fails."""
    if expr is None:
        return expr
    try:
        parsed = sexpdata.loads(expr)
        return pretty_print_sexp(parsed)
    except Exception as e:
        return f"Error pretty-printing: {e}\n{expr}"


def process_items(items, item_type, generator, process_id, timestamp, args):
    batch_logger = setup_batch_logger(item_type, process_id, timestamp, args.debug)
    batch_logger.info(f"Starting processing for {item_type} with {len(items)} items.")
    results = []

    if item_type == "class":
        for class_uri in items:
            expr = generator.class_to_km(class_uri)
            result = send_to_km(expr, dry_run=args.dry_run)
            logged_result = pretty_print(result) if args.pretty_print else result
            batch_logger.info(f"Generated: {expr} | Result:\n{logged_result}")
            results.append((expr, result))
    elif item_type == "individual":
        for ind_uri, class_uri in items:
            expr = generator.individual_to_km(ind_uri, class_uri)
            result = send_to_km(expr, dry_run=args.dry_run)
            logged_result = pretty_print(result) if args.pretty_print else result
            batch_logger.info(f"Generated: {expr} | Result:\n{logged_result}")
            results.append((expr, result))
    elif item_type == "property":
        for prop_uri in items:
            expr = generator.property_to_km(prop_uri)
            result = send_to_km(expr, dry_run=args.dry_run)
            logged_result = pretty_print(result) if args.pretty_print else result
            batch_logger.info(f"Generated: {expr} | Result:\n{logged_result}")
            results.append((expr, result))
    batch_logger.info("Processing complete.")
    return results


def main():
    parser = argparse.ArgumentParser(description="Translate OpenCyc OWL to KM KRL.")
    parser.add_argument("--single-thread", action="store_true", help="Run in single-threaded mode for testing.")
    parser.add_argument("--debug", action="store_true", help="Enable debug output to console.")
    parser.add_argument("--dry-run", action="store_true", help="Skip sending requests to KM server.")
    parser.add_argument("--num-processes", type=int, help="Specify the number of processes to fork.")
    parser.add_argument("--pretty-print", action="store_true", help="Enable pretty-printing of KM results in logs.")
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
