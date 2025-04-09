import argparse
import os
import sys
import rdflib
from datetime import datetime
from multiprocessing import Pool, cpu_count
from rdflib import Literal, URIRef, BNode, RDFS
from preprocess import preprocess_owl_file
from ontology_loader import load_ontology
from km_syntax import KMSyntaxGenerator
from rest_client import send_to_km
from logging_setup import setup_logging, setup_batch_logger
from config import FIXED_OWL_FILE


def process_items(items, item_type, generator, process_id, timestamp, args):
    batch_logger = setup_batch_logger(item_type, process_id, timestamp, args.debug)
    batch_logger.info(f"Starting processing for {item_type} with {len(items)} items.")
    results = []
    if item_type == "class":
        for class_uri in items:
            expr = generator.class_to_km(class_uri)
            result = send_to_km(expr, dry_run=args.dry_run)
            batch_logger.info(f"Generated: {expr} | Result: {result}")
            results.append((expr, result))
    elif item_type == "individual":
        for ind_uri, class_uri in items:
            expr = generator.individual_to_km(ind_uri, class_uri)
            result = send_to_km(expr, dry_run=args.dry_run)
            batch_logger.info(f"Generated: {expr} | Result: {result}")
            results.append((expr, result))
    elif item_type == "property":
        for prop_uri in items:
            expr = generator.property_to_km(prop_uri)
            result = send_to_km(expr, dry_run=args.dry_run)
            batch_logger.info(f"Generated: {expr} | Result: {result}")
            results.append((expr, result))
    batch_logger.info("Processing complete.")
    return results


def print_graph_step_by_step(graph):
    labels = {}
    for subject, obj in graph.subject_objects( RDFS.label):
        if subject not in labels:
            labels[subject] = str(obj)

    def get_label(node):
        if node in labels:
            return f"'{labels[node]}'"  # Quote labels to distinguish them
        elif isinstance(node, URIRef):
            try:
                qname = graph.qname(node)
                return qname
            except RecursionError:
                print(f"Warning: RecursionError for node {node}, using full URI instead.")
                return str(node)  # Fallback to full URI
        else:
            return str(node)  # For blank nodes or literals

    visited = set()

    def dfs(node, depth=0):
        if node in visited:
            return
        visited.add(node)

        # Get all outgoing triples for this node
        triples = list(graph.predicate_objects(node))
        if not triples:
            return  # Skip nodes with no properties

        # Print the node being visited with its label
        node_str = get_label(node)
        print("  " * depth + f"Visiting {node_str}")

        # Process each property (predicate-object pair)
        for predicate, obj in triples:
            pred_str = get_label(predicate)

            # Handle the object based on its type
            if isinstance(obj, Literal):
                obj_str = f'"{obj}"'  # Literals in quotes
            else:
                obj_str = get_label(obj)  # Resources use label or identifier

            # Print the edge
            print("  " * (depth + 1) + f"{pred_str} -> {obj_str}")

            # Recurse on resource objects (URIRef or BNode)
            if isinstance(obj, (URIRef, BNode)):
                dfs(obj, depth + 1)

    # Step 4: Start traversal from all unvisited subjects
    for subject in graph.subjects():
        if subject not in visited:
            dfs(subject)


def main():
    parser = argparse.ArgumentParser(description="Translate OpenCyc OWL to KM KRL.")
    parser.add_argument("--single-thread", action="store_true", help="Run in single-threaded mode for testing.")
    parser.add_argument("--debug", action="store_true", help="Enable debug output to console.")
    parser.add_argument("--dry-run", action="store_true", help="Skip sending requests to KM server.")
    args = parser.parse_args()

    logger = setup_logging("main", args.debug)
    logger.info("Starting KM translation process.")

    if not os.path.exists(FIXED_OWL_FILE):
        preprocess_owl_file()

    graph = load_ontology()
    print_graph_step_by_step(graph)
    sys.exit(1)
    km_generator = KMSyntaxGenerator(graph)

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
        num_processes = cpu_count()
        logger.info(f"Running in multi-threaded mode with {num_processes} processes.")

        batch_size = max(1, len(classes) // num_processes)
        class_batches = [classes[i:i + batch_size] for i in range(0, len(classes), batch_size)]
        batch_size = max(1, len(individuals) // num_processes)
        individual_batches = [individuals[i:i + batch_size] for i in range(0, len(individuals), batch_size)]
        batch_size = max(1, len(properties) // num_processes)
        property_batches = [properties[i:i + batch_size] for i in range(0, len(properties), batch_size)]

        with Pool(processes=num_processes) as pool:
            # Create task lists with all arguments
            property_tasks = [(batch, "property", km_generator, i, timestamp, args)
                              for i, batch in enumerate(property_batches)]
            property_results = pool.starmap(process_items, property_tasks)

            class_tasks = [(batch, "class", km_generator, i, timestamp, args)
                           for i, batch in enumerate(class_batches)]
            class_results = pool.starmap(process_items, class_tasks)

            individual_tasks = [(batch, "individual", km_generator, i, timestamp, args)
                                for i, batch in enumerate(individual_batches)]
            individual_results = pool.starmap(process_items, individual_tasks)

    # Log summary
    total_expressions = (sum(len(batch) for batch in property_results) +
                         sum(len(batch) for batch in class_results) +
                         sum(len(batch) for batch in individual_results))
    logger.info(f"Processed and sent {total_expressions} KRL expressions in total.")


if __name__ == "__main__":
    main()
