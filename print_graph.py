import logging
from collections import defaultdict
import rdflib
from time import perf_counter
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from multiprocessing import Manager

# Configure logging
logging.basicConfig(filename='progress.log', level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger()

# Define OWL namespace
OWL = rdflib.Namespace("http://www.w3.org/2002/07/owl#")


# Factory function for defaultdict
def default_list_factory():
    return defaultdict(list)


# Build a dictionary of triples excluding owl:sameAs
def build_triple_dict(graph):
    triple_dict = defaultdict(default_list_factory)
    for s, p, o in graph:
        if p != OWL.sameAs:
            triple_dict[s][p].append(o)
    return triple_dict


# Get a readable label for a node
def get_node_label(node, graph, label_dict):
    if node in label_dict:
        return f"'{label_dict[node]}'"
    elif isinstance(node, rdflib.URIRef):
        try:
            return graph.qname(node)
        except Exception:
            return str(node)
    elif isinstance(node, rdflib.BNode):
        return str(node)
    elif isinstance(node, rdflib.Literal):
        return f'"{node}"'
    else:
        return str(node)


# Preprocessing step to find the deepest nodes
def find_deepest_nodes(triple_dict, subjects):
    depths = defaultdict(int)  # Store maximum depth for each node
    max_depth = 0
    deepest_nodes = set()

    for subject in subjects:
        stack = [(subject, 0)]  # (node, depth)
        visited = set()

        while stack:
            node, depth = stack.pop()
            if node in visited:
                continue
            visited.add(node)

            # Update depth if this path is deeper
            if depth > depths[node]:
                depths[node] = depth
                if depth > max_depth:
                    max_depth = depth
                    deepest_nodes = {node}
                elif depth == max_depth:
                    deepest_nodes.add(node)

            # Explore children
            for predicate, objects in triple_dict[node].items():
                for obj in objects:
                    if isinstance(obj, (rdflib.URIRef, rdflib.BNode)) and obj not in visited:
                        stack.append((obj, depth + 1))

    return max_depth, deepest_nodes


# Process a single subject and generate output
def process_subject(subject, triple_dict, node_labels, output_list, counter, lock, total_subjects):
    stack = [(subject, 0)]  # (node, depth)
    visited = set()
    local_output = []

    while stack:
        node, depth = stack.pop()
        if node in visited:
            continue
        visited.add(node)

        node_str = node_labels[node]
        local_output.append(("  " * depth + f"Visiting {node_str}", depth))

        for predicate, objects in triple_dict[node].items():
            pred_str = node_labels[predicate]
            for obj in objects:
                obj_str = node_labels[obj]
                local_output.append(("  " * (depth + 1) + f"{pred_str} -> {obj_str}", depth + 1))
                if isinstance(obj, (rdflib.URIRef, rdflib.BNode)):
                    stack.append((obj, depth + 1))

    output_list.append((subject, local_output))

    with lock:
        counter.value += 1
        if counter.value % 100 == 0:
            logger.info(f"Processed {counter.value} out of {total_subjects} subjects")


# Main function to print the graph step-by-step
def print_graph_step_by_step(graph, num_threads=4):
    process_start = perf_counter()

    # Build triple dictionary and label dictionary
    triple_dict = build_triple_dict(graph)
    label_dict = {s: str(o) for s, p, o in graph.triples((None, rdflib.RDFS.label, None)) if
                  isinstance(o, rdflib.Literal)}

    # Generate labels for all nodes
    all_nodes = set(graph.subjects()) | set(graph.predicates()) | set(graph.objects())
    node_labels = {node: get_node_label(node, graph, label_dict) for node in all_nodes}

    # Get all subjects
    subjects = list(graph.subjects())
    total_subjects = len(subjects)
    logger.info(f"Total subjects to process: {total_subjects}")

    # Preprocessing: Find deepest nodes
    max_depth, deepest_nodes = find_deepest_nodes(triple_dict, subjects)
    logger.info(f"Deepest nodes at depth {max_depth}: {', '.join(node_labels[node] for node in deepest_nodes)}")

    # Process subjects in parallel
    with Manager() as manager:
        output_list = manager.list()
        counter = manager.Value('i', 0)
        lock = manager.Lock()

        logger.info("Starting to process subjects")
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            executor.map(
                partial(process_subject, triple_dict=triple_dict, node_labels=node_labels, output_list=output_list,
                        counter=counter, lock=lock, total_subjects=total_subjects),
                subjects
            )
        logger.info("Finished processing subjects")

        # Print sorted results
        results = sorted(output_list, key=lambda x: str(x[0]))
        for subject, local_output in results:
            for line, depth in sorted(local_output, key=lambda x: x[1]):
                print(line)

    process_end = perf_counter()
    logger.info(f"Processing time: {process_end - process_start:.4f} seconds")


# Entry point
if __name__ == "__main__":
    start_time = perf_counter()
    logger.info("Starting to parse OWL file")
    g = rdflib.Graph()
    g.parse("opencyc-owl/opencyc-2012-05-10_fixed.owl", format="xml")
    logger.info(f"Finished parsing OWL file, took {perf_counter() - start_time:.4f} seconds")
    print_graph_step_by_step(g, num_threads=8)
    logger.info(f"Total execution time: {perf_counter() - start_time:.4f} seconds")