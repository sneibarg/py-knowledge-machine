import logging
from collections import defaultdict
import rdflib
from time import perf_counter
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from multiprocessing import Manager
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(filename='progress.log', level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger()
logging.basicConfig(
    filename='compute_depths.log',
    level=logging.INFO,
    format='%(asctime)s [%(processName)s] %(message)s'
)
compute_depths_logger = logging.getLogger(__name__)
OWL = rdflib.Namespace("http://www.w3.org/2002/07/owl#")


def default_list_factory():
    return defaultdict(list)


def build_triple_dict(graph):
    triple_dict = defaultdict(default_list_factory)
    for s, p, o in graph:
        if p != OWL.sameAs:
            triple_dict[s][p].append(o)
    return triple_dict


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


def compute_depths(subjects_chunk, triple_dict):
    """
    Compute depths for a chunk of subjects in an RDF graph.

    Args:
        subjects_chunk: List of subjects to process.
        triple_dict: Dictionary mapping nodes to their predicates and objects.

    Returns:
        Tuple of (local_depths, local_max_depth, local_deepest_nodes).
    """
    local_depths = defaultdict(int)
    local_max_depth = 0
    local_deepest_nodes = set()

    for subject in subjects_chunk:
        compute_depths_logger.info(f"Starting to process subject: {subject}")
        stack = [(subject, 0)]  # (node, depth)
        visited = set()
        node_count = 0

        while stack:
            node, depth = stack.pop()
            if node in visited:
                continue
            visited.add(node)
            node_count += 1

            if node_count % 1000 == 0:
                compute_depths_logger.info(f"Processed {node_count} nodes for subject {subject}")

            if depth > local_depths[node]:
                local_depths[node] = depth
                if depth > local_max_depth:
                    local_max_depth = depth
                    local_deepest_nodes = {node}
                elif depth == local_max_depth:
                    local_deepest_nodes.add(node)

            # Explore children
            for predicate, objects in triple_dict[node].items():
                for obj in objects:
                    if isinstance(obj, (rdflib.URIRef, rdflib.BNode)) and obj not in visited:
                        stack.append((obj, depth + 1))

        # Log completion of the subject
        compute_depths_logger.info(f"Finished processing subject: {subject}, processed {node_count} nodes")

    return local_depths, local_max_depth, local_deepest_nodes


# Enhanced find_deepest_nodes with multiprocessing
def find_deepest_nodes(triple_dict, subjects, num_processes=4):
    start_time = perf_counter()
    depths = defaultdict(int)
    max_depth = 0
    deepest_nodes = set()

    # Split subjects into chunks for parallel processing
    chunk_size = max(1, len(subjects) // num_processes)  # Ensure at least 1 subject per chunk
    subject_chunks = [subjects[i:i + chunk_size] for i in range(0, len(subjects), chunk_size)]

    # Use ProcessPoolExecutor to distribute work
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        results = list(executor.map(partial(compute_depths, triple_dict=triple_dict), subject_chunks))

    # Aggregate results from all processes
    for local_depths, local_max_depth, local_deepest_nodes in results:
        for node, depth in local_depths.items():
            if depth > depths[node]:
                depths[node] = depth
                if depth > max_depth:
                    max_depth = depth
                    deepest_nodes = {node}
                elif depth == max_depth:
                    deepest_nodes.add(node)
        # Consider local deepest nodes from each process
        if local_max_depth == max_depth:
            deepest_nodes.update(local_deepest_nodes)
        elif local_max_depth > max_depth:
            max_depth = local_max_depth
            deepest_nodes = local_deepest_nodes.copy()

    end_time = perf_counter()
    logger.info(f"find_deepest_nodes took {end_time - start_time:.4f} seconds")
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


# Main function with performance counter for final computation
def print_graph_step_by_step(graph, num_threads=4, num_processes=4):
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

    # Find deepest nodes with multiprocessing
    max_depth, deepest_nodes = find_deepest_nodes(triple_dict, subjects, num_processes)
    logger.info(f"Deepest nodes at depth {max_depth}: {', '.join(node_labels[node] for node in deepest_nodes)}")

    # Process subjects in parallel and collect results
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

        # Final computation with performance counter
        final_start = perf_counter()
        results = sorted(output_list, key=lambda x: str(x[0]))
        for subject, local_output in results:
            for line, depth in sorted(local_output, key=lambda x: x[1]):
                print(line)
        final_end = perf_counter()
        logger.info(f"Final computation (sorting and printing) took {final_end - final_start:.4f} seconds")

    process_end = perf_counter()
    logger.info(f"Total processing time: {process_end - process_start:.4f} seconds")


# Entry point
if __name__ == "__main__":
    start_time = perf_counter()
    logger.info("Starting to parse OWL file")
    g = rdflib.Graph()
    g.parse("opencyc-owl/opencyc-2012-05-10_fixed.owl", format="xml")
    logger.info(f"Finished parsing OWL file, took {perf_counter() - start_time:.4f} seconds")
    print_graph_step_by_step(g, num_threads=8, num_processes=32)
    logger.info(f"Total execution time: {perf_counter() - start_time:.4f} seconds")