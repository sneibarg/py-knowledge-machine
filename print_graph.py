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


def process_subject(subject, triple_dict, node_labels, output_list, counter, lock, total_subjects, max_depth=50):
    stack = [(subject, 0)]
    visited = set()
    local_output = []

    while stack:
        node, depth = stack.pop()
        # Limit traversal depth
        if depth > max_depth:
            logger.warning(f"Reached maximum depth {max_depth} for node {node}")
            continue
        if node in visited:
            continue
        visited.add(node)

        start_process = perf_counter()
        node_str = node_labels[node]
        local_output.append(("  " * depth + f"Visiting {node_str}", depth))
        # Count triples for this node
        num_triples = sum(len(objects) for objects in triple_dict[node].values())
        logger.info(f"Processing node {node_str} at depth {depth} with {num_triples} triples")

        for predicate, objects in triple_dict[node].items():
            if predicate != OWL.sameAs:
                pred_str = node_labels[predicate]
                # Warn if a predicate has many objects
                if len(objects) > 1000:
                    logger.warning(f"Node {node_str} has {len(objects)} objects for predicate {pred_str}")
                for obj in objects:
                    obj_str = node_labels[obj]
                    local_output.append(("  " * (depth + 1) + f"{pred_str} -> {obj_str}", depth + 1))
                    if isinstance(obj, (rdflib.URIRef, rdflib.BNode)):
                        stack.append((obj, depth + 1))
        elapsed = perf_counter() - start_process
        logger.info(f"Visited node {node_str} at depth {depth} in {elapsed:.4f} seconds")

    output_list.append((subject, local_output))

    with lock:
        counter.value += 1
        if counter.value % 100 == 0:
            logger.info(f"Processed {counter.value} out of {total_subjects} subjects")


def print_graph_step_by_step(graph, num_threads=4, max_depth=50):
    process_start = perf_counter()
    triple_dict = build_triple_dict(graph)
    label_dict = {}
    for s, p, o in graph.triples((None, rdflib.RDFS.label, None)):
        if isinstance(o, rdflib.Literal):
            label_dict[s] = str(o)

    all_nodes = set(graph.subjects()) | set(graph.predicates()) | set(graph.objects())
    node_labels = {node: get_node_label(node, graph, label_dict) for node in all_nodes}

    subjects = list(graph.subjects())
    total_subjects = len(subjects)
    logger.info(f"Total subjects to process: {total_subjects}")

    with Manager() as manager:
        output_list = manager.list()
        counter = manager.Value('i', 0)
        lock = manager.Lock()

        logger.info("Starting to process subjects")
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            executor.map(
                partial(process_subject, triple_dict=triple_dict, node_labels=node_labels, output_list=output_list,
                        counter=counter, lock=lock, total_subjects=total_subjects, max_depth=max_depth),
                subjects
            )
        logger.info("Finished processing subjects")

        results = sorted(output_list, key=lambda x: str(x[0]))
        for subject, local_output in results:
            for line, depth in sorted(local_output, key=lambda x: x[1]):
                print(line)

    process_end = perf_counter()
    process_time = process_end - process_start
    logger.info(f"Processing time: {process_time:.4f} seconds")


if __name__ == "__main__":
    start_time = perf_counter()
    logger.info("Starting to parse OWL file")
    g = rdflib.Graph()
    g.parse("opencyc-owl/opencyc-2012-05-10_fixed.owl", format="xml")
    parse_time = perf_counter() - start_time
    logger.info(f"Finished parsing OWL file, took {parse_time:.4f} seconds")
    # Reduced num_threads to 4 and added max_depth
    print_graph_step_by_step(g, num_threads=4, max_depth=50)
    total_time = perf_counter() - start_time
    logger.info(f"Total execution time: {total_time:.4f} seconds")
