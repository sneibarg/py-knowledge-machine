import argparse
import json
import logging
import os
import re
import sys
import time
import requests
import rdflib

from typing import Tuple, Optional, List
from multiprocessing import Pool, Manager, current_process
from processor.dataset.DatasetProcessor import DatasetProcessor
from service.HuggingFaceDatasetService import HuggingFaceDatasetService
from service.KMSyntaxGenerator import KMSyntaxGenerator
from service.LoggingService import LoggingService
from processor.owl.OWLGraphProcessor import OWLGraphProcessor
from service.OpenCycService import CYC_ANNOT_LABEL, CYC_BASES, is_cyc_id, OpenCycService, FIXED_OWL_FILE, TINY_OWL_FILE

BASE_DIR = os.getcwd()
LOG_DIR = os.path.join(BASE_DIR, "runtime/logs")
logging_service = LoggingService(LOG_DIR, "OWL-to-KM")
logger = logging_service.setup_logging(False)
open_cyc_service = OpenCycService(logger)
adapter = None
session = None
worker_logger = None
manager = None
pool = None
successfully_sent = None
failed_assertions = None

os.makedirs(LOG_DIR, exist_ok=True)

CAMEL_CASE_PATTERN = r"[A-Z][a-z]+([A-Z][a-z]*)*"
HYPHEN = r"-"
WORD_PATTERN = r"\w+"
START_ANCHOR = r"^"
END_ANCHOR = r"$"
FUNCTION_GROUP = "function"
ACTIVITY_GROUP = "activity"

PATTERN = (
        START_ANCHOR
        + r"(?P<" + FUNCTION_GROUP + r">" + CAMEL_CASE_PATTERN + r")"
        + HYPHEN
        + r"(?P<" + ACTIVITY_GROUP + r">" + WORD_PATTERN + r")"
        + END_ANCHOR
)

HAS_PATTERN = (
        START_ANCHOR
        + r"(?P<words>(\w+\s)*)"
        + r"has"
        + END_ANCHOR
)


def lambda_match(input_str, pattern, anon_dict):
    match = re.match(pattern, input_str)
    if match:
        named_captures = {k: v for k, v in match.groupdict().items() if v is not None}
        return {**anon_dict, **named_captures}
    else:
        return anon_dict


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


def process_zst(args: Tuple[str, bool, bool, bool, HuggingFaceDatasetService, DatasetProcessor, Optional[int]]) -> None:
    local_path, print_contents, summarize, rank, data_agent, dataset_processor, record_index = args
    dataset = data_agent.dump_zstd(local_path)
    if not dataset:
        logger.error(f"No records found in file: {local_path}")
        return

    if record_index is not None:
        if not isinstance(record_index, int) or record_index < 0:
            logger.error(f"Invalid record index: {record_index}. Must be a non-negative integer.")
            return
        if record_index >= len(dataset):
            logger.error(f"Record index {record_index} out of range. File has {len(dataset)} records.")
            return
        logger.info(f"Processing record at index {record_index} in file: {local_path}")
        dataset_processor.process_record(dataset[record_index], print_contents, summarize, rank, record_index)
    else:
        logger.info(f"Processing all records in file: {local_path}")
        for i, row in enumerate(dataset):
            dataset_processor.process_record(row, print_contents, summarize, rank, i)


def worker_process(file_chunk: List[Tuple[str, str]],
                   print_contents: bool,
                   summarize: bool,
                   rank: bool,
                   data_agent: HuggingFaceDatasetService,
                   dataset_processor: DatasetProcessor,
                   record_index: Optional[int]) -> None:
    for relative_path, local_path in file_chunk:
        worker_logger.info(f"Processing file: {relative_path}")
        try:
            process_zst((local_path, print_contents, summarize, rank, data_agent, dataset_processor, record_index))
        except Exception as e:
            worker_logger.error(f"Error processing file {relative_path}: {e}")


def init_worker(debug):
    global adapter, session, worker_logger

    worker_logger = logger.getChild(f'Worker.{current_process().name}')
    worker_logger.setLevel(logging.DEBUG if debug else logging.INFO)

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=10,
        pool_maxsize=10,
        max_retries=0
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    worker_logger.info("Initialized worker with session.")


def translate_assertions(assertion_list, km_generator):
    translated_assertions = []
    for assertion in assertion_list:
        translated_assertions.append(translate(assertion, km_generator))
    return translated_assertions


def translate(assertion, km_generator):
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


def process_assertion(assertion, dry_run, km_generator, km_service):
    if assertion in successfully_sent:
        return True

    all_deps_successful = True
    for ref in km_generator.get_referenced_assertions(assertion):
        if ref not in successfully_sent and not process_assertion(ref, dry_run):
            all_deps_successful = False
            if ref not in successfully_sent and ref not in failed_assertions:
                failed_assertions[ref] = "dependency_failure"

    if not all_deps_successful:
        failed_assertions[assertion] = "dependency_failure"
        return False

    try:
        result = km_service.send_to_km(assertion, dry_run=dry_run)
        if result.get("success", False):
            successfully_sent[assertion] = assertion
            return True
        else:
            failed_assertions[assertion] = "processing_failure"
            return False
    except Exception as e:
        failed_assertions[assertion] = f"exception: {str(e)}"
        return False


def extract_labels_and_ids(graph, parent_logger):
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


def parse_arguments():
    parser = argparse.ArgumentParser(description="Inspect Hugging Face dataset files")
    parser.add_argument('repo_id', type=str, help='Dataset repository ID (e.g., mlfoundations/dclm-baseline-1.0)')
    parser.add_argument('--local-snapshot-dir', type=str, help='Path to local snapshot directory')
    parser.add_argument('--files', type=str, nargs='*', help='Specific files to inspect')
    parser.add_argument('--cache-dir', type=str, default=None, help='Cache directory for downloaded files')
    parser.add_argument('--retry-count', type=int, default=3, help='Number of retry attempts for downloads')
    parser.add_argument('--print-contents', action='store_true', help='Print contents of each file')
    parser.add_argument('--summarize', action='store_true', help='Log Mistral one-shot summary.')
    parser.add_argument('--rank', action='store_true', help='Ten responses will be generated and ranked.')
    parser.add_argument('--num-procs', type=int, default=1, help='Number of processes to use for parallel processing')
    parser.add_argument('--record-index', type=int, default=None, help='Index of the record to process in each file')
    parser.add_argument("--dry-run", action="store_true", help="Skip sending requests to KM server.")
    parser.add_argument("--translate-only", action="store_true", help="Translate and log only.")
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()
    if args.num_procs < 1:
        print("Error: --num-procs must be at least 1", file=sys.stderr)
        sys.exit(1)
    return args


def main():
    args = parse_arguments()
    num_processes = args.num_processes if args.num_processes else 1

    if args.debug is True:
        logger.setLevel(logging.DEBUG)

    if not os.path.exists(FIXED_OWL_FILE):
        logger.error("Fixed OWL file not found at %s.", FIXED_OWL_FILE)
        sys.exit(1)

    if num_processes > 1:
        global pool, manager, failed_assertions, successfully_sent
        manager = Manager()
        failed_assertions = manager.dict()
        successfully_sent = manager.dict()
        pool = Pool(processes=num_processes, initializer=init_worker, initargs=(args.debug,))

    processing_start = time.time()
    logger.info("Starting KM translation process.")
    owl_graph_processor = OWLGraphProcessor(logger,
                                            TINY_OWL_FILE,
                                            pool,
                                            open_cyc_service.preprocess_cyc_file,
                                            is_cyc_id,
                                            args)
    owl_graph_processor.set_annotation_label(CYC_ANNOT_LABEL)
    owl_graph_processor.set_bases(CYC_BASES)
    object_map = extract_labels_and_ids(owl_graph_processor.graph, logger)

    km_generator = KMSyntaxGenerator(owl_graph_processor.graph, object_map, logger)
    assertions = preprocess(owl_graph_processor.graph)
    translated_assertions = translate_assertions(assertions, km_generator)
    logger.info(f"Translated {str(len(translated_assertions))} in {str(int(time.time() - processing_start))} seconds.")
    if args.translate_only:
        for assertion in translated_assertions:
            logger.info(
                "-------------------------------------------------------------------------------------------------")
            logger.info(
                "-------------------------------------------------------------------------------------------------")
            logger.info(json.dumps(assertion, indent=2))
        sys.exit(0)

    total_expressions = len(owl_graph_processor.successfully_sent)
    logger.info("Processed and sent %d KRL expressions in total.", total_expressions)


if __name__ == "__main__":
    main()
