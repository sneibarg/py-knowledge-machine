import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime

import requests
import requests.exceptions
from multiprocessing import Pool, current_process
from typing import List, Tuple, Optional
from huggingface_hub import HfApi
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from HuggingFaceDatasetService import HuggingFaceDatasetService

logger = None
nlp_api_url = "http://malboji:8069/nlp/relations"
mistral_api_url = "http://dragon:11435/api/generate"
url_prompt = ("I am your automated ontology editor, and I am reviewing a Uniform Resource Locator."
              "I will generate a one sentence response describing the URL. The URL is: ")
ontologist_prompt = ("I am your automated ontology editor, and I am reviewing data step by step "
                     "to validate and assert to our knowledge and understanding. "
                     "I will ignore formalities, not be verbose, and respond with only the facts. "
                     "The following text you have given me is: ")

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(
    pool_connections=10,
    pool_maxsize=10,
    max_retries=0
)
session.mount('http://', adapter)
session.mount('https://', adapter)


def setup_logging(log_directory, debug=False):
    global logger
    """Configure logging with a single file for all logs."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pid = os.getpid()
    log_file = os.path.join(log_directory, f"application_{timestamp}_{pid}.log")
    logging.getLogger('').handlers = []
    logger = logging.getLogger('OWL-to-KM')
    logger.setLevel(logging.INFO if not debug else logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s [PID %(process)d] [%(levelname)s] [%(name)s] %(message)s")
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if debug:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        if sys.platform.startswith('win'):
            console_handler.stream = sys.stdout
            console_handler.stream.reconfigure(encoding='utf-8', errors='replace')
        logger.addHandler(console_handler)

    return logger


def parse_options():
    """Parse command-line arguments and inspect the dataset files in parallel."""
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
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()
    if args.num_procs < 1:
        print("Error: --num-procs must be at least 1", file=sys.stderr)
        sys.exit(1)
    return args


def process(args) -> None:
    global logger
    files_to_process: List[Tuple[str, str]] = []
    data_agent = HuggingFaceDatasetService(logger)
    dataset_processor = DatasetProcessor(logger)
    if args.local_snapshot_dir and os.path.isdir(args.local_snapshot_dir):
        try:
            refs_path = os.path.join(args.local_snapshot_dir, 'refs', 'main')
            if not os.path.isfile(refs_path):
                raise FileNotFoundError(f"'refs/main' not found in {args.local_snapshot_dir}")
            with open(refs_path, 'r') as f:
                commit_hash = f.read().strip()

            snapshot_dir = os.path.join(args.local_snapshot_dir, 'snapshots', commit_hash)
            if not os.path.isdir(snapshot_dir):
                raise FileNotFoundError(f"Snapshot directory {snapshot_dir} does not exist")

            for root, dirs, files in os.walk(snapshot_dir):
                for file in files:
                    if file.endswith('.jsonl.zst'):
                        full_path = os.path.join(root, file)
                        relative_path = os.path.relpath(full_path, snapshot_dir)
                        files_to_process.append((relative_path, full_path))

            if not files_to_process:
                raise FileNotFoundError(f"No .jsonl.zst files found in {snapshot_dir} or its subdirectories")

            files_to_process.sort(key=lambda x: data_agent.get_sort_key(x[0]))
            logger.info(f"Found {len(files_to_process)} .jsonl.zst files in the local snapshot.")

            if args.files:
                specified_files = set(args.files)
                files_to_process = [f for f in files_to_process if f[0] in specified_files]
                if len(files_to_process) < len(specified_files):
                    logging.warning("Some specified files were not found in the local snapshot.")
        except Exception as e:
            logger.error(f"Error accessing local snapshot directory: {e}")
            logger.info("Falling back to downloading files.")
            files_to_process = []

    if not files_to_process:
        logger.info("No local files available. Proceeding to download from the repository.")
        api = HfApi()
        repo_files = api.list_repo_files(repo_id=args.repo_id, repo_type='dataset')
        jsonl_files = [f for f in repo_files if f.endswith('.jsonl.zst')]
        if args.files:
            files_to_process_remote = [f for f in args.files if f in jsonl_files]
            if len(files_to_process_remote) < len(args.files):
                logging.warning("Some specified files were not found in the dataset repository.")
        else:
            files_to_process_remote = jsonl_files

        files_to_process_remote.sort(key=lambda x: data_agent.get_sort_key(x))
        for file_path in files_to_process_remote:
            try:
                local_path = data_agent.download(args.repo_id, file_path, args.cache_dir, args.retry_count)
                files_to_process.append((file_path, local_path))
            except Exception as e:
                logger.error(f"Failed to download {file_path}: {e}")

    total_files = len(files_to_process)
    if total_files == 0:
        logger.error("No files to process. Check your local snapshot directory or repository settings.")
        return

    file_chunks = data_agent.split_files(files_to_process, args.num_procs)
    logger.info(f"Distributing {total_files} files across {args.num_procs} processes")

    if args.num_procs == 1:
        global worker_logger
        worker_logger = logger
        worker_process(files_to_process, args.print_contents, args.summarize, args.rank, args.record_index)
    else:
        with Pool(processes=args.num_procs, initializer=init_worker, initargs=(args.debug,)) as pool:
            pool.starmap(worker_process,
                         [(chunk, args.print_contents, args.summarize, args.rank, args.record_index)
                          for chunk in file_chunks])

    logger.info('Inspection completed')


def worker_process(file_chunk: List[Tuple[str, str]], print_contents: bool, summarize: bool, rank: bool,
                   data_agent: HuggingFaceDatasetService, record_index: Optional[int]) -> None:
    """Process a chunk of files in a worker process."""
    global worker_logger
    for relative_path, local_path in file_chunk:
        worker_logger.info(f"Processing file: {relative_path}")
        try:
            process_zst((local_path, print_contents, summarize, rank, data_agent, record_index))
        except Exception as e:
            worker_logger.error(f"Error processing file {relative_path}: {e}")


def init_worker(debug):
    """Initialize worker process with a logger and session."""
    global logger, worker_logger, session
    if logger is None:
        logger = setup_logging(debug)
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


class DatasetProcessor:
    def __init__(self, parent_logger, max_shots):
        self.logger = parent_logger
        self.max_shots = max_shots

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.exceptions.ConnectionError,
                                       requests.exceptions.Timeout,
                                       requests.exceptions.HTTPError)),
        before_sleep=lambda retry_state: logger.debug(
            f"Retrying stanford_relations (attempt {retry_state.attempt_number}) after {retry_state.idle_for}s"
        )
    )
    def stanford_relations(self, data: str) -> dict:
        response = None
        headers = {'Content-Type': 'application/json'}
        if not isinstance(data, str):
            self.logger.error(f"Invalid input type for stanford_relations: {type(data)}")
            return {}

        try:
            start_time = time.time()
            response = session.post(
                nlp_api_url,
                data=data.encode('utf-8', errors='replace'),
                headers=headers,
                timeout=(120, 360)
            )
            response.raise_for_status()
            end_time = time.time()
            duration = end_time - start_time
            self.logger.info(f"REST call to {nlp_api_url} took {duration:.3f} seconds")
            try:
                return response.json()
            except json.JSONDecodeError as e:
                self.logger.error(f"Invalid JSON response from {nlp_api_url}: {e}")
                return {}
        except requests.exceptions.Timeout as e:
            self.logger.error(f"Timeout error contacting {nlp_api_url}: {e}")
            raise
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"Connection error contacting {nlp_api_url}: {e}")
            raise
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                self.logger.error(f"Rate limit exceeded for {nlp_api_url}: {e}")
            elif response.status_code >= 500:
                self.logger.error(f"Server error from {nlp_api_url} (status {response.status_code}): {e}")
                raise
            else:
                self.logger.error(f"HTTP error from {nlp_api_url} (status {response.status_code}): {e}")
            return {}
        except Exception as e:
            self.logger.error(f"Unexpected error in stanford_relations: {e}")
            return {}
        finally:
            time.sleep(0.1)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.exceptions.ConnectionError,
                                       requests.exceptions.Timeout,
                                       requests.exceptions.HTTPError)),
        before_sleep=lambda retry_state: logger.debug(
            f"Retrying mistral_one_shot (attempt {retry_state.attempt_number}) after {retry_state.idle_for}s"
        )
    )
    def mistral_one_shot(self, text: str, base_prompt: str) -> Optional[str]:
        response = None
        safe_text = text.encode('utf-8', errors='replace').decode('utf-8') if text else ""
        full_prompt = f"<s>[INST] {base_prompt} {safe_text} [/INST]"
        payload = {
            "model": "mistral:7b-instruct-q4_0",
            "prompt": full_prompt,
            "stream": False
        }

        try:
            start_time = time.time()
            response = session.post(
                mistral_api_url,
                json=payload,
                timeout=(120, 360)
            )
            response.raise_for_status()
            end_time = time.time()
            duration = end_time - start_time
            self.logger.info(f"REST call to {mistral_api_url} took {duration:.3f} seconds")
            try:
                json_response = response.json()
                if 'response' not in json_response:
                    worker_logger.error(f"Missing 'response' key in JSON from {mistral_api_url}")
                    return None
                return json_response['response']
            except json.JSONDecodeError as e:
                worker_logger.error(f"Invalid JSON response from {mistral_api_url}: {e}")
                return None
        except requests.exceptions.Timeout as e:
            self.logger.error(f"Timeout error contacting {mistral_api_url}: {e}")
            raise
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"Connection error contacting {mistral_api_url}: {e}")
            raise
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                self.logger.error(f"Rate limit exceeded for {mistral_api_url}: {e}")
            elif response.status_code >= 500:
                self.logger.error(f"Server error from {mistral_api_url} (status {response.status_code}): {e}")
                raise
            else:
                self.logger.error(f"HTTP error from {mistral_api_url} (status {response.status_code}): {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error in mistral_one_shot: {e}")
            return None
        finally:
            time.sleep(0.1)

    def summarize_text(self, text: str, rank: bool, key_terms: Optional[set] = None) -> tuple[Optional[str], dict]:
        if rank and key_terms is not None:
            summaries = []
            for shot in range(self.max_shots):
                summary, relations = self.generate_one_shot(text, ontologist_prompt)
                relations = self.stanford_relations(summary)
                summary_nouns = set()
                if "parseTree" in str(relations):
                    for sentence in relations['sentences']:
                        tokens = sentence.get('tokens', [])
                        for token in tokens:
                            word = token.get('word')
                            pos = token.get('pos', '')
                            if pos.startswith('N') and word is not None:
                                summary_nouns.add(word.lower())
                score = len(summary_nouns.intersection(key_terms))
                summaries.append((summary, score))

            summaries.sort(key=lambda x: x[1], reverse=True)
            self.logger.info("Ranked Summaries:")
            for i, (summary, score) in enumerate(summaries, 1):
                worker_logger.info(f"Rank {i} (Score: {score}): {summary}")
        else:
            return self.generate_one_shot(text, ontologist_prompt)

    def classify_url(self, url: str, prompt: str) -> tuple[Optional[str], dict]:
        return self.generate_one_shot(url, prompt)

    def generate_one_shot(self, text: str, prompt: str) -> tuple[Optional[str], dict]:
        mistral_response = self.mistral_one_shot(text, prompt)
        relations = self.stanford_relations(mistral_response)
        return mistral_response, relations

    def process_record(self, record: str, print_contents: bool, summarize: bool, rank: bool,
                       record_index: Optional[int] = None) -> None:
        try:
            record_data = json.loads(record)
            text = record_data['text']
            url = record_data['url']
            url_response, relations = self.classify_url(url, url_prompt)
            worker_logger.info(f"URL Description: {url_response}")

            if print_contents:
                self.logger.info(f"URL content: {text}")

            if summarize:
                summary, relations = self.summarize_text(text, rank)
                self.logger.info(f"Summary: {summary}")
        except json.JSONDecodeError as e:
            if record_index is not None:
                self.logger.error(f"Error parsing record at index {record_index}: {e}")
            else:
                self.logger.error(f"Error parsing record: {e}")


def process_zst(args: Tuple[str, bool, bool, bool, HuggingFaceDatasetService, DatasetProcessor, Optional[int]]) -> None:
    global logger
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