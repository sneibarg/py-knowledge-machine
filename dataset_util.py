import argparse
import json
import logging
import os
import re
import sys
import time
import requests
import zstandard as zstd
import requests.exceptions
from multiprocessing import Pool, current_process
from typing import List, Tuple, Union, Optional
from huggingface_hub import HfApi, hf_hub_download
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from core import setup_logging

logger = None
worker_logger = None
SortKey = Tuple[Union[int, float], Union[int, float], Union[int, float]]
nlp_api_url = "http://malboji:8069/nlp/relations"
mistral_api_url = "http://dragon:11435/api/generate"
max_shots = 10
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


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((requests.exceptions.ConnectionError,
                                   requests.exceptions.Timeout,
                                   requests.exceptions.HTTPError)),
    before_sleep=lambda retry_state: worker_logger.debug(
        f"Retrying stanford_relations (attempt {retry_state.attempt_number}) after {retry_state.idle_for}s"
    )
)
def stanford_relations(data: str) -> dict:
    headers = {'Content-Type': 'application/json'}
    if not isinstance(data, str):
        worker_logger.error(f"Invalid input type for stanford_relations: {type(data)}")
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
        worker_logger.info(f"REST call to {nlp_api_url} took {duration:.3f} seconds")
        try:
            return response.json()
        except json.JSONDecodeError as e:
            worker_logger.error(f"Invalid JSON response from {nlp_api_url}: {e}")
            return {}
    except requests.exceptions.Timeout as e:
        worker_logger.error(f"Timeout error contacting {nlp_api_url}: {e}")
        raise  # Let tenacity handle retries
    except requests.exceptions.ConnectionError as e:
        worker_logger.error(f"Connection error contacting {nlp_api_url}: {e}")
        raise  # Let tenacity handle retries
    except requests.exceptions.HTTPError as e:
        if response.status_code == 429:
            worker_logger.error(f"Rate limit exceeded for {nlp_api_url}: {e}")
        elif response.status_code >= 500:
            worker_logger.error(f"Server error from {nlp_api_url} (status {response.status_code}): {e}")
            raise  # Retry 5xx errors
        else:
            worker_logger.error(f"HTTP error from {nlp_api_url} (status {response.status_code}): {e}")
        return {}
    except Exception as e:
        worker_logger.error(f"Unexpected error in stanford_relations: {e}")
        return {}
    finally:
        time.sleep(0.1)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((requests.exceptions.ConnectionError,
                                  requests.exceptions.Timeout,
                                  requests.exceptions.HTTPError)),
    before_sleep=lambda retry_state: worker_logger.debug(
        f"Retrying mistral_one_shot (attempt {retry_state.attempt_number}) after {retry_state.idle_for}s"
    )
)
def mistral_one_shot(text: str, base_prompt: str) -> Optional[str]:
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
        worker_logger.info(f"REST call to {mistral_api_url} took {duration:.3f} seconds")
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
        worker_logger.error(f"Timeout error contacting {mistral_api_url}: {e}")
        raise
    except requests.exceptions.ConnectionError as e:
        worker_logger.error(f"Connection error contacting {mistral_api_url}: {e}")
        raise
    except requests.exceptions.HTTPError as e:
        if response.status_code == 429:
            worker_logger.error(f"Rate limit exceeded for {mistral_api_url}: {e}")
        elif response.status_code >= 500:
            worker_logger.error(f"Server error from {mistral_api_url} (status {response.status_code}): {e}")
            raise
        else:
            worker_logger.error(f"HTTP error from {mistral_api_url} (status {response.status_code}): {e}")
        return None
    except Exception as e:
        worker_logger.error(f"Unexpected error in mistral_one_shot: {e}")
        return None
    finally:
        time.sleep(0.1)


def download_and_get_local_path(repo_id: str, file_path: str, cache_dir: Optional[str] = None,
                                retry_count: int = 3) -> str:
    """Download a file from the Hugging Face Hub with retries and return its local path."""
    for attempt in range(retry_count):
        try:
            logger.info(f'Downloading {file_path}, attempt {attempt + 1}')
            local_path = hf_hub_download(
                repo_id=repo_id,
                filename=file_path,
                repo_type='dataset',
                cache_dir=cache_dir
            )
            logger.info(f'Successfully downloaded {file_path} to {local_path}')
            return local_path
        except (TimeoutError, ConnectionError) as ce:
            logging.warning(f'Attempt {attempt + 1}/{retry_count} for {file_path} failed: {ce}')
            if attempt < retry_count - 1:
                time.sleep(30)
            else:
                logger.error(f'Max retries reached for {file_path}')
                raise
        except Exception as ue:
            logger.error(f'Unexpected error downloading {file_path}: {ue}')
            raise


def dump_data(local_path: str) -> List[str]:
    lines = []
    try:
        with zstd.open(local_path, 'rt') as f:
            for line in f:
                lines.append(line)
        return lines
    except Exception as e:
        logger.error(f"Error processing file: {e}")
        return []


def classify_url(url: str, prompt: str) -> tuple[Optional[str], dict]:
    return generate_one_shot(url, prompt)


def generate_one_shot(text: str, prompt: str) -> tuple[Optional[str], dict]:
    mistral_response = mistral_one_shot(text, prompt)
    relations = stanford_relations(mistral_response)
    return mistral_response, relations


def process_record(record: str, print_contents: bool, summarize: bool, rank: bool,
                   record_index: Optional[int] = None) -> None:
    """Process a single JSON record and log information about it."""
    try:
        record_data = json.loads(record)
        text = record_data['text']
        url = record_data['url']
        url_response, relations = classify_url(url, url_prompt)
        worker_logger.info(f"URL Description: {url_response}")

        if print_contents:
            worker_logger.info(f"URL content: {text}")

        if summarize:
            summary, relations = summarize_text(text, rank)
            worker_logger.info(f"Summary: {summary}")
    except json.JSONDecodeError as e:
        if record_index is not None:
            worker_logger.error(f"Error parsing record at index {record_index}: {e}")
        else:
            worker_logger.error(f"Error parsing record: {e}")


def process_file(args: Tuple[str, bool, bool, bool, Optional[int]]) -> None:
    """Process a local .jsonl.zst file and log information about it."""
    local_path, print_contents, summarize, rank, record_index = args
    dataset = dump_data(local_path)
    if not dataset:
        worker_logger.error(f"No records found in file: {local_path}")
        return

    if record_index is not None:
        if not isinstance(record_index, int) or record_index < 0:
            worker_logger.error(f"Invalid record index: {record_index}. Must be a non-negative integer.")
            return
        if record_index >= len(dataset):
            worker_logger.error(f"Record index {record_index} out of range. File has {len(dataset)} records.")
            return
        worker_logger.info(f"Processing record at index {record_index} in file: {local_path}")
        process_record(dataset[record_index], print_contents, summarize, rank, record_index)
    else:
        worker_logger.info(f"Processing all records in file: {local_path}")
        for i, row in enumerate(dataset):
            process_record(row, print_contents, summarize, rank, i)


def summarize_text(text: str, rank: bool, key_terms: Optional[set] = None) -> tuple[Optional[str], dict]:
    if rank and key_terms is not None:
        summaries = []
        for shot in range(max_shots):
            summary, relations = generate_one_shot(text, ontologist_prompt)
            relations = stanford_relations(summary)
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
        worker_logger.info("Ranked Summaries:")
        for i, (summary, score) in enumerate(summaries, 1):
            worker_logger.info(f"Rank {i} (Score: {score}): {summary}")
    else:
        return generate_one_shot(text, ontologist_prompt)


def get_sort_key(file_path: str) -> SortKey:
    """Extract sorting keys from the file path for sequential ordering."""
    match = re.match(r'global-shard_(\d+)_of_\d+/local-shard_(\d+)_of_\d+/shard_(\d+)_processed\.jsonl\.zst',
                     file_path)
    if match:
        global_shard = int(match.group(1))
        local_shard = int(match.group(2))
        shard_number = int(match.group(3))
        return global_shard, local_shard, shard_number
    return float('inf'), float('inf'), float('inf')


def split_files(files: List[Tuple[str, str]], num_procs: int) -> List[List[Tuple[str, str]]]:
    """Split files into chunks for each process."""
    chunk_size = max(1, len(files) // num_procs)
    return [files[i:i + chunk_size] for i in range(0, len(files), chunk_size)]


def worker_process(file_chunk: List[Tuple[str, str]], print_contents: bool, summarize: bool, rank: bool,
                   record_index: Optional[int]) -> None:
    """Process a chunk of files in a worker process."""
    global worker_logger
    for relative_path, local_path in file_chunk:
        worker_logger.info(f"Processing file: {relative_path}")
        try:
            process_file((local_path, print_contents, summarize, rank, record_index))
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


def main() -> None:
    global logger
    args = parse_options()
    logger = setup_logging(args.debug)
    files_to_process: List[Tuple[str, str]] = []
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

            files_to_process.sort(key=lambda x: get_sort_key(x[0]))
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

        files_to_process_remote.sort(key=lambda x: get_sort_key(x))
        for file_path in files_to_process_remote:
            try:
                local_path = download_and_get_local_path(args.repo_id, file_path, args.cache_dir, args.retry_count)
                files_to_process.append((file_path, local_path))
            except Exception as e:
                logger.error(f"Failed to download {file_path}: {e}")

    total_files = len(files_to_process)
    if total_files == 0:
        logger.error("No files to process. Check your local snapshot directory or repository settings.")
        return

    file_chunks = split_files(files_to_process, args.num_procs)
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


if __name__ == '__main__':
    main()
