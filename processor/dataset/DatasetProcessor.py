import json
import logging
import os
import time
import requests
import requests.exceptions

from multiprocessing import Pool
from typing import List, Tuple, Optional
from huggingface_hub import HfApi
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from service import get_session
from service.HuggingFaceDatasetService import HuggingFaceDatasetService
from service.LoggingService import LoggingService

nlp_api_url = "http://malboji:8069/nlp/relations"
mistral_api_url = "http://dragon:11435/api/generate"
url_prompt = ("I am your automated ontology editor, and I am reviewing a Uniform Resource Locator."
              "I will generate a one sentence response describing the URL. The URL is: ")
ontologist_prompt = ("I am your automated ontology editor, and I am reviewing data step by step "
                     "to validate and assert to our knowledge and understanding. "
                     "I will ignore formalities, not be verbose, and respond with only the facts. "
                     "The following text you have given me is: ")

logging_service = LoggingService('DataProcessor', os.path.join(os.getcwd(), "../../runtime/logs"))
logger = logging_service.setup_logging(False)
worker_logger = None


class DatasetProcessor:
    def __init__(self, parent_logger, max_shots):
        self.logger = parent_logger
        self.api = HfApi()
        self.repo_id = None
        self.local_snapshot_dir = None
        self.cache_dir = None
        self.retry_count = 3
        self.print_contents = False
        self.summarize = False
        self.rank = False
        self.debug = False
        self.session = get_session(max_retries=3)
        self.files = []
        self.num_procs = 1
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
            response = self.session.post(
                nlp_api_url,
                data=data.encode('utf-8', errors='replace'),
                headers=headers,
                timeout=(120, 360)
            )
            response.raise_for_status()
            return response.json()
        except json.JSONDecodeError as jde:
            self.logger.error(f"Invalid JSON from NLP API: {jde}")
            raise ValueError("NLP API returned malformed JSON") from jde
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
            response = self.session.post(
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

    def process_record(self, record: str,
                       print_contents: bool,
                       summarize: bool,
                       rank: bool,
                       record_index: Optional[int] = None) -> None:
        if record_index is not None and record_index < 0:
            raise ValueError("record_index must be non-negative")

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

    def process_zst(self, args) -> None:
        local_path, print_contents, summarize, rank, data_agent, dataset_processor, record_index = args
        dataset = data_agent.dump_zstd(local_path)

        if not dataset:
            raise ValueError(f"No records found in file: {local_path}")

        if record_index is not None:
            if not isinstance(record_index, int) or record_index < 0:
                raise IndexError(f"Invalid record index: {record_index}. Must be a non-negative integer.")

            if record_index >= len(dataset):
                raise IndexError(f"Record index {record_index} exceeds dataset length {len(dataset)}")

            logger.info(f"Processing record at index {record_index} in file: {local_path}")
            self.process_record(dataset[record_index], print_contents, summarize, rank, record_index)
        else:
            logger.info(f"Processing all records in file: {local_path}")
            for i, row in enumerate(dataset):
                self.process_record(row, print_contents, summarize, rank, i)

    def process(self, init_worker, worker_process) -> None:
        files_to_process: List[Tuple[str, str]] = []
        data_agent = HuggingFaceDatasetService(logger)
        if self.local_snapshot_dir and os.path.isdir(self.local_snapshot_dir):
            try:
                refs_path = os.path.join(self.local_snapshot_dir, 'refs', 'main')
                if not os.path.isfile(refs_path):
                    raise FileNotFoundError(f"'refs/main' not found in {self.local_snapshot_dir}")
                with open(refs_path, 'r') as f:
                    commit_hash = f.read().strip()

                snapshot_dir = os.path.join(self.local_snapshot_dir, 'snapshots', commit_hash)
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

                if self.files:
                    specified_files = set(self.files)
                    files_to_process = [f for f in files_to_process if f[0] in specified_files]
                    if len(files_to_process) < len(specified_files):
                        logging.warning("Some specified files were not found in the local snapshot.")
            except Exception as e:
                logger.error(f"Error accessing local snapshot directory: {e}")
                logger.info("Falling back to downloading files.")
                files_to_process = []

        if not files_to_process:
            logger.info("No local files available. Proceeding to download from the repository.")
            repo_files = self.api.list_repo_files(repo_id=self.repo_id, repo_type='dataset')
            jsonl_files = [f for f in repo_files if f.endswith('.jsonl.zst')]
            if self.files:
                files_to_process_remote = [f for f in self.files if f in jsonl_files]
                if len(files_to_process_remote) < len(self.files):
                    logging.warning("Some specified files were not found in the dataset repository.")
            else:
                files_to_process_remote = jsonl_files

            files_to_process_remote.sort(key=lambda x: data_agent.get_sort_key(x))
            for file_path in files_to_process_remote:
                try:
                    local_path = data_agent.download(self.repo_id, file_path, self.cache_dir, self.retry_count)
                    files_to_process.append((file_path, local_path))
                except Exception as e:
                    logger.error(f"Failed to download {file_path}: {e}")

        total_files = len(files_to_process)
        if total_files == 0:
            logger.error("No files to process. Check your local snapshot directory or repository settings.")
            return

        file_chunks = data_agent.split_files(files_to_process, self.num_procs)
        self.logger.info(f"Distributing {total_files} files across {self.num_procs} processes")

        if self.num_procs == 1:
            worker_process(files_to_process, self.print_contents, self.summarize, self.rank, self.record_index)
        else:
            chonks = [(chunk, self.print_contents, self.summarize, self.rank, self.record_index) for chunk in file_chunks]
            with Pool(processes=self.num_procs, initializer=init_worker, initargs=(self.debug,)) as pool:
                pool.starmap(worker_process, chonks)
        logger.info('Inspection completed')

