import zstandard as zstd
import json
import logging
import time
import argparse
import os
import re
import requests
import sys
from huggingface_hub import HfApi, hf_hub_download

# Configure logging to write to a file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ontology_editor.log'),  # Log to file
        logging.StreamHandler()  # Optional: keep console output
    ]
)

nlp_api_url = "http://malboji:8069/nlp/relations"
mistral_api_url = "http://localhost:11434/api/generate"
max_shots = 10
url_prompt = ("I am your automated ontology editor, and I am reviewing a Uniform Resource Locator."
              "I will generate a one sentence response describing the URL. The URL is: ")
ontologist_prompt = ("I am your automated ontology editor, and I am reviewing data step by step "
                     "to validate and assert to our knowledge and understanding. "
                     "I will ignore formalities, not be verbose, and respond with only the facts. "
                     "The following text you have given me is: ")


def stanford_relations(data):
    headers = {'Content-Type': 'application/json'}
    try:
        start_time = time.time()  # Start timer
        response = requests.post(nlp_api_url, data=data, headers=headers)
        end_time = time.time()  # End timer
        duration = end_time - start_time
        logging.info(f"REST call to {nlp_api_url} took {duration:.3f} seconds")
        return response.json()
    except Exception as e:
        logging.error(f"An error occurred getting relations: {e}")


def mistral_one_shot(text, base_prompt):
    full_prompt = f"<s>[INST] {base_prompt} {text} [/INST]"
    payload = {
        "model": "mistral:7b-instruct-q4_0",
        "prompt": full_prompt,
        "stream": False
    }

    try:
        try:
            start_time = time.time()  # Start timer
            response = requests.post(mistral_api_url, json=payload)
            end_time = time.time()  # End timer
            duration = end_time - start_time
            logging.info(f"REST call to {mistral_api_url} took {duration:.3f} seconds")
            response.raise_for_status()
            return response.json()['response']
        except requests.exceptions.RequestException as e:
            logging.error(f"Error communicating with Ollama server: {e}")
        except json.JSONDecodeError:
            logging.error("Invalid response format from Ollama server")
        except KeyError:
            logging.error("Unexpected response structure from Ollama server")
    except Exception as e:
        logging.error(f"An error occurred: {e}")


def download_and_get_local_path(repo_id, file_path, cache_dir=None, retry_count=3):
    """Download a file from the Hugging Face Hub with retries and return its local path."""
    for attempt in range(retry_count):
        try:
            logging.info(f'Downloading {file_path}, attempt {attempt + 1}')
            local_path = hf_hub_download(
                repo_id=repo_id,
                filename=file_path,
                repo_type='dataset',
                cache_dir=cache_dir
            )
            logging.info(f'Successfully downloaded {file_path} to {local_path}')
            return local_path
        except (TimeoutError, ConnectionError) as ce:
            logging.warning(f'Attempt {attempt + 1}/{retry_count} for {file_path} failed: {ce}')
            if attempt < retry_count - 1:
                time.sleep(30)
            else:
                logging.error(f'Max retries reached for {file_path}')
                raise
        except Exception as ue:
            logging.error(f'Unexpected error downloading {file_path}: {ue}')
            raise


def dump_data(local_path):
    lines = []
    try:
        with zstd.open(local_path, 'rt') as f:
            lines.append(next(f))
        return lines
    except Exception as e:
        logging.error(f"Error processing file: {e}")


def classify_url(url, prompt):
    return generate_one_shot(url, prompt)


def generate_one_shot(text, prompt):
    logging.info(f"Text: {text}")
    mistral_response = mistral_one_shot(text, prompt)
    logging.info(f"Mistral Response: {mistral_response}")
    relations = stanford_relations(mistral_response)
    if "parseTree" in str(relations):
        for sentence in relations['sentences']:
            parse_tree = sentence['parseTree']
            logging.info(f"parseTree: {parse_tree}")
    return mistral_response


def process_file(local_path, print_contents=False, summarize=False, rank=False):
    """Process a local .jsonl.zst file and log information about it."""
    dataset = dump_data(local_path)
    for row in dataset:
        record = json.loads(row)
        text = record['text']
        url = record['url']
        url_response = classify_url(url, url_prompt)
        logging.info(f"URL Description: {url_response}")

        key_terms = set()
        if summarize and rank:
            logging.info("Ranking summaries...")
            relations = stanford_relations(url_response)
            if "parseTree" in str(relations):
                for sentence in relations['sentences']:
                    tokens = sentence.get('tokens', [])
                    for token in tokens:
                        word = token.get('word')
                        pos = token.get('pos', '')
                        if pos.startswith('N') and word is not None:  # Nouns: NN, NNS, NNP, NNPS
                            key_terms.add(word.lower())
            logging.info(f"Key Terms from URL: {key_terms}")

        if print_contents and summarize:
            summarize_text(text, rank, key_terms if rank else None)
            sys.exit(0)


def summarize_text(text, rank, key_terms=None):
    if rank and key_terms is not None:
        summaries = []
        for shot in range(max_shots):
            summary = generate_one_shot(text, ontologist_prompt)
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
        logging.info("Ranked Summaries:")
        for i, (summary, score) in enumerate(summaries, 1):
            logging.info(f"Rank {i} (Score: {score}): {summary}")
    else:
        generate_one_shot(text, ontologist_prompt)


def get_sort_key(file_path):
    """Extract sorting keys from the file path for sequential ordering."""
    match = re.match(r'global-shard_(\d+)_of_\d+/local-shard_(\d+)_of_\d+/shard_(\d+)_processed\.jsonl\.zst',
                     file_path)
    if match:
        global_shard = int(match.group(1))
        local_shard = int(match.group(2))
        shard_number = int(match.group(3))
        return global_shard, local_shard, shard_number
    return float('inf'), float('inf'), float('inf')


def main():
    """Parse command-line arguments and inspect the dataset files in sequential order."""
    parser = argparse.ArgumentParser(description="Inspect Hugging Face dataset files")
    parser.add_argument('repo_id', type=str, help='Dataset repository ID (e.g., mlfoundations/dclm-baseline-1.0)')
    parser.add_argument('--local-snapshot-dir', type=str, help='Path to local snapshot directory')
    parser.add_argument('--files', type=str, nargs='*', help='Specific files to inspect')
    parser.add_argument('--cache-dir', type=str, default=None, help='Cache directory for downloaded files')
    parser.add_argument('--retry-count', type=int, default=3, help='Number of retry attempts for downloads')
    parser.add_argument('--print-contents', action='store_true', help='Print contents of each file')
    parser.add_argument('--summarize', action='store_true', help='Log Mistral one-shot summary.')
    parser.add_argument('--rank', action='store_true', help='Ten responses will be generated and ranked.')
    parser.add_argument('--max-lines', type=int, default=99999999999, help='Maximum number of lines to log per file')
    args = parser.parse_args()

    files_to_process = []
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
            logging.info(f"Found {len(files_to_process)} .jsonl.zst files in the local snapshot.")

            if args.files:
                specified_files = set(args.files)
                files_to_process = [f for f in files_to_process if f[0] in specified_files]
                if len(files_to_process) < len(specified_files):
                    logging.warning("Some specified files were not found in the local snapshot.")
        except Exception as e:
            logging.error(f"Error accessing local snapshot directory: {e}")
            logging.info("Falling back to downloading files.")
            files_to_process = []

    if not files_to_process:
        logging.info("No local files available. Proceeding to download from the repository.")
        api = HfApi()
        repo_files = api.list_repo_files(repo_id=args.repo_id, repo_type='dataset')
        jsonl_files = [f for f in repo_files if f.endswith('.jsonl.zst')]
        if args.files:
            files_to_process_remote = [f for f in args.files if f in jsonl_files]
            if len(files_to_process_remote) < len(args.files):
                logging.warning("Some specified files were not found in the dataset repository.")
        else:
            files_to_process_remote = jsonl_files

        files_to_process_remote.sort(key=get_sort_key)
        for file_path in files_to_process_remote:
            try:
                local_path = download_and_get_local_path(args.repo_id, file_path, args.cache_dir, args.retry_count)
                files_to_process.append((file_path, local_path))
            except Exception as e:
                logging.error(f"Failed to download {file_path}: {e}")

    total_files = len(files_to_process)
    if total_files == 0:
        logging.error("No files to process. Check your local snapshot directory or repository settings.")
        return

    for idx, (relative_path, local_path) in enumerate(files_to_process, 1):
        process_file(local_path, print_contents=args.print_contents, summarize=args.summarize, rank=args.rank)

    logging.info('Inspection completed')


if __name__ == '__main__':
    main()