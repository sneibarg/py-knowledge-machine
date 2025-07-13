import logging
import re
import zstandard as zstd
from datetime import time
from typing import List, Optional, Union, Tuple
from huggingface_hub import hf_hub_download, HfApi


class HuggingFaceDatasetService:
    def __init__(self, logger):
        self.api = HfApi()
        self.logger = logger

    def dump_zstd(self, local_path) -> List[str]:
        lines = []
        try:
            with zstd.open(local_path, 'rt') as f:
                for line in f:
                    lines.append(line)
            if not lines:
                self.logger.warning(f"Empty ZST file: {local_path}")
            return lines
        except zstd.ZstdError as ze:
            self.logger.error(f"Malformed ZST file {local_path}: {ze}")
            raise ValueError("Invalid ZST compression") from ze
        except Exception as e:
            self.logger.error(f"Error processing ZST {local_path}: {e}")
            return []

    def download(self, repo_id: str, file_path: str, cache_dir: Optional[str] = None, retry_count: int = 3) -> str:
        try:
            self.api.dataset_info(repo_id)
        except Exception as e:
            raise ValueError(f"Invalid or inaccessible repo_id '{repo_id}': {e}") from e

        for attempt in range(retry_count):
            try:
                self.logger.info(f'Downloading {file_path}, attempt {attempt + 1}')
                local_path = hf_hub_download(
                    repo_id=repo_id,
                    filename=file_path,
                    repo_type='dataset',
                    cache_dir=cache_dir
                )
                self.logger.info(f'Successfully downloaded {file_path} to {local_path}')
                return local_path
            except (TimeoutError, ConnectionError) as ce:
                logging.warning(f'Attempt {attempt + 1}/{retry_count} for {file_path} failed: {ce}')
                if attempt < retry_count - 1:
                    time.sleep(30)
                else:
                    self.logger.error(f'Max retries reached for {file_path}')
                    raise
            except Exception as ue:
                self.logger.error(f'Unexpected error downloading {file_path}: {ue}')
                raise

    @staticmethod
    def split_files(files: List[Tuple[str, str]], num_procs: int) -> List[List[Tuple[str, str]]]:
        chunk_size = max(1, len(files) // num_procs)
        return [files[i:i + chunk_size] for i in range(0, len(files), chunk_size)]

    @staticmethod
    def get_sort_key(local_path) -> Union[tuple[int, int, int], tuple[float, float, float]]:
        match = re.match(r'global-shard_(\d+)_of_\d+/local-shard_(\d+)_of_\d+/shard_(\d+)_processed\.jsonl\.zst',
                         local_path)
        if match:
            global_shard = int(match.group(1))
            local_shard = int(match.group(2))
            shard_number = int(match.group(3))
            return global_shard, local_shard, shard_number
        return float('inf'), float('inf'), float('inf')
