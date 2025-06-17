import logging
import re
from pstats import SortKey

import zstandard as zstd
from datetime import time
from typing import List, Optional, Union, Tuple

from huggingface_hub import hf_hub_download


class HuggingFaceDatasetService:
    def __init__(self, logger):
        self.logger = logger
        pass

    def dump_zstd(self, local_path) -> List[str]:
        lines = []
        try:
            with zstd.open(local_path, 'rt') as f:
                for line in f:
                    lines.append(line)
            return lines
        except Exception as e:
            self.logger.error(f"Error processing file: {e}")
            return []

    def download(self, repo_id: str, file_path: str, cache_dir: Optional[str] = None, retry_count: int = 3) -> str:
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
        """Split files into chunks for each process."""
        chunk_size = max(1, len(files) // num_procs)
        return [files[i:i + chunk_size] for i in range(0, len(files), chunk_size)]

    @staticmethod
    def get_sort_key(local_path) -> Union[tuple[int, int, int], tuple[float, float, float]]:
        """Extract sorting keys from the file path for sequential ordering."""
        match = re.match(r'global-shard_(\d+)_of_\d+/local-shard_(\d+)_of_\d+/shard_(\d+)_processed\.jsonl\.zst',
                         local_path)
        if match:
            global_shard = int(match.group(1))
            local_shard = int(match.group(2))
            shard_number = int(match.group(3))
            return global_shard, local_shard, shard_number
        return float('inf'), float('inf'), float('inf')
