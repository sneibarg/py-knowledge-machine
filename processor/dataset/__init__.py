import logging
import os
import shutil
import sys
import requests
import pkg_resources

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Union
from datasets import load_dataset, Dataset, DownloadConfig, IterableDataset, get_dataset_config_names, get_dataset_split_names
from huggingface_hub import list_datasets, HfApi
from requests.adapters import HTTPAdapter
from urllib3 import Retry

DEFAULT_CONFIG = {
    "dataset_name": None,
    "subset": None,
    "split": None,
    "no_splits": False,
    "namespace_mode": False,
    "output_dir": "./datasets",
    "cache_dir": "./cache",
    "hf_token": None,
    "max_retries": 3,
    "timeout": 30,
    "log_dir": "./logs",
    "clean_default_cache": True,
}


def inspect_dataset_metadata(dataset_id: str, hf_token: Optional[str]) -> None:
    try:
        api = HfApi(token=hf_token)
        dataset_info = api.dataset_info(dataset_id)
        logging.info(f"Metadata for {dataset_id}: {dataset_info.__dict__}")
    except Exception as e:
        logging.warning(f"Failed to inspect metadata for {dataset_id}: {e}")


def check_default_cache(clean_default_cache: bool) -> None:
    default_cache = os.path.expanduser("~/.cache/huggingface")
    if os.path.exists(default_cache):
        if clean_default_cache:
            try:
                shutil.rmtree(default_cache)
                logging.info(f"Cleaned default cache directory: {default_cache}")
            except Exception as e:
                logging.error(f"Failed to clean default cache {default_cache}: {e}")
        else:
            logging.warning(f"Default cache {default_cache} exists. Use --clean_default_cache to remove.")


def check_library_versions() -> None:
    for pkg in ["datasets", "huggingface_hub"]:
        try:
            version = pkg_resources.get_distribution(pkg).version
            logging.info(f"{pkg} version: {version}")
        except pkg_resources.DistributionNotFound:
            logging.error(f"{pkg} is not installed.")
            sys.exit(1)


def create_resilient_session(max_retries: int, timeout: int) -> requests.Session:
    session = requests.Session()
    retries = Retry(total=max_retries, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.timeout = timeout
    return session


def get_dataset_info(dataset_id: str, cache_dir: str, no_splits: bool, hf_token: Optional[str]) -> Dict[str, List[str]]:
    try:
        subsets = get_dataset_config_names(dataset_id, download_config=DownloadConfig(cache_dir=cache_dir), token=hf_token)
        if not subsets:
            subsets = [None]
        logging.info(f"Found subsets for {dataset_id}: {subsets}")
    except Exception as e:
        logging.warning(f"Failed to retrieve subsets for {dataset_id}: {e}")
        subsets = [None]
    subset_splits = {}
    for subset in subsets:
        try:
            splits = get_dataset_split_names(dataset_id, config_name=subset,
                                             download_config=DownloadConfig(cache_dir=cache_dir),
                                             token=hf_token)
            subset_splits[subset or "default"] = ["full"] if no_splits else splits
            logging.info(f"Found splits for {dataset_id}/{subset or 'default'}: {subset_splits[subset or 'default']}")
        except Exception as e:
            logging.warning(f"Failed to retrieve splits for {dataset_id}/{subset or 'default'}: {e}")
            subset_splits[subset or 'default'] = ['full'] if no_splits else ['train']
    return subset_splits


def get_namespace_datasets(namespace: str, hf_token: Optional[str]) -> List[str]:
    try:
        datasets = list_datasets(author=namespace, token=hf_token)
        dataset_ids = [ds.id for ds in datasets]
        logging.info(f"Found datasets under namespace {namespace}: {dataset_ids}")
        return dataset_ids
    except Exception as e:
        logging.error(f"Failed to list datasets for namespace {namespace}: {e}")
        return []


@dataclass
class DatasetConfig:
    """Configuration for downloading a single dataset."""
    dataset_name: str
    subset: Optional[str]
    split: Optional[str]
    output_dir: str


class DatasetDownloader:
    def __init__(self, config: Dict):
        self.config = config
        self.hf_token = config["hf_token"]
        self.cache_dir = config["cache_dir"]
        self.no_splits = config["no_splits"]
        self.downloaded = []
        self.failed = []

    def get_dataset_configurations(self) -> List[DatasetConfig]:
        configurations = []
        base_output_dir = self.config["output_dir"]
        if self.config["namespace_mode"]:
            dataset_ids = get_namespace_datasets(self.config["dataset_name"], self.hf_token)
            if not dataset_ids:
                logging.error(f"No datasets found under namespace {self.config['dataset_name']}.")
                return configurations
            for dataset_id in dataset_ids:
                inspect_dataset_metadata(dataset_id, self.hf_token)
                output_dir = os.path.join(base_output_dir, dataset_id.replace("/", "_"))
                subset_splits = get_dataset_info(dataset_id, self.cache_dir, self.no_splits, self.hf_token)
                if not subset_splits:
                    split = "full" if self.no_splits else "train"
                    configurations.append(DatasetConfig(dataset_id, None, split, output_dir))
                else:
                    for subset, splits in subset_splits.items():
                        for split in splits:
                            configurations.append(DatasetConfig(dataset_id, subset, split, output_dir))
        else:
            dataset_id = self.config["dataset_name"]
            inspect_dataset_metadata(dataset_id, self.hf_token)
            output_dir = os.path.join(base_output_dir, dataset_id.replace("/", "_"))
            subset = self.config["subset"]
            split = self.config["split"]
            if subset or (split and not self.no_splits):
                configurations.append(DatasetConfig(dataset_id, subset, split, output_dir))
            else:
                subset_splits = get_dataset_info(dataset_id, self.cache_dir, self.no_splits, self.hf_token)
                if not subset_splits:
                    split = "full" if self.no_splits else "train"
                    configurations.append(DatasetConfig(dataset_id, None, split, output_dir))
                else:
                    for subset, splits in subset_splits.items():
                        for split in splits:
                            configurations.append(DatasetConfig(dataset_id, subset, split, output_dir))
        return configurations

    def download_dataset(self, config: DatasetConfig) -> Optional[Union[Dataset, IterableDataset]]:
        split_display = "full" if config.split == "full" else config.split or "all"
        logging.info(f"Downloading {config.dataset_name}/{config.subset or 'default'}/{split_display}")
        try:
            dataset = load_dataset(
                path=config.dataset_name,
                name=config.subset,
                split=None if config.split == "full" else config.split,
                cache_dir=self.cache_dir,
                token=self.hf_token,
            )
            if self.no_splits and hasattr(dataset, "split") and dataset.split:
                logging.warning(f"Dataset {config.dataset_name} imposed split '{dataset.split}' despite no_splits=True")
            dataset_output_path = os.path.join(config.output_dir, f"{config.subset or 'default'}_{split_display}")
            Path(dataset_output_path).mkdir(parents=True, exist_ok=True)
            dataset.save_to_disk(dataset_output_path)
            logging.info(f"Saved to {dataset_output_path}")
            return dataset
        except Exception as e:
            logging.error(f"Download failed: {e}")
            if self.no_splits and config.split == "full":
                logging.info(f"Retrying {config.dataset_name} with 'train' split")
                try:
                    dataset = load_dataset(
                        path=config.dataset_name,
                        name=config.subset,
                        split="train",
                        cache_dir=self.cache_dir,
                        token=self.hf_token,
                    )
                    dataset_output_path = os.path.join(config.output_dir, f"{config.subset or 'default'}_train")
                    Path(dataset_output_path).mkdir(parents=True, exist_ok=True)
                    dataset.save_to_disk(dataset_output_path)
                    logging.info(f"Fallback saved to {dataset_output_path}")
                    return dataset
                except Exception as e2:
                    logging.error(f"Fallback failed: {e2}")
            return None

    def download(self) -> List[Union[Dataset, IterableDataset]]:
        configurations = self.get_dataset_configurations()
        for config in configurations:
            dataset = self.download_dataset(config)
            if dataset:
                self.downloaded.append(dataset)
                logging.info(
                    f"Completed download for {config.dataset_name}/{config.subset or 'default'}/{config.split or 'all'}")
            else:
                self.failed.append(f"{config.dataset_name}/{config.subset or 'default'}/{config.split or 'all'}")
        logging.info(f"Summary: {len(self.downloaded)} datasets downloaded, {len(self.failed)} failed")
        if self.failed:
            logging.warning(f"Failed downloads: {self.failed}")
        return self.downloaded
