import argparse
import sys

from processor.dataset import DatasetDownloader, DEFAULT_CONFIG


def parse_args():
    parser = argparse.ArgumentParser(description="Download Hugging Face datasets")
    parser.add_argument('--namespace', type=str, required=True, help='The HuggingFace namespace.')
    parser.add_argument('--dataset-name', type=str, help='Dataset or namespace (e.g., mlfoundations)')
    parser.add_argument('--namespace-mode', action='store_true', help='Treat dataset_name as namespace')
    parser.add_argument('--hf-token', action='store_true', help='Override the HF_TOKEN environment variable.')
    parser.add_argument('--output-dir', type=str, help='Directory where dataset will be saved.')
    parser.add_argument('--cache-dir', type=str, help='Directory where the HuggingFace cache is stored.')
    parser.add_argument('--log-dir', type=str, help='Directory where the logs will be saved.')
    parser.add_argument('--no-splits', action='store_true', help='Skip training splits when downloading a dataset.')
    parser.add_argument('--clean-default-cache', action='store_true', help='Delete the dataset cache before downloading.')
    parser.add_argument('--skip-datasets', nargs='*', default=[], help='Datasets to skip in namespace mode')

    args = parser.parse_args()
    if str(sys.argv[0]) is "-h" or str(sys.argv[0]) is "--help":
        parser.print_help()
        sys.exit(0)

    config = DEFAULT_CONFIG.copy()
    config.update(vars(args))
    return config


if __name__ == "__main__":
    parsed_config = parse_args()
    downloader = DatasetDownloader(parsed_config)
    downloader.download()
