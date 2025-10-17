import rdflib
from logging import Logger
from temporalio import activity
from processor.dataset import DatasetDownloader
from processor.dataset.DatasetProcessor import DatasetProcessor
from service import LoggingService, NlpService, OllamaService


# noinspection PyCallingNonCallable
@activity.defn
async def download_dataset_activity(config: dict) -> dict:
    logger: Logger = LoggingService(config.get('log_dir', './logs'), 'DatasetDownloader').setup_logging()
    downloader = DatasetDownloader(config)
    downloaded = downloader.download()
    return {"downloaded": len(downloaded)}


# noinspection PyCallingNonCallable
@activity.defn
async def process_dataset_activity(config: dict) -> dict:
    logger: Logger = LoggingService(config.get('log_dir', './logs'), 'DatasetProcessor').setup_logging()
    nlp_service = NlpService(config['nlp_api_url'], logger)
    ollama_service = OllamaService(config['ollama_api_url'], logger)
    processor = DatasetProcessor(nlp_service, ollama_service, logger, max_shots=config.get('max_shots', 10))
    return {"records": []}


@activity.defn
async def summarize_text_activity(params: dict) -> str:
    text = params['text']
    rank = params['rank']
    summary = ""
    return summary


@activity.defn
async def load_ontology_activity(file_path: str) -> dict:
    g = rdflib.Graph()
    g.parse(file_path, format="xml")
    return {"graph": g.serialize(format="turtle")}


@activity.defn
async def process_assertions_activity(params: dict) -> dict:
    graph_str = params['graph']
    g = rdflib.Graph()
    g.parse(data=graph_str, format="turtle")
    # processor = OWLGraphProcessor(...)  # Inject services/logger
    # count = processor.run(params['assertions'])
    return {"processed_count": None}

