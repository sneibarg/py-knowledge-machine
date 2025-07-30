import os
import sys
import requests

from processor.dataset.DatasetProcessor import DatasetProcessor
from service.LoggingService import LoggingService
from service.NlpService import NlpService
from service.OllamaService import OllamaService
from service.OpenCycService import OpenCycService
from agent.CycReasoningAgent import CycReasoningAgent
from nltk.stem import WordNetLemmatizer

wnl = WordNetLemmatizer()
payload = {"word": "egg", "pos": "noun"}
wordnet_api = "http://dragon:9081/api/v1/wordnet"
nlp_api_url = "http://dragon:9081/nlp"
cyc_host = "dragon:3602"
logging_service = LoggingService(os.path.join(os.getcwd(), "runtime", "logs"), "word-to-KRL")
logger = logging_service.setup_logging(False)
ollama_api_url = "http://localhost:11435/api/generate"

open_cyc_service = OpenCycService(cyc_host, logger)
ollama_service = OllamaService(ollama_api_url, logger)
nlp_service = NlpService(nlp_api_url, logger)
cyc_reasoning_agent = CycReasoningAgent(ollama_service, open_cyc_service, logger)
dataset_processor = DatasetProcessor(nlp_service, ollama_service, logger, 3)
synsets = requests.get(wordnet_api, params=payload).json()['synsets']
cyc_reasoning_agent.gemma3n_analysis()

sys.exit(1)






