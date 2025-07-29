import os
import sys
import requests

from processor.dataset.DatasetProcessor import DatasetProcessor
from service.LoggingService import LoggingService
from service.OpenCycService import OpenCycService
from agent.CycReasoningAgent import CycReasoningAgent
from nltk.stem import WordNetLemmatizer

wnl = WordNetLemmatizer()
payload = {"word": "egg", "pos": "noun"}
wordnet_api = "http://dragon:9081/api/v1/wordnet"
cyc_host = "dragon:3602"
logger = LoggingService(os.path.join(os.getcwd(), "runtime", "logs"), "word-to-KRL")
open_cyc_service = OpenCycService(host=cyc_host, logger=logger)
cyc_reasoning_agent = CycReasoningAgent(open_cyc_service, parent_logger=logger)
dataset_processor = DatasetProcessor(logger, 3)
synsets = requests.get(wordnet_api, params=payload).json()['synsets']
cyc_reasoning_agent.mistral_analysis()
sys.exit(1)






