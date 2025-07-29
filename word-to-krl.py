import os
import sys
import requests

from processor.dataset.DatasetProcessor import DatasetProcessor
from service.LoggingService import LoggingService
from service.OpenCycService import OpenCycService
from nltk.stem import WordNetLemmatizer

wnl = WordNetLemmatizer()
payload = {"word": "egg", "pos": "noun"}
wordnet_api = "http://dragon:9081/api/v1/wordnet"
cyc_host = "dragon:3602"
logger = LoggingService(os.path.join(os.getcwd(), "runtime", "logs"), "word-to-KRL")
open_cyc_service = OpenCycService(host=cyc_host, logger=logger)
dataset_processor = DatasetProcessor(parent_logger=logger, max_shots=3)
synsets = requests.get(wordnet_api, params=payload).json()['synsets']
mt_query = "(#$isa ?ARG1 #$Microtheory)"
mt_list = open_cyc_service.query_sentence(mt_query, mt_monad='CurrentWorldDataCollectorMt-NonHomocentric')
print("ANSWERS="+str(mt_list['answers']))
sys.exit(1)






