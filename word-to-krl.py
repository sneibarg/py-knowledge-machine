import os
import sys
import requests
import time

from service.CycLService import CycLService
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
cycl_service = CycLService(host=cyc_host)
term_comment = cycl_service.search_term("#$comment")
print(str(term_comment))
terms_comment = cycl_service.search_terms("comment")
print(str(terms_comment))
# cyc_reasoning_agent = CycReasoningAgent(ollama_service, open_cyc_service, logger)
# synsets = requests.get(wordnet_api, params=payload).json()['synsets']
#
# start_time = time.time()
# cyc_reasoning_agent.gemma3n_analysis()
# print(f"Gemma3n analysis of microtheories took {int(time.time() - start_time)} seconds.")
# sys.exit(0)






