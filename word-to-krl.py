import os
import sys
import requests

from processor.nlp import translate_parse_tree
from processor.nlp.PartOfSpeech import PartOfSpeech
from processor.nlp.CycSynset import get_cyc_synset
from service.LoggingService import LoggingService
from service.NlpService import NlpService
from service.OllamaService import OllamaService
from service.OpenCycService import OpenCycService
from agent.CycReasoningAgent import CycReasoningAgent
from nltk.stem import WordNetLemmatizer

test_sentence = "if you can't sit up while laying flat then you could stand to do some situps"
wnl = WordNetLemmatizer()
payload = {"word": "egg", "pos": "noun"}
wordnet_api = "http://dragon:9081/wordnet/synsets"
nlp_api_url = "http://dragon:9081/nlp"
ollama_api_url = "http://localhost:11434/api/generate"
cyc_host = "dragon:3602"
logging_service = LoggingService(os.path.join(os.getcwd(), "runtime", "logs"), "word-to-KRL")
logger = logging_service.setup_logging(False)
open_cyc_service = OpenCycService(cyc_host, logger)
ollama_service = OllamaService(ollama_api_url, logger)
nlp_service = NlpService(nlp_api_url, logger)
cyc_reasoning_agent = CycReasoningAgent(ollama_service, open_cyc_service, logger)
synsets = requests.get(wordnet_api, params=payload).json()['synsets']
for synset in synsets:
    definition = str(synset['definition'])
    tokens = nlp_service.stanford_tokenize(definition)['sentences']
    for sentence in tokens:
        sentence = ' '.join(sentence)
        print(f"SENTENCE={sentence}")
    relations = nlp_service.stanford_relations(definition)
    openie_triples = nlp_service.stanford_relations(definition, True)
    parse_tree = str(relations['sentences'][0]['parseTree'])
    tree = translate_parse_tree(parse_tree)
    all_nouns = [tree.get_nodes_by_type(noun) for noun in
                 [PartOfSpeech.NN.value, PartOfSpeech.NNS.value, PartOfSpeech.NNP.value, PartOfSpeech.NNPS.value]]
    all_verbs = [tree.get_nodes_by_type(verb) for verb in [PartOfSpeech.VBG.value, PartOfSpeech.VBN.value]]
    for noun in all_nouns:
        for node in noun:
            if node.label is None:
                continue
            try:
                singular = wnl.lemmatize(node.label, 'n')
                term = open_cyc_service.search_term(singular)  # this is my unexpected frameset structure for 'animal'.
                cyc_synset = get_cyc_synset(node, nlp_service, ollama_service, open_cyc_service, wnl)
                print(f"TERM={term}")
                print(f"TERM_COMMENT={cyc_synset.term_comment}")
                cyc_english_word = open_cyc_service.query_sentence(f"(#$prettyString-Canonical ?TERM \"{singular}\")",
                                                                   mt_monad='EnglishMt')
                word_instances = open_cyc_service.query_sentence(f"(#$isa #${singular.capitalize()} ?ARG2)",
                                                                 mt_monad='BaseKB')
            except ValueError as ve:
                print(ve)
        sys.exit(0)
