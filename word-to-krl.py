import os
import sys
import requests

from processor.nlp import translate_parse_tree, Node
from processor.nlp.PartOfSpeech import PartOfSpeech
from processor.nlp.CycSynset import get_cyc_synset
from service.LoggingService import LoggingService
from service.NlpService import NlpService
from service.OllamaService import OllamaService
from service.OpenCycService import OpenCycService
from agent.CycReasoningAgent import CycReasoningAgent
from nltk.stem import WordNetLemmatizer


test_sentence = "if you can't sit up while laying flat then you could stand to do some sit-ups"
wnl = WordNetLemmatizer()
payload = {"word": "sit-up", "pos": "noun"}
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
relations = nlp_service.stanford_relations(test_sentence)
parse_tree = str(relations['sentences'][0]['parseTree'])
print(f"PARSE_TREE={str(parse_tree)}")
tree = translate_parse_tree(parse_tree)
tag_map = PartOfSpeech.get_tag_map()
all_nouns = [tree.get_nodes_by_type(tag) for tag in tag_map['noun']]
all_nouns = [node for tag_nodes in all_nouns for node in tag_nodes]
print(f"ALL_NOUNS={len(all_nouns)}")
all_verbs = [tree.get_nodes_by_type(tag) for tag in tag_map['verb']]
all_verbs = [node for tag_nodes in all_verbs for node in tag_nodes]
print(f"ALL_VERBS={len(all_verbs)}")
all_clauses = [tree.get_nodes_by_type(tag) for tag in tag_map['clause']]
all_clauses = [node for tag_nodes in all_clauses for node in tag_nodes]
print(f"ALL_CLAUSES={len(all_clauses)}")
for node in all_nouns:
    print(f"NOUN={str(node)}")
    if node is None:
        continue
    if isinstance(node, Node):
        try:
            print(f"NOUN={node.label}")
            singular = wnl.lemmatize(node.label, 'n')
            if node.label == singular and singular[-1] == "s":
                singular = singular[:-1]
            print(f"SINGULAR={singular}")
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
sys.exit(1)
print(f"PAYLOAD={payload}")
synsets = requests.get(wordnet_api, params=payload).json()['synsets']
for synset in synsets:
    print(f"SYNSET={str(synset)}")
    definition = str(synset['definition'])
    tokens = nlp_service.stanford_tokenize(definition)['sentences']
    for sentence in tokens:
        sentence = ' '.join(sentence)
        print(f"SENTENCE={sentence}")
    relations = nlp_service.stanford_relations(definition)
    openie_triples = nlp_service.stanford_relations(definition, True)
    parse_tree = str(relations['sentences'][0]['parseTree'])
    tree = translate_parse_tree(parse_tree)
    all_nouns = [tree.get_nodes_by_type(tag.value) for tag in PartOfSpeech.get_tag_map('word') if tag in [PartOfSpeech.NN, PartOfSpeech.NNS, PartOfSpeech.NNP, PartOfSpeech.NNPS]]
    all_verbs = [tree.get_nodes_by_type(tag.value) for tag in PartOfSpeech.get_tag_map('word') if tag in [PartOfSpeech.VBG.value, PartOfSpeech.VBN.value]]
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
                cyc_english_word = open_cyc_service.query_sentence(f"(#$prettyString-Canonical ?TERM \"{singular}\")", mt_monad='EnglishMt')
                word_instances = open_cyc_service.query_sentence(f"(#$isa #${singular.capitalize()} ?ARG2)", mt_monad='BaseKB')
            except ValueError as ve:
                print(ve)
        sys.exit(0)
