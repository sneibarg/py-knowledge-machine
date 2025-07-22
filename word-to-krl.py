import os
import requests

from processor.dataset.DatasetProcessor import DatasetProcessor
from processor.nlp import ParseTreeNode
from service.LoggingService import LoggingService
from service.OpenCycService import OpenCycService

payload = {"word": "egg", "pos": "noun"}
wordnet_api = "http://dragon:9081/api/v1/wordnet"
cyc_host = "dragon:3602"
logger = LoggingService(os.path.join(os.getcwd(), "runtime", "logs"), "word-to-KRL")
open_cyc_service = OpenCycService(host=cyc_host, logger=logger)
dataset_processor = DatasetProcessor(parent_logger=logger, max_shots=3)
synsets = requests.get(wordnet_api, params=payload).json()['synsets']
for synset in synsets:
    definition = str(synset['definition'])
    print("Definition: "+definition)
    relations = dataset_processor.stanford_relations(definition)
    parse_tree = relations['sentences'][0]['parseTree']
    tree = ParseTreeNode.from_string(parse_tree)
    top_np = tree.children[0] if tree.label == 'ROOT' and len(
        tree.children) > 0 else None  # Skip the root's direct NP child (the entire sentence)
    if top_np:
        np_nodes = top_np.get_nodes_by_label('VP', skip_self=True)
        for node in np_nodes:
            print([leaf for leaf in node.leaves()])
    all_nouns = tree.get_words_by_tags({'NN', 'NNS'})
    print("NOUNS="+str(all_nouns))
    print("--------------------------------------------------------------")





