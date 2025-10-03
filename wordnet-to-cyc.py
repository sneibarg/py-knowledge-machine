import os
import yaml

from logging import Logger
from typing import List
from nltk.corpus.reader import Synset
from nltk.stem import WordNetLemmatizer
from agent.CycReasoningAgent import CycReasoningAgent
from processor.nlp import TreeGenerator, translate_parse_tree, Node
from service.LoggingService import LoggingService
from service.NlpService import NlpService
from service.OllamaService import OllamaService
from service.OpenCycService import OpenCycService
from service.WordNetService import WordNetService


def assert_cyc_synset(service: OpenCycService):
    wff: str = ""
    service.assert_sentence(wff)


with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

test_sentence: str = "if you can't sit up while laying flat then you could stand to do some sit-ups"
wnl: WordNetLemmatizer = WordNetLemmatizer()
logging_service: LoggingService = LoggingService(os.path.join(os.getcwd(), "runtime", "logs"), "word-to-KRL")
logger: Logger = logging_service.setup_logging(False)
wordnet_service: WordNetService = WordNetService(logger)
open_cyc_service: OpenCycService = OpenCycService(config['cyc_host'], logger)
ollama_service: OllamaService = OllamaService(config['ollama_api_url'], logger)
nlp_service: NlpService = NlpService(config['nlp_api_url'], logger)
cyc_reasoning_agent: CycReasoningAgent = CycReasoningAgent(ollama_service, open_cyc_service, logger)
relations: dict = nlp_service.stanford_relations(test_sentence)
generator: TreeGenerator = translate_parse_tree(str(relations['sentences'][0]['parseTree']), print_tree=True)
parent_phrase: Node = generator.get_parent_phrase('HYPH')
nouns: List[Synset] = wordnet_service.get_synsets_by_pos('n')
verbs: List[Synset] = wordnet_service.get_synsets_by_pos('v')
adjectives: List[Synset] = wordnet_service.get_synsets_by_pos('a')
adverbs: List[Synset] = wordnet_service.get_synsets_by_pos('r')
adverb_satellites: List[Synset] = wordnet_service.get_synsets_by_pos('s')

# if parent_phrase:
#     node, reconstructed_noun = reconstruct_hyphenated_noun(parent_phrase)
#     print(f"\nParent phrase node containing HYPH:")
#     print(f"pos: {parent_phrase.pos}, label: {parent_phrase.label}")
#     sub_generator = TreeGenerator(parent_phrase)
#     print('\n'.join(sub_generator.build_tree()))
#     print(f"Reconstructed noun is: {reconstructed_noun}")
#     singular = wnl.lemmatize(reconstructed_noun, 'n')
#     print(f"Singular noun is: {singular}")
#     wordnet_term = wordnet_service.get_synsets(singular)
#     print(f"Wordnet term is: {wordnet_term}")
#     try:
#         term = open_cyc_service.search_term(singular)
#         print(f"Cyc term is: {term}")
#         cyc_synset = get_cyc_synset(node, nlp_service, ollama_service, open_cyc_service, wnl)
#         comment_text = open_cyc_service.query_sentence(f"(#$comment {singular.capitalize()} ?ARG2)")
#         cyc_english_word = open_cyc_service.query_sentence(f"(#$prettyString-Canonical ?TERM \"{singular}\")", mt_monad='EnglishMt')
#         word_instances = open_cyc_service.query_sentence(f"(#$isa #${singular.capitalize()} ?ARG2)", mt_monad='BaseKB')
#     except AttributeError as ae:
#         print(ae)
#     except ValueError:
#         print(f"CycSynset for term {singular} does not exist.")
#         assert_cyc_synset(open_cyc_service)
# else:
#     print("No parent phrase node containing HYPH found.")
