import os
from logging import Logger

from nltk.stem import WordNetLemmatizer
from agent.CycReasoningAgent import CycReasoningAgent
from processor.nlp import TreeGenerator, translate_parse_tree, reconstruct_hyphenated_noun, Node
from processor.nlp.CycSynset import get_cyc_synset
from service.LoggingService import LoggingService
from service.NlpService import NlpService
from service.OllamaService import OllamaService
from service.OpenCycService import OpenCycService
from service.WordNetService import WordNetService

test_sentence: str = "if you can't sit up while laying flat then you could stand to do some sit-ups"
wnl: WordNetLemmatizer = WordNetLemmatizer()
payload: dict = {"word": "sit-up", "pos": "noun"}
wordnet_api: str = "http://dragon:9081/wordnet/synsets"
nlp_api_url: str = "http://dragon:9081/nlp"
ollama_api_url: str = "http://localhost:11434/api/generate"
cyc_host: str = "dragon:3602"
logging_service: LoggingService = LoggingService(os.path.join(os.getcwd(), "runtime", "logs"), "word-to-KRL")
logger: Logger = logging_service.setup_logging(False)
wordnet_service: WordNetService = WordNetService(logger)
open_cyc_service: OpenCycService = OpenCycService(cyc_host, logger)
ollama_service: OllamaService = OllamaService(ollama_api_url, logger)
nlp_service: NlpService = NlpService(nlp_api_url, logger)
cyc_reasoning_agent: CycReasoningAgent = CycReasoningAgent(ollama_service, open_cyc_service, logger)
relations: dict = nlp_service.stanford_relations(test_sentence)
test_sentence: str = "if you can't sit up while laying flat then you could stand to do some sit-ups"
parse_tree: str = str(relations['sentences'][0]['parseTree'])
generator: TreeGenerator = translate_parse_tree(parse_tree, print_tree=True)
parent_phrase: Node = generator.get_parent_phrase('HYPH')

if parent_phrase:
    node, reconstructed_noun = reconstruct_hyphenated_noun(parent_phrase)
    print(f"\nParent phrase node containing HYPH:")
    print(f"pos: {parent_phrase.pos}, label: {parent_phrase.label}")
    sub_generator = TreeGenerator(parent_phrase)
    print('\n'.join(sub_generator.build_tree()))
    print(f"Reconstructed noun is: {reconstructed_noun}")
    singular = wnl.lemmatize(reconstructed_noun, 'n')
    print(f"Singular noun is: {singular}")
    wordnet_term = wordnet_service.get_synsets(singular)
    print(f"Wordnet term is: {wordnet_term}")
    try:
        term = open_cyc_service.search_term(singular)
        print(f"Cyc term is: {term}")
        cyc_synset = get_cyc_synset(node, nlp_service, ollama_service, open_cyc_service, wnl)
        cyc_english_word = open_cyc_service.query_sentence(f"(#$prettyString-Canonical ?TERM \"{singular}\")", mt_monad='EnglishMt')
        word_instances = open_cyc_service.query_sentence(f"(#$isa #${singular.capitalize()} ?ARG2)", mt_monad='BaseKB')
    except AttributeError as ae:
        print(ae)
    except ValueError:
        print(f"CycSynset for term {singular} does not exist.")
        sentence: str = None
        open_cyc_service.assert_sentence(sentence)
else:
    print("No parent phrase node containing HYPH found.")
