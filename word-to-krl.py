import os

from nltk.stem import WordNetLemmatizer
from agent.CycReasoningAgent import CycReasoningAgent
from processor.nlp import TreeGenerator, translate_parse_tree, reconstruct_hyphenated_noun
from processor.nlp.CycSynset import get_cyc_synset
from service.LoggingService import LoggingService
from service.NlpService import NlpService
from service.OllamaService import OllamaService
from service.OpenCycService import OpenCycService
from service.WordNetService import WordNetService

test_sentence = "if you can't sit up while laying flat then you could stand to do some sit-ups"
wnl = WordNetLemmatizer()
payload = {"word": "sit-up", "pos": "noun"}
wordnet_api = "http://dragon:9081/wordnet/synsets"
nlp_api_url = "http://dragon:9081/nlp"
ollama_api_url = "http://localhost:11434/api/generate"
cyc_host = "dragon:3602"
logging_service = LoggingService(os.path.join(os.getcwd(), "runtime", "logs"), "word-to-KRL")
logger = logging_service.setup_logging(False)
wordnet_service = WordNetService(logger)
open_cyc_service = OpenCycService(cyc_host, logger)
ollama_service = OllamaService(ollama_api_url, logger)
nlp_service = NlpService(nlp_api_url, logger)
cyc_reasoning_agent = CycReasoningAgent(ollama_service, open_cyc_service, logger)
relations = nlp_service.stanford_relations(test_sentence)
test_sentence = "if you can't sit up while laying flat then you could stand to do some sit-ups"
# parse_tree = "(ROOT (S (SBAR (IN if) (S (NP (PRP you)) (VP (MD ca) (RB n't) (VP (VB sit) (PRT (RP up)) (PP (IN while) (S (VP (VBG laying) (ADJP (JJ flat))))))))) (ADVP (RB then)) (NP (PRP you)) (VP (MD could) (VP (VB stand) (S (VP (TO to) (VP (VB do) (NP (DT some) (NN sit) (HYPH -) (NNS ups)))))))))"
parse_tree = str(relations['sentences'][0]['parseTree'])
generator = translate_parse_tree(parse_tree, print_tree=True)
parent_phrase = generator.get_parent_phrase('HYPH')

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
else:
    print("No parent phrase node containing HYPH found.")
