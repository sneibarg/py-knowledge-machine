from typing import List

from processor.nlp.PartOfSpeech import PartOfSpeech
from processor.nlp.Synset import Synset
from service.CycLService import CycLService
from service.NlpService import NlpService
from service.OllamaService import OllamaService

model_name = "gpt-oss:20b"
base_prompt = ("I am your automated ontology editor, and I am reviewing the results of a CycL query."
               "I will be given a list of OpenCyc instances. "
               "I understand that ?INSTANCE instances are a feature of the OpenCyc platform."
               "OpenCyc describes them as such: ?COMMENT")


def get_cyc_synset(node, nlp_service, ollama_service, cycl_service, lemmatizer):
    if node.label is None:
        return None
    try:
        pos_map = {
            PartOfSpeech.NN.value: 'n', PartOfSpeech.NNS.value: 'n',
            PartOfSpeech.NNP.value: 'n', PartOfSpeech.NNPS.value: 'n',
            PartOfSpeech.VB.value: 'v', PartOfSpeech.VBD.value: 'v',
            PartOfSpeech.VBG.value: 'v', PartOfSpeech.VBN.value: 'v',
            PartOfSpeech.VBP.value: 'v', PartOfSpeech.VBZ.value: 'v',
            PartOfSpeech.JJ.value: 'a', PartOfSpeech.JJR.value: 'a',
            PartOfSpeech.JJS.value: 'a', PartOfSpeech.RB.value: 'r',
            PartOfSpeech.RBR.value: 'r', PartOfSpeech.RBS.value: 'r'
        }
        pos_tag = PartOfSpeech.from_tag(node.pos)
        if pos_tag == PartOfSpeech.PUNCT:
            return None
        lemma_pos = pos_map.get(node.pos, 'n')
        singular = lemmatizer.lemmatize(node.label, lemma_pos)
        return CycSynset(singular, node.pos, nlp_service, ollama_service, cycl_service)
    except ValueError as ve:
        print(ve)
        return None


class CycSynset(Synset):
    def __init__(self, term: str, pos: str, nlp_service: NlpService, ollama_service: OllamaService,
                 cycl_service: CycLService):
        super().__init__(term, pos, nlp_service, ollama_service)
        self.cycl_service = cycl_service
        self.predicate_comment = self.__init_predicate_comment()
        self.function_comment = self.__init_function_comment()
        self.term_comment = self.__init_term_comment()
        self.predicates = self.__init_predicates()
        self.functions = self.__init_functions()
        self.model_name = "gpt-oss:20b"

    def __relevant(self, answers, instance_type) -> List:
        prompt = base_prompt.replace("?INSTANCE", instance_type)
        if instance_type == "Predicate":
            prompt = prompt.replace("?COMMENT", self.predicate_comment)
        else:
            prompt = prompt.replace("?COMMENT", self.function_comment)
        text = f"The OpenCyc result set for {self.term} is listed as follows: {' '.join(answers.values())}.\n"
        text = text + "I will reason through this list.\n"
        text = text + "As part of my reasoning process, I will determine whether the instance is a member of the upper, middle, or lower ontology.\n"
        text = text + "I will output the OpenCyc result set as three lines each of comma-separated (with a space after the comma) names without introducing any other text.\n"
        print(f"Submitting Ollama request with the following prompt: \n{prompt}\n\nPrompt text is {text}")
        # response = self.ollama_service.one_shot(model_name, text, prompt)
        # print(f"The response was: {str(response)}")
        return self.ollama_service.one_shot(model_name, text, prompt).split('\n')

    def __init_term_comment(self) -> str:
        query = f"(comment {self.term.capitalize()} ?TEXT)"
        term_comment = self.cycl_service.query_sentence(query, mt_monad="EnglishMt")['answers']['[Explain]']
        return term_comment

    def __init_predicate_comment(self) -> str:
        query = f"(comment #$Predicate ?TEXT)"
        return self.cycl_service.query_sentence(query, mt_monad="BaseKB")['answers']['[Explain]']

    def __init_function_comment(self) -> str:
        query = f"(comment #$CollectionDenotingFunction ?TEXT)"
        return self.cycl_service.query_sentence(query, mt_monad="BaseKB")['answers']['[Explain]']

    def __init_predicates(self) -> List:
        query = f"(#$isa {self.term.capitalize()} #$Predicate)"  # returns Query is not proven when unlinked
        answers = self.cycl_service.query_sentence(query, mt_monad="BaseKB")['answers']
        if not answers:
            return self.__link_predicates()
        else:
            return answers

    def __init_functions(self) -> List:
        query = f"(#$isa {self.term.capitalize()} #$CollectionDenotingFunction)"
        answers = self.cycl_service.query_sentence(query, mt_monad="BaseKB")['answers']
        if not answers:
            return self.__link_functions()
        else:
            return answers

    def __link_predicates(self) -> List:
        query = f"(#$isa #$Predicate ?ARG2)"
        answers = self.cycl_service.query_sentence(query, mt_monad="BaseKB")['answers']
        return self.__relevant(answers, "Predicate")

    def __link_functions(self) -> List:
        query = f"(#$isa #$CollectionDenotingFunction ?ARG2)"
        answers = self.cycl_service.query_sentence(query, mt_monad="BaseKB")['answers']
        return self.__relevant(answers, "CollectionDenotingFunction")
