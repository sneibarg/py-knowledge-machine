from typing import List
from processor.nlp.Synset import Synset
from service.CycLService import CycLService
from service.NlpService import NlpService
from service.OllamaService import OllamaService

base_prompt = ("I am your automated ontology editor, and I am reviewing the results of a CycL query."
               "I will be given a list of OpenCyc instances. "
               "I understand that ?INSTANCE instances are a feature of the OpenCyc platform."
               "OpenCyc describes them as such: ?COMMENT")


class CycSynset(Synset):
    def __init__(self, term: str, pos: str, nlp_service: NlpService, ollama_service: OllamaService, cycl_service: CycLService):
        super().__init__(term, pos, nlp_service, ollama_service)
        self.ollama_service = ollama_service
        self.nlp_service = nlp_service
        self.cycl_service = cycl_service
        self.predicate_comment = self.__init_predicate_comment()
        self.function_comment = self.__init_function_comment()
        self.term_comment = self.__init_term_comment()
        self.predicates = self.__init_predicates()
        self.functions = self.__init_functions()

    def __init_term_comment(self) -> str:
        query = f"(comment {self.term.capitalize()} ?TEXT)"
        term_comment = self.cycl_service.query_sentence(query, mt_monad="EnglishMt")['answers'].items()[0]
        return term_comment

    def __init_predicate_comment(self) -> str:
        query = f"(comment #$Predicate ?TEXT)"
        return self.cycl_service.query_sentence(query, mt_monad="BaseKB")['answers'].items()[0]

    def __init_function_comment(self) -> str:
        query = f"(comment #$CollectionDenotingFunction ?TEXT)"
        return self.cycl_service.query_sentence(query, mt_monad="BaseKB")['answers'].items()[0]

    def __init_predicates(self) -> List:
        query = f"(#$isa {self.term.capitalize()} #$Predicate)"  # returns Query is not proven when unlinked
        answers = self.cycl_service.query_sentence(query, mt_monad="BaseKB")['answers']
        if answers.keys() < 1:
            return self.__link_predicates()
        else:
            return answers

    def __init_functions(self) -> List:
        query = f"(#$isa {self.term.capitalize()} #$CollectionDenotingFunction)"
        answers = self.cycl_service.query_sentence(query, mt_monad="BaseKB")['answers']
        if answers.keys() < 1:
            return self.__link_functions()
        else:
            return answers

    def __link_predicates(self) -> List:
        query = f"(#$isa #$Predicate ?ARG2)"
        answers = self.cycl_service.query_sentence(query, mt_monad="BaseKB")['answers']
        relevant_answers = []
        return relevant_answers

    def __link_functions(self) -> List:
        query = f"(#$isa #$CollectionDenotingFunction ?ARG2)"
        answers = self.cycl_service.query_sentence(query, mt_monad="BaseKB")['answers']
        relevant_answers = []
        return relevant_answers
