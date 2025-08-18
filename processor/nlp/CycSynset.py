from typing import List
from processor.nlp.Synset import Synset
from service.CycLService import CycLService
from service.NlpService import NlpService


class CycSynset(Synset):
    def __init__(self, term: str, pos: str, nlp_service: NlpService, cycl_service: CycLService):
        super().__init__(term, pos, nlp_service)
        self.cycl_service = cycl_service
        self.predicates = self.__init_predicates()
        self.functions = self.__init_functions()

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
