from typing import List
from processor.nlp.Synset import Synset
from service.CycLService import CycLService
from service.NlpService import NlpService

base_prompt = "I am your automated ontology editor, and I am reviewing the results of CycL query."
predicate_prompt = base_prompt + ("I understand that Predicate instances are a feature of the OpenCyc platform."
                                  "OpenCyc describes them as such: A specialization of TruthFunction (q.v.). "
                                  "Each instance of Predicate is either a property of things (see UnaryPredicate) or a relationship holding between two or more things. "
                                  "Like other truth-functions, predicates, or rather the expressions that represent or denote them, are used to form sentences. "
                                  "More precisely, any CycL expression that denotes an instance of Predicate (and only such an expression) can appear in the \"0th\" (or \"arg0\") "
                                  "position (i.e. as the term following the opening parenthesis) of a CycLAtomicSentence (q.v.). "
                                  "Important specializations of Predicate include UnaryPredicate, BinaryPredicate, TernaryPredicate, QuaternaryPredicate, and QuintaryPredicate. "
                                  "Note that, despite its name, Predicate is a collection of relations, and not a collection of expressions that represent or denote such relations."
                                  "Given OpenCyc's description, and the name of the term, I will refine the result set."
                                  "The given predicates are likely relevant to be asserted: ")


class CycSynset(Synset):
    def __init__(self, term: str, pos: str, nlp_service: NlpService, cycl_service: CycLService):
        super().__init__(term, pos, nlp_service)
        self.cycl_service = cycl_service
        self.predicate_comment = self.__init_predicate_comment()
        self.function_comment = self.__init_function_comment()
        self.predicates = self.__init_predicates()
        self.functions = self.__init_functions()

    def __init_predicate_comment(self) -> str:
        pass

    def __init_function_comment(self) -> str:
        pass

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
