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
    def __init__(self, term: str, pos: str, nlp_service: NlpService, ollama_service: OllamaService,
                 cycl_service: CycLService):
        super().__init__(term, pos, nlp_service, ollama_service)
        self.cycl_service = cycl_service
        self.predicate_comment = self.__init_predicate_comment()
        self.function_comment = self.__init_function_comment()
        self.term_comment = self.__init_term_comment()
        self.predicates = self.__init_predicates()
        self.functions = self.__init_functions()

    def __relevant(self, answers, instance_type) -> List:
        prompt = base_prompt.replace("?INSTANCE", instance_type)
        if instance_type == "Predicate":
            prompt = prompt.replace("?COMMENT", self.predicate_comment)
        else:
            prompt = prompt.replace("?COMMENT", self.function_comment)

        text = f"The OpenCyc result set for {self.term} is listed as follows: {' '.join(answers)}.\n"
        text = text + "I will reason through this list and return a much smaller list to assert to a different Microtheory.\n"
        text = text + "As part of my reasoning process, I will determine whether the instance is a member of the upper, middle, or lower ontology."
        text = text + "The selected instances are: "
        return self.ollama_service.one_shot(text, prompt).replace("The selected instances are: ", "").split(" ")

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
        return self.__relevant(answers, "Predicate")

    def __link_functions(self) -> List:
        query = f"(#$isa #$CollectionDenotingFunction ?ARG2)"
        answers = self.cycl_service.query_sentence(query, mt_monad="BaseKB")['answers']
        return self.__relevant(answers, "CollectionDenotingFunction")
