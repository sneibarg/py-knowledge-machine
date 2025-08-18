from processor.nlp.Synset import Synset
from service.CycLService import CycLService
from service.NlpService import NlpService


class CycSynset(Synset):
    def __init__(self, term: str, pos: str, nlp_service: NlpService, cycl_service: CycLService):
        super().__init__(term, pos, nlp_service)
        self.cycl_service = cycl_service
        self.predicates = None
        self.functions = None

    def __init_predicates(self):
        query = f"(#$isa {self.term.capitalize()} #$Predicate)"
        self.functions = self.cycl_service.query_sentence(query, mt_monad="BaseKB")

    def __init_functions(self):
        query = f"(#$isa {self.term.capitalize()} #$CollectionDenotingFunction)"
        self.functions = self.cycl_service.query_sentence(query, mt_monad="BaseKB")

