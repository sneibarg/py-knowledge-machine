from service.NlpService import NlpService


class Synset:
    def __init__(self, term: str, pos: str, nlp_service: NlpService):
        self.term = term
        self.pos = pos
        self.nlp_service = nlp_service
        self.relations = nlp_service.stanford_relations(term)
        self.openie_triples = nlp_service.stanford_relations(term, True)
