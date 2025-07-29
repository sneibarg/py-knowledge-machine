from typing import List


class CycReasoningAgent:
    def __init__(self, cyc_server_agent):
        self.cyc_server_agent = cyc_server_agent
        self.microtheories = self._populate_microtheories()
        self.predicates = None

    def _populate_microtheories(self) -> List:
        sentence = "(#$isa ?ARG1 #$Microtheory)"
        microtheory_list = self.cyc_server_agent.query_sentence(sentence, mt_monad='MtSpace')
        return microtheory_list
