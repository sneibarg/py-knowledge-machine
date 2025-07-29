from processor.dataset.DatasetProcessor import DatasetProcessor


class CycReasoningAgent:
    def __init__(self, cyc_server_agent, parent_logger):
        self.logger = parent_logger
        self.cyc_server_agent = cyc_server_agent
        self.dataset_processor = DatasetProcessor(self.logger, 3)
        self.microtheories = self.cyc_server_agent.query_sentence("(#$isa ?ARG1 #$Microtheory)", mt_monad='BaseKB')['answers'].values()
        self.predicate_microtheories = []
        self._populate_predicate_microtheories()
        self.predicates = None

    def _populate_predicate_microtheories(self):
        print(str(self.microtheories))

    def mistral_analysis(self):
        base_prompt = ("I am your automated ontology editor, and I am reviewing our current set of Microtheories."
                       "I will generate a concise summary of the following list: ")
        result = self.dataset_processor.mistral_one_shot(' '.join(self.microtheories), base_prompt)
        print(str(result))
