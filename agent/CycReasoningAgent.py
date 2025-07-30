import logging
import os
import time

from agent.CycLServerAgent import CycLServerAgent
from service.OllamaService import OllamaService

ollama_model = "gemma3n:latest"


class CycReasoningAgent:
    def __init__(self, ollama_service: OllamaService, cyc_server_agent: CycLServerAgent, logger: logging.Logger):
        self.logger = logger
        self.cyc_server_agent = cyc_server_agent
        self.ollama_service = ollama_service
        self.microtheories = []
        self.predicate_microtheories = []
        self._populate_microtheories()
        self.predicates = None

    def _populate_microtheories(self):
        mt_list = self.cyc_server_agent.query_sentence("(#$isa ?ARG1 #$Microtheory)", mt_monad='BaseKB')['answers']
        for explanation in mt_list:
            mt = mt_list[explanation]
            if not mt.startswith("(") and mt.endswith("Mt"):
                self.microtheories.append(mt)
            else:
                self.predicate_microtheories.append(mt)

    def _populate_predicates(self):
        predicate_list = self.cyc_server_agent.query_sentence("(#$isa ?ARG1 #$Predicate", mt_monad='BaseKB')['answers']
        print("PREDICATES="+str(predicate_list))

    def gemma3n_analysis(self):
        base_prompt = ("I am your automated ontology editor, and I am reviewing our current set of Microtheories."
                       "I understand that Microtheories are a feature of the OpenCyc platform."
                       "The intention of microtheories is to organize human knowledge in ways that enable valid reasoning."
                       "Given a microtheory name, I will provide a concise and accurate answer describing the microtheory name."
                       "The given microtheory is: ")
        for mt in self.microtheories:
            start_time = time.time()
            mt_dir = os.path.join(os.getcwd(), "runtime", "microtheories", mt)
            if not os.path.exists(mt_dir):
                os.makedirs(mt_dir)
            filename = os.path.join(mt_dir, f"{mt}.gemma-analysis.txt")
            result = self.ollama_service.one_shot(ollama_model, mt, base_prompt)
            if result is None:
                result = "None"
            elapsed_time = time.time() - start_time
            with open(filename, 'w') as fh:
                fh.write(result)
                fh.write(f"-------\n")
                fh.write(f"Inference for microtheory took {elapsed_time} seconds.")
            fh.close()

