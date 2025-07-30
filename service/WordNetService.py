import logging
from typing import List, Optional, Set

try:
    import nltk
    from nltk.corpus import wordnet as wn
except ImportError:
    raise ImportError("NLTK is required for WordNetService. Install with 'pip install nltk'.")


class WordNetService:
    def __init__(self, parent_logger: logging.Logger, download_corpus: bool = True):
        self.logger = parent_logger.getChild('WordNetService')
        try:
            if download_corpus:
                nltk.download('wordnet', quiet=True)
            wn.ensure_loaded()
            self.logger.info("WordNet corpus loaded successfully.")
        except Exception as e:
            self.logger.warning(f"Failed to load WordNet corpus: {e}. Falling back to offline mode if available.")

    def get_synsets(self, word: str) -> List:
        synsets = wn.synsets(word)
        if not synsets:
            self.logger.debug(f"No synsets found for word: {word}")
        return synsets

    def get_synonyms(self, word: str, pos: Optional[str] = None) -> Set[str]:
        synonyms = set()
        for synset in self.get_synsets(word):
            if pos and synset.pos() != pos:
                continue
            for lemma in synset.lemmas():
                synonyms.add(lemma.name().replace('_', ' '))
        return synonyms - {word}

    def get_hypernyms(self, word: str, depth: int = 1) -> Set[str]:
        hypernyms = set()
        for synset in self.get_synsets(word):
            current = synset
            for _ in range(depth):
                hypers = current.hypernyms()
                if not hypers:
                    break
                current = hypers[0]
                hypernyms.add(current.name().split('.')[0].replace('_', ' '))
        return hypernyms

    def semantic_similarity(self, word1: str, word2: str) -> Optional[float]:
        synsets1 = self.get_synsets(word1)
        synsets2 = self.get_synsets(word2)
        if not synsets1 or not synsets2:
            return None
        max_sim = max((wn.wup_similarity(s1, s2) or 0) for s1 in synsets1 for s2 in synsets2)
        return max_sim if max_sim > 0 else None
