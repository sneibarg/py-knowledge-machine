import logging
from typing import List, Tuple, Set, Dict


class NounIntersectionRanker:
    """A class to rank items based on the intersection of nouns from a parseTree with key terms."""

    def __init__(self, key_terms: Set[str], ontology: Set[str]):
        self.logger = logging.getLogger('OWL-to-KM.NounIntersectionRanker')
        self.key_terms = key_terms
        self.cyc_concepts = ontology  # TO-DO
        self.logger.info("Initialized NounIntersectionRanker with %d key terms", len(key_terms))

    def extract_nouns(self, parse_tree: Dict) -> Set[str]:
        """Extract nouns from a parse tree.
        Returns:
            Set[str]: A set of lowercase nouns extracted from the parse tree.
        """
        nouns = set()
        if "sentences" in parse_tree:
            for sentence in parse_tree.get("sentences", []):
                for token in sentence.get("tokens", []):
                    word = token.get("word")
                    pos = token.get("pos", "")
                    if pos.startswith("N") and word is not None:
                        nouns.add(word.lower())
            self.logger.debug("Extracted %d nouns from parse tree", len(nouns))
        else:
            self.logger.warning("No 'sentences' key in parse tree")
        return nouns

    def rank_items(self, items: List[Tuple[str, Dict]]) -> List[Tuple[str, int]]:
        """Rank items based on the intersection of nouns in their parse trees with key terms.
        Returns:
            List[Tuple[str, int]]: Ranked list of tuples containing item text and score.
        """
        self.logger.info("Ranking %d items", len(items))
        ranked_items = []

        for item_text, parse_tree in items:
            if parse_tree and "parseTree" in parse_tree:  # Check for parseTree
                nouns = self.extract_nouns(parse_tree["parseTree"])
                score = len(nouns.intersection(self.key_terms))
                ranked_items.append((item_text, score))
                self.logger.debug("Score for item '%s...': %d", item_text[:30], score)
            else:
                self.logger.warning("No parseTree found for item: %s...", item_text[:30])
                ranked_items.append((item_text, 0))  # Default score if no parseTree

        # Sort by score in descending order
        ranked_items.sort(key=lambda x: x[1], reverse=True)
        self.logger.info("Ranking complete; top score: %d", ranked_items[0][1] if ranked_items else 0)
        return ranked_items
