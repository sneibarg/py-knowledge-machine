from enum import Enum
from typing import List, Union, Dict


class PartOfSpeech(Enum):
    PUNCT = "PUNCT"  # Special category for punctuation

    # Clause-level tags (formerly in Clause)
    S = "S"  # Sentence
    SBAR = "SBAR"  # Clause introduced by a (possibly empty) subordinating conjunction
    SBARQ = "SBARQ"  # Direct question introduced by a wh-word or a wh-phrase. Indirect questions and relative clauses should be bracked as SBAR - not SBARQ.
    SINV = "SINV"  # Inverted declarative sentence, i.e., one in which the subject follows the tensed verb or modal.
    SQ = "SQ"  # Inverted yes/no question, or main clause of a wh-question, following the wh-phrase in SBARQ.

    # Phrase-level tags (formerly in Phrase)
    ADJP = "ADJP"  # Adjective phrase
    ADVP = "ADVP"  # Adverb phrase
    CONJP = "CONJP"  # Conjunction phrase
    FRAG = "FRAG"  # Fragment
    INTJ = "INTJ"  # Interjection. Corresponds approximately to the part-of-speech tag UH.
    LST = "LST"  # List marker. Includes surrounding punctuation
    NAC = "NAC"  # Not a constituent; used to show the scope of certain prenominal modifieers within an NP.
    NP = "NP"  # Noun phrase
    NX = "NX"  # Used within certain complex NPs to mark the head of the NP. Corresponds very roughly to N-bar level but used quite differently.
    PP = "PP"  # prepositional phrase
    PRN = "PRN"  # Parenthetical
    PRT = "PRT"  # Particle
    QP = "QP"  # Quantifier Phrase (i.e. complex measure/amount phrase); used within NP.
    RRC = "RRC"  # Reduced Relative Clause.
    UCP = "UCP"  # Unlike Coordinated Phrase.
    VP = "VP"  # Verb Phrase.
    WHADJP = "WHADJP"  # WH-adjective phrase. Adjectival phrase containing a wh-adverb, as in how hot.
    WHAVP = "WHAVP"  # WH-adverb phrase. Introduces a clause within an NP gap. May be null ( containing the 0 complementizer) or lexical, containing a wh-adverb such as how or why.
    WHNP = "WHNP"  # WH-noun phrase. Introduces a clause with an NP gap. May be null (containing the 0 complementizer) or lexical, containing some wh-word, e.g. who, which book, whose daughter, none of which, or how many leopards.
    WHPP = "WHPP"  # WH-prepositional phrase. Prepositional prhase containing a wh-noun phrase ( such as of which or by whose authority) that either introduces a PP gap or is contained by a WHNP.
    X = "X"  # Unknown, uncertain, or unbracketable. X is often used for bracketing typos and in bracketing the...the-constructions.

    # Word-level tags (formerly in Word)
    CC = "CC"  # Coordinating conjunction (e.g., and, but, or)
    CD = "CD"  # Cardinal number (e.g., one, two, 100)
    DT = "DT"  # Determiner (e.g., the, a, an)
    EX = "EX"  # Existential there (e.g., there in "there is")
    FW = "FW"  # Foreign word (e.g., ad hoc, laissez faire)
    HYPH = "HYPH"  # hyphenated words and phrases
    IN = "IN"  # Preposition or subordinating conjunction (e.g., in, of, because)
    JJ = "JJ"  # Adjective (e.g., big, happy)
    JJR = "JJR"  # Adjective, comparative (e.g., bigger, happier)
    JJS = "JJS"  # Adjective, superlative (e.g., biggest, happiest)
    LS = "LS"  # List item marker (e.g., 1., 2., a., b.)
    MD = "MD"  # Modal (e.g., can, will, should)
    NN = "NN"  # Noun, singular or mass (e.g., dog, water)
    NNS = "NNS"  # Noun, plural (e.g., dogs, waters)
    NNP = "NNP"  # Proper noun, singular (e.g., John, London)
    NNPS = "NNPS"  # Proper noun, plural (e.g., Americans, Smiths)
    PDT = "PDT"  # Predeterminer (e.g., all, both in "all the books")
    POS = "POS"  # Possessive ending (e.g., 's in "John's")
    PRP = "PRP"  # Personal pronoun (e.g., I, you, he, she)
    PRP_dollar = "PRP$"  # Possessive pronoun (e.g., my, your, his)
    RB = "RB"  # Adverb (e.g., quickly, silently)
    RBR = "RBR"  # Adverb, comparative (e.g., faster, more quickly)
    RBS = "RBS"  # Adverb, superlative (e.g., fastest, most quickly)
    RP = "RP"  # Particle (e.g., up, off in "pick up")
    SYM = "SYM"  # Symbol (e.g., %, $, &)
    TO = "TO"  # to (e.g., to as preposition or infinitive marker)
    UH = "UH"  # Interjection (e.g., oh, wow, uh)
    VB = "VB"  # Verb, base form (e.g., run, be)
    VBD = "VBD"  # Verb, past tense (e.g., ran, was)
    VBG = "VBG"  # Verb, gerund/present participle (e.g., running, being)
    VBN = "VBN"  # Verb, past participle (e.g., run, been)
    VBP = "VBP"  # Verb, non-3rd person singular present (e.g., run, am)
    VBZ = "VBZ"  # Verb, 3rd person singular present (e.g., runs, is)
    WDT = "WDT"  # Wh-determiner (e.g., which, that in "the book which...")
    WP = "WP"  # Wh-pronoun (e.g., who, what)
    WP_dollar = "WP$"  # Possessive wh-pronoun (e.g., whose)
    WRB = "WRB"  # Wh-adverb (e.g., where, when, how)

    PUNCT_TAGS: List[str] = ["#", "$", "''", "(", ")", ",", ".", ":", "``"]
    CLAUSE_TAGS: List['PartOfSpeech'] = [S, SBAR, SBARQ, SINV, SQ]
    PHRASE_TAGS: List['PartOfSpeech'] = [ADJP, ADVP, CONJP, FRAG, INTJ, LST, NAC, NP, NX, PP, PRN, PRT, QP, RRC, UCP, VP, WHADJP, WHAVP, WHNP, WHPP, X]
    WORD_TAGS: List['PartOfSpeech'] = [CC, CD, DT, EX, FW, HYPH, IN, JJ, JJR, JJS, LS, MD, NN, NNS, NNP, NNPS, PDT, POS, PRP, PRP_dollar, RB, RBR, RBS, RP, SYM, TO, UH, VB, VBD, VBG, VBN, VBP, VBZ, WDT, WP, WP_dollar, WRB]
    NOUN_TAGS: List['PartOfSpeech'] = [NN, NNS, NNP, NNPS]
    VERB_TAGS: List['PartOfSpeech'] = [VBZ, VBP, VBN, VBG, VBD]

    @classmethod
    def is_punctuation(cls, tag: str) -> bool:
        """
        Check if a given tag represents punctuation.

        Args:
            tag (str): The POS tag to check.

        Returns:
            bool: True if the tag is a punctuation tag, False otherwise.
        """
        return tag in cls.PUNCT_TAGS

    @classmethod
    def from_tag(cls, tag: str) -> 'PartOfSpeech':
        """
        Get the PartOfSpeech enum member from a tag string.

        Args:
            tag (str): The POS tag string.

        Returns:
            PartOfSpeech: The corresponding enum member.

        Raises:
            ValueError: If the tag is not a valid Penn Treebank tag.
        """
        if tag in cls.PUNCT_TAGS:
            return cls.PUNCT
        for member in cls:
            if member.value == tag:
                return member
        raise ValueError(f"Invalid POS tag: {tag}")

    @classmethod
    def get_tag_map(cls, category: str = None) -> Union[Dict[str, List[str]], List[str]]:
        """
        Return a dictionary of POS tag lists for all categories or a list of tags for a specific category.

        Args:
            category (str, optional): The category of tags to retrieve. Valid options are:
                                     'punct', 'clause', 'phrase', 'word', 'noun'. If None, returns
                                     a dictionary of all categories.

        Returns:
            Union[Dict[str, List[str]], List[str]]: Dictionary mapping categories to tag lists if
                                                    category is None, otherwise a list of tag strings
                                                    for the specified category.

        Raises:
            ValueError: If the category is invalid.
        """
        tag_map = {
            'punct': cls.PUNCT_TAGS,
            'clause': [tag for tag in cls.CLAUSE_TAGS.__str__()],
            'phrase': [tag for tag in cls.PHRASE_TAGS.__str__()],
            'word': [tag for tag in cls.WORD_TAGS.__str__()],
            'noun': [tag for tag in cls.NOUN_TAGS.__str__()],
            'verb': [tag for tag in cls.VERB_TAGS.__str__()]
        }
        if category is None:
            return tag_map
        category = category.lower()
        if category not in tag_map:
            raise ValueError(f"Invalid category: {category}. Valid options are: {', '.join(tag_map.keys())}")
        return tag_map[category]

    def __str__(self) -> str:
        """
        Return the string representation of the POS tag.

        Returns:
            str: The tag value.
        """
        return self.value
