from enum import Enum


class PartOfSpeech(Enum):
    """
    An immutable class representing parts of speech as defined in the Penn Treebank tag set
    used by the Stanford Parser.
    """
    CC = "CC"  # Coordinating conjunction (e.g., and, but, or)
    CD = "CD"  # Cardinal number (e.g., one, two, 100)
    DT = "DT"  # Determiner (e.g., the, a, an)
    EX = "EX"  # Existential there (e.g., there in "there is")
    FW = "FW"  # Foreign word (e.g., ad hoc, laissez faire)
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
    PUNCT = ["#", "$", "''", "(", ")", ",", ".", ":", "``"]  # Punctuation tags

    @classmethod
    def is_punctuation(cls, tag: str) -> bool:
        """
        Check if a given tag represents punctuation.

        Args:
            tag (str): The POS tag to check.

        Returns:
            bool: True if the tag is a punctuation tag, False otherwise.
        """
        return tag in cls.PUNCT.value

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
        for member in cls:
            if member is cls.PUNCT:
                if tag in member.value:
                    return member
            elif member.value == tag:
                return member
        raise ValueError(f"Invalid POS tag: {tag}")

    def __str__(self) -> str:
        """
        Return the string representation of the POS tag.

        Returns:
            str: The tag value or a string representation for punctuation.
        """
        if self is PartOfSpeech.PUNCT:
            return "PUNCT"
        return self.value
