from typing import List, Optional, Tuple
from processor.nlp.PartOfSpeech import PartOfSpeech
from processor.nlp.TreeGenerator import TreeGenerator, Node

tag_map = PartOfSpeech.get_tag_map()


def tokenize(s):
    s = s.replace('(', ' ( ')
    s = s.replace(')', ' ) ')
    return [token for token in s.split() if token]  # Remove any empty tokens


def parse(tokens):
    if not tokens:
        raise ValueError("Empty tokens")

    token = tokens.pop(0)
    if token != '(':
        raise ValueError("Expected '('")

    label = tokens.pop(0)
    children = []

    while tokens:
        next_token = tokens[0]
        if next_token == ')':
            tokens.pop(0)
            return [label, children]
        elif next_token == '(':
            children.append(parse(tokens))
        else:
            children.append(tokens.pop(0))

    raise ValueError("Unclosed parenthesis")


def to_node(tree):
    if isinstance(tree, str):
        return Node(label=tree, pos=None)
    label, children = tree
    parsed_children = [to_node(c) for c in children]
    if len(parsed_children) == 1 and isinstance(parsed_children[0], Node) and parsed_children[0].pos is None:
        word_node = parsed_children[0]
        return Node(label=word_node.label, pos=label, children=[])
    else:
        return Node(label=None, pos=label, children=parsed_children)


def translate_parse_tree(tree_or_s, logger=None, log_tree=False, print_tree=False) -> TreeGenerator:
    if isinstance(tree_or_s, str):
        tokens = tokenize(tree_or_s)
        tree = parse(tokens)
    else:
        tree = tree_or_s  # Assume it's already the parsed tree structure as [label, children]
    root = to_node(tree)
    generator = TreeGenerator(root)
    lines = generator.build_tree()
    if print_tree:
        print('\n'.join(lines))
    if log_tree and logger is not None:
        logger.info('\n'.join(line.encode('utf-8').decode('utf-8') for line in lines))
    return generator


def get_all_clauses(tree_generator: TreeGenerator) -> List[Node]:
    clauses = [tree_generator.get_nodes_by_type(tag) for tag in tag_map['clause']]
    flattened_clauses = [tag_node for tag_nodes in clauses for tag_node in tag_nodes]
    return flattened_clauses


def get_all_verbs(tree_generator: TreeGenerator) -> List[Node]:
    verbs = [tree_generator.get_nodes_by_type(tag) for tag in tag_map['verb']]
    flattened_verbs = [tag_node for tag_nodes in verbs for tag_node in tag_nodes]
    return flattened_verbs


def get_all_nouns(tree_generator: TreeGenerator) -> List[Node]:
    nouns = [tree_generator.get_nodes_by_type(tag) for tag in tag_map['noun']]
    flattened_nouns = [tag_node for tag_nodes in nouns for tag_node in tag_nodes]
    return flattened_nouns


def get_hyphenated_phrases(tree_generator: TreeGenerator) -> List[PartOfSpeech]:
    hyphenated_phrases = []
    hyphenated_nouns = tree_generator.get_nodes_by_type(PartOfSpeech.HYPH.value)
    if len(hyphenated_nouns) > 0:
        hyphenation = tree_generator.get_parent_phrase(PartOfSpeech.HYPH.value)
        print(f"PARENT_PHRASE={hyphenation.pos}")
        print(f"PARENT_CHILDREN={hyphenation.children}")
        hyphenated_phrases.append(hyphenation.children)
    return hyphenated_phrases


def reconstruct_hyphenated_noun(node: Node) -> Optional[Tuple[Node, str]]:
    reconstructed_noun = None
    if not node.children:
        return None
    for child in node.children:
        if child.pos == PartOfSpeech.NN.value or child.label == PartOfSpeech.NNS.value and reconstructed_noun is None:
            reconstructed_noun = child.label + "-"
            node = child
        elif child.pos == PartOfSpeech.NNS.value:
            reconstructed_noun = reconstructed_noun + child.label
            node = child
    print(f"Returning reconstructed noun {reconstructed_noun}")
    return node, reconstructed_noun


def sentence_analysis(tree_generator: TreeGenerator):
    hyphenated_phrases = get_hyphenated_phrases(tree_generator)
    if hyphenated_phrases > 0:
        print(f"Sentence has {len(hyphenated_phrases)} hyphenated phrases!")