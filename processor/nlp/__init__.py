from typing import List, Optional, Tuple
from processor.nlp.PartOfSpeech import PartOfSpeech

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


class Node:
    def __init__(self, label, pos, children=None, cyc_term=None):
        self.label = label
        self.pos = pos
        self.children = children or []
        self.cyc_term = cyc_term


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


PIPE = "│"
ELBOW = "└──"
TEE = "├──"
PIPE_PREFIX = "│   "
SPACE_PREFIX = "    "


class TreeGenerator:
    def __init__(self, root):
        self._tree = []
        self._root = root

    def build_tree(self):
        self._tree.append(self.get_display(self._root))
        self._add_children(self._root.children, "")
        return self._tree

    def _add_children(self, children, prefix):
        entries_count = len(children)
        for index, child in enumerate(children):
            connector = ELBOW if index == entries_count - 1 else TEE
            self._tree.append(f"{prefix}{connector} {self.get_display(child)}")
            if child.children:
                new_prefix = prefix + (SPACE_PREFIX if index == entries_count - 1 else PIPE_PREFIX)
                self._add_children(child.children, new_prefix)

    def get_display(self, node):
        if node.label is not None:
            return f"{node.label} ({node.pos})"
        else:
            return node.pos

    def get_nodes_by_type(self, node_types):
        def collect(node):
            res = []
            if node.pos in node_types:
                res.append(node)
            for child in node.children:
                res.extend(collect(child))
            return res

        return collect(self._root)

    def repopulate_nodes(self, populator):
        """
        Repopulates the nodes with CycTerm instances.
        populator: a function that takes a Node and returns a CycTerm instance or None.
        """

        def recurse(node):
            node.cyc_term = populator(node)
            for child in node.children:
                recurse(child)

        recurse(self._root)

    def get_leaves(self):
        """
        Returns a list of all leaf nodes in the tree, in left-to-right order.
        """

        def collect(node):
            if not node.children:
                return [node]
            res = []
            for child in node.children:
                res.extend(collect(child))
            return res

        return collect(self._root)

    def get_top_level_node(self, target_node_type):
        """
        Returns the top-level phrase node containing a node of target_node_type.
        The top-level phrase is the highest ancestor with a phrase label (as defined in
        PartOfSpeech.PHRASE_TAGS) that is not nested within another node of the same phrase label.

        Args:
            target_node_type (str): The pos type of the node to search for (e.g., 'HYPH').

        Returns:
            Node: The top-level phrase node containing a node of target_node_type,
                  or None if not found.
        """
        phrase_tags = [tag for tag in PartOfSpeech.PHRASE_TAGS.__str__()]

        def find_top_level_phrase(node):
            """
            Find the highest ancestor that is a phrase node, not nested within
            another node of the same phrase label.
            """
            if not node:
                return None

            candidate = node if node.pos in phrase_tags else None
            parent = getattr(node, '_parent', None)
            if parent:
                parent_result = find_top_level_phrase(parent)
                if parent_result and parent_result.pos != node.pos:
                    return parent_result
                return candidate
            return candidate

        target_nodes = self.get_nodes_by_type([target_node_type])
        if not target_nodes:
            return None

        def set_parents(node, parent=None):
            node._parent = parent
            for child in node.children:
                set_parents(child, node)
        set_parents(self._root)

        for target_node in target_nodes:
            top_level_phrase = find_top_level_phrase(target_node)
            if top_level_phrase:
                return top_level_phrase

        return None

    def get_parent_phrase(self, target_node_type):
        """
        Returns the immediate parent phrase node containing a node of target_node_type.
        The parent phrase is the closest ancestor with a phrase label (as defined in
        PartOfSpeech.PHRASE_TAGS).

        Args:
            target_node_type (str): The pos type of the node to search for (e.g., 'HYPH').

        Returns:
            Node: The immediate parent phrase node containing a node of target_node_type,
                  or None if not found.
        """
        phrase_tags = [tag for tag in PartOfSpeech.PHRASE_TAGS.__str__()]

        def find_parent_phrase(node):
            """
            Find the immediate parent that is a phrase node.
            """
            parent = getattr(node, '_parent', None)
            if parent and parent.pos in phrase_tags:
                return parent
            return None

        target_nodes = self.get_nodes_by_type([target_node_type])
        if not target_nodes:
            return None

        def set_parents(node, parent=None):
            node._parent = parent
            for child in node.children:
                set_parents(child, node)
        set_parents(self._root)

        for target_node in target_nodes:
            parent_phrase = find_parent_phrase(target_node)
            if parent_phrase:
                return parent_phrase  # Return the first match
        return None


def translate_parse_tree(tree_or_s, print_tree=False) -> TreeGenerator:
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