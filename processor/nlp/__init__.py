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
