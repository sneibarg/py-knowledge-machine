from processor.nlp import PartOfSpeech

PIPE = "│"
ELBOW = "└──"
TEE = "├──"
PIPE_PREFIX = "│   "
SPACE_PREFIX = "    "


class Node:
    def __init__(self, label, pos, children=None, cyc_term=None):
        self.label = label
        self.pos = pos
        self.children = children or []
        self.cyc_term = cyc_term


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
