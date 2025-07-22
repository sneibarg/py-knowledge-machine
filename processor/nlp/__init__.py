class ParseTreeNode:
    def __init__(self, label):
        self.label = label
        self.children = []
        self.word = None

    def __str__(self):
        if self.word is not None:
            return f"({self.label} {self.word})"
        else:
            return f"({self.label} {' '.join(map(str, self.children))})"

    @classmethod
    def from_string(cls, s):
        tokens = cls._tokenize(s)
        tree, _ = cls._parse(tokens, 0)
        return tree

    @staticmethod
    def _tokenize(s):
        tokens = []
        curr = ''
        for char in s:
            if char.isspace():
                if curr:
                    tokens.append(curr)
                curr = ''
                continue
            if char in '()':
                if curr:
                    tokens.append(curr)
                curr = ''
                tokens.append(char)
            else:
                curr += char
        if curr:
            tokens.append(curr)
        return tokens

    @staticmethod
    def _parse(tokens, idx):
        assert tokens[idx] == '('
        idx += 1
        label = tokens[idx]
        idx += 1
        node = ParseTreeNode(label)
        if tokens[idx] == ')':
            idx += 1
            return node, idx
        if tokens[idx] != '(':
            node.word = tokens[idx]
            idx += 1
            assert tokens[idx] == ')'
            idx += 1
            return node, idx
        while tokens[idx] != ')':
            child, idx = ParseTreeNode._parse(tokens, idx)
            node.children.append(child)
        idx += 1
        return node, idx

    def get_nodes_by_label(self, label, skip_self=False):
        res = []
        if not skip_self and self.label == label:
            res.append(self)
        for child in self.children:
            res.extend(child.get_nodes_by_label(label, skip_self=False))
        return res

    def get_words_by_tags(self, tags):
        res = []
        if self.word is not None:
            if self.label in tags:
                res.append(self.word)
        else:
            for child in self.children:
                res.extend(child.get_words_by_tags(tags))
        return res

    def leaves(self):
        """Return a list of all leaf words in the tree."""
        if self.word is not None:
            return [self.word]
        res = []
        for child in self.children:
            res.extend(child.leaves())
        return res
