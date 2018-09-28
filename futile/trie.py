import string
from typing import Any


class TrieNode:

    def __init__(self, data: Any = None):
        self.children = {}
        self.data = data


class Trie:
    """
    >>> trie = Trie()
    >>> trie.add('www.google.com', 'foo')
    >>> trie.longest_match('www.google.com/index.html')
    'foo'
    >>> print(trie.longest_match('www.facebook.com'))
    None
    """

    def __init__(self):
        self._root = TrieNode()

    def add(self, word: str, data: Any):
        node = self._root
        for char in word:
            if char in node.children:
                node = node.children[char]
            else:
                new_node = TrieNode()
                node.children[char] = new_node
                node = new_node
        node.data = data

    def longest_match(self, word):
        node = self._root
        for char in word:
            if char in node.children:
                node = node.children[char]
            else:
                break
        return node.data


if __name__ == '__main__':
    import doctest
    doctest.testmod()
