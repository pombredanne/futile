def hamming(h1: int, h2: int) -> int:
    """
    >>> hamming("00100100", "00000000")
    2
    >>> hamming("11111111", "00000000")
    8
    """
    h1, h2 = int(h1), int(h2)
    h, d = 0, h1 ^ h2
    while d:
        h += 1
        d &= d - 1
    return h


def ngram(s, n=2) -> set:
    gram_set = set()
    for i in range(len(s) - n + 1):
        gram_word = ''.join(s[i:i + n])
        gram_set.add(gram_word)
    return gram_set


def jaccard(s1, s2, n=2):
    n1 = ngram(s1)
    n2 = ngram(s2)
    return len(n1 & n2) / len(n1 | n2)


def lcs(s1, s2):
    """
    longest common substring
    """
    m = [[0] * (1 + len(s2)) for i in range(1 + len(s1))]
    longest, x_longest = 0, 0
    for x in range(1, 1 + len(s1)):
        for y in range(1, 1 + len(s2)):
            if s1[x - 1] == s2[y - 1]:
                m[x][y] = m[x - 1][y - 1] + 1
                if m[x][y] > longest:
                    longest = m[x][y]
                    x_longest = x
            else:
                m[x][y] = 0
    return s1[x_longest - longest: x_longest]
