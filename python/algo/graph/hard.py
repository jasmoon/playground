from collections import defaultdict, deque

# Strongly Connected Components
# Given a directed graph, find all strongly connected components.
# SCC: In a directed graph, a strongly connected component is a maximal set of vertices where every vertex is reachable
# from every other vertex in the set.
def scc_kosaraju(edges: list[tuple[int, int]], n: int) -> list[list[int]]:
    """
    runtime: O(2V + 3E) = O(V + E)
    """
    graph: defaultdict[int, list[int]] = defaultdict(list)
    for u, v in edges:
        graph[u].append(v)

    stack: list[int] = []
    visited = [False] * n

    def dfs1(node: int): # O(V + E)
        visited[node] = True

        for nbr in graph[node]:
            if not visited[nbr]:
                dfs1(nbr)
        stack.append(node)

    for i in range(n):
        if not visited[i]:
            dfs1(i)

    # Transpose the graph (reverse edges) # O(E)
    transpose: defaultdict[int, list[int]] = defaultdict(list)
    for u, v in edges:
        transpose[v].append(u)

    visited = [False] * n
    sccs: list[list[int]] = []

    def dfs2(node: int, component: list[int]): # O(V + E)
        visited[node] = True
        component.append(node)
        for nbr in transpose[node]:
            if not visited[nbr]:
                dfs2(nbr, component)

    while stack:
        node = stack.pop()
        if not visited[node]:
            component: list[int] = []
            dfs2(node, component)
            sccs.append(component)
    return sccs

# Alien Dictionary (LeetCode 269)
#
# There is a new alien language which uses the same lowercase English letters, but the order among letters is unknown to you.
# You receive a list of non-empty words from the alien language’s dictionary, sorted lexicographically by the rules of this alien language.
# Derive the order of letters in this language.
# If the given order of words is invalid, return "".
# If multiple valid orders are possible, return any valid one.

# Notes / constraints:

# - If a word is a prefix of the previous word and comes later (e.g. "abc" then "ab"), that’s invalid → return "".
# - You only need to output letters that appear in the words list.
# - The answer must respect all ordering constraints derived from adjacent word comparisons.

# Example 1:

# Input: ["wrt","wrf","er","ett","rftt"]
# Output: "wertf"

# Explanation:
# - From "wrt" vs "wrf", you infer t < f.
# - From "wrf" vs "er", you infer w < e.
# - From "er" vs "ett", you infer r < t.
# - From "ett" vs "rftt", you infer e < r.
# One possible valid order that satisfies these is "wertf".
# (This is the standard example.)

# Example 2:

# Input: ["z","x"]
# Output: "zx"

# Explanation:
# - Only two letters, z and x, and z appears before x → order is "zx".

# Example 3:

# Input: ["z","x","z"]
# Output: ""

# Explanation:
# - From "z" vs "x" → infer z < x.
# - From "x" vs "z" → infer x < z.
# That’s a contradiction (cycle), no valid order → return "".

def alien_dictionary(words: list[str]) -> str:
    # calculate indegree and track edges
    # try to create topological order
    in_degrees = defaultdict(int)
    graph: defaultdict[str, set[str]] = defaultdict(set) # maybe list is required here
    chars = set("".join(words))
    
    for i in range(len(words)-1):
        w1, w2 = words[i], words[i + 1]
        if len(w1) > len(w2) and w1.startswith(w2):
            return ""

        # Find first differing character
        for j in range(min(len(w1), len(w2))):
            if w1[j] != w2[j]:
                if w2[j] not in graph[w1[j]]:
                    graph[w1[j]].add(w2[j])
                    in_degrees[w2[j]] += 1
                break

    queue = deque([char for char in chars if char not in in_degrees])
    order = []

    while queue:
        node = queue.popleft()
        order.append(node)
        for nbr in graph[node]:
            in_degrees[nbr] -= 1
            if in_degrees[nbr] == 0:
                queue.append(nbr)

    return "" if len(order) < len(chars) else "".join(order)

def test_simple():
    scc_tcs = [
        (
            [
                (0, 1), (1, 2), (2, 0), (2, 3),
                (3, 4), (4, 5), (5, 3),
                (6, 5), (6, 7), (7, 6)
            ],
            8,
            [[6, 7], [3, 5, 4], [0, 2, 1]]
        )
    ]
    for edges, n, expected in scc_tcs:
        output = scc_kosaraju(edges, n)
        assert sorted(expected) == sorted(output)

    alien_dictionary_tccs = [
        (
            ["wrt","wrf","er","ett","rftt"],
            "wertf"
        ),
        (
            ["z","x"],
            "zx"
        ),
        (
            ["z","x","z"],
            ""
        )
    ]
    for words, expected in alien_dictionary_tccs:
        output = alien_dictionary(words)
        print(f"output is {output}")
        assert expected == output

if __name__ == "__main__":
    test_simple()