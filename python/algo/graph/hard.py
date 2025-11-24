from collections import defaultdict, deque

# Shortest Path with One Discount Coupon

# You are given a directed weighted graph with n nodes and m edges.
# Each edge (u, v, w) means there is a directed edge from node u to node v with cost w, where w > 0.
# You start from node 1 and want to reach node n.

# You have one discount coupon that can be used at most once along your path.
# When you use it on an edge with cost w, that edge’s cost becomes ⌊w / 2⌋.
# You may also choose not to use the coupon.

# Return the minimum possible total cost from node 1 to node n.
# If no path exists, return -1.

# Input

# First line: two integers n m

# Next m lines: u v w
# (a directed edge from u to v with cost w)

# There are no self-loops, but multiple edges between the same pair may exist.

# Output

# A single integer — the minimum total cost from 1 to n.

# Constraints

# 1 ≤ n ≤ 2 * 10^5

# 1 ≤ m ≤ 5 * 10^5

# 1 ≤ w ≤ 10^9

# Example
# Input:
# 5 6
# 1 2 10
# 2 5 10
# 1 3 100
# 3 4 1
# 4 5 1
# 2 3 1

# Output:
# 8


# Explanation
# One optimal path is 1 → 2 → 3 → 4 → 5 with total original cost 10 + 1 + 1 + 1 = 13.
# Using the coupon on the edge (1, 2) reduces its cost to 5,
# so total cost becomes 5 + 1 + 1 + 1 = 8.
# No other path yields a smaller result.

# n nodes
# m edges
# positive weights
# start from node 1
# costs to every node = [undiscounted, discounted]
# dikstraja
# 
# Deal with discount
# iterate each edge to use discount: O(E * (E log V))
# 
# boolean array
# state = use coupon/ not use
# dp to deal with the discount?
from collections import defaultdict
import heapq


def discounted_path(n: int, edges: list[list[int]]):
    """
    best, average, worst: O((V + E) log V)
    """
    graph: defaultdict[int, list[tuple[int, int]]] = defaultdict(list)

    for u, v, w in edges: # O(E)
        graph[u].append((v, w))
        graph[v].append((u, w))

    def dikstraja(start_node: int): # O((E+V) * log V))
        costs = [float("inf")] * n
        costs[start_node] = 0
        min_heap: list[tuple[int, int]] = [(0, start_node)]

        while min_heap:
            cur_node, cost = heapq.heappop(min_heap)
            for nbr, add_cost in graph[cur_node]:
                total_cost = cost + add_cost
                if total_cost < costs[nbr]:
                    costs[nbr] = total_cost
                    heapq.heappush(min_heap, (total_cost, nbr))
        return costs

    costs_from_source = dikstraja(0)
    costs_from_dest = dikstraja(n-1)

    lowest_cost = float("inf")
    for u, v, w in edges: # O(E)
        total_cost = costs_from_source[u] + costs_from_dest[v] + w / 2
        lowest_cost = min(total_cost, lowest_cost)
    return lowest_cost

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
    """
    best, average, worst case: O(V + E)
    """
    # calculate indegree and track edges
    # try to create topological order
    in_degrees = defaultdict(int)
    graph: defaultdict[str, set[str]] = defaultdict(set) # maybe list is required here
    chars = set("".join(words))
    
    for i in range(len(words)-1):
        w1, w2 = words[i], words[i + 1]
        # If a word is a prefix of the previous word and comes later (e.g. "abc" then "ab"), that’s invalid → return ""
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
    print(f"queue: {in_degrees}")

    while queue:
        node = queue.popleft()
        order.append(node)
        for nbr in graph[node]:
            in_degrees[nbr] -= 1
            if in_degrees[nbr] == 0: # for characters that are cyclic in the input, can never have their indegrees become 0
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
            ["z","x","z"], # `queue` before iteration will be empty in this test case
            ""
        ),
        (
            ["w", "z", "x", "z"],
            ""
        )
    ]
    for words, expected in alien_dictionary_tccs:
        output = alien_dictionary(words)
        print(f"output is {output}")
        assert expected == output

if __name__ == "__main__":
    test_simple()