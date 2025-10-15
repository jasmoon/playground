from typing import Any


from collections import deque, defaultdict
import heapq

# Shortest Path in Binary Matrix
# Given an n x n binary matrix, return the shortest path from top-left to bottom-right, moving in 8 directions.
def shortest_path(grid: list[list[int]])-> int:
    """
    runtime: O(m * n)
    """
    rows, cols = len(grid), len(grid[0])
    queue = deque([(0, 0)])
    moves = [(0, 1), (0, -1), (1, 0), (-1, 0), (1, 1), (1, -1), (-1, -1), (-1, 1)]
    steps = 1

    while queue:
        cur_len = len(queue)

        for _ in range(cur_len):
            row, col = queue.popleft()

            for drow, dcol in moves:
                nrow, ncol = row + drow, col + dcol
                if nrow < 0 or nrow >= rows or ncol < 0 or ncol >= cols or grid[nrow][ncol] == 1:
                    continue
                if nrow == rows - 1 and ncol == cols - 1:
                    return steps + 1

                grid[nrow][ncol] = 1
                queue.append((nrow, ncol))
        steps += 1

    return -1

# Question
# Given the weighted edges and the number of nodes in a graph, return the mst and weight of the mst
def mst_prim(edges: list[tuple[int, int, int]], n: int) -> tuple[None, None] | tuple[list[tuple[int, int, int]], int]:
    """
    runtime: O((V + E) log n)
    """
    graph: defaultdict[int, list[tuple[int, int]]] = defaultdict(list)
    for u, v, cost in edges:
        graph[u].append((v, cost))
        graph[v].append((u, cost))

    visited = visited = [False] * n
    mst: list[tuple[int, int, int]] = []
    total_cost = 0
    min_heap = [(0, 0, -1)] # weight, node, parent

    while min_heap:
        weight, node, parent = heapq.heappop(min_heap)
        if visited[node]:
            continue

        visited[node] = True
        total_cost += weight
        if parent != -1:
            mst.append((parent, node, weight))

        for nbr, cost in graph[node]:
            if visited[nbr]:
                continue

            heapq.heappush(min_heap, (cost, nbr, node))

    if not all(visited):
        return None, None
    return mst, total_cost


class UnionFind:
    def __init__(self, n: int) -> None:
        self.parent = list(range(n)) # set every node as parent of itself
        self.rank = [0] * n

    def find(self, x: int):
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x: int, y: int):
        px, py = self.find(x), self.find(y)
        if px == py:
            return False

        if self.rank[px] < self.rank[py]:
            self.parent[px] = py
        elif self.rank[py] < self.rank[px]:
            self.parent[py] = px
        else:
            # promote px to parent of py
            self.parent[py] = px
            self.rank[px] += 1
        return True

def mst_kruskal(edges: list[tuple[int, int, int]], n: int) -> tuple[list[tuple[int, int, int]], int]:
    """
    runtime: O(E log E)
    """
    edges.sort(key=lambda edge: edge[2])
    ufds = UnionFind(n)
    total = 0
    mst: list[tuple[int, int, int]] = []
    print(edges)
    for u, v, weight in edges:
        if ufds.union(u, v):
            total += weight
            mst.append((u, v, weight))

        if len(mst) == n-1:
            break
    return mst, total

def test_simple():
    shortest_path_tcs = [
        (
            [
                [0, 1],
                [1, 0]
            ],
            2,
        ),
        (
            [
                [0,0,0],
                [1,1,0],
                [1,1,0]
            ],
            4
        ),
        (
            [
                [0,1,1],
                [1,1,0],
                [1,1,0]
            ],
            -1
        )
    ]
    for grid, expected in shortest_path_tcs:
        output = shortest_path(grid)
        assert expected == output

    mst_tcs = [
        (
            [
                (0, 1, 10),
                (0, 2, 6),
                (0, 3, 5),
                (1, 3, 15),
                (2, 3, 4),
            ],
            4,
            [(0, 1, 10), (0, 3, 5), (3, 2, 4)],
            [(0, 1 ,10), (0, 3, 5), (2, 3, 4)],
            19
        ),
        (
            [
                (0, 1, 1),
                (0, 2, 2),
                (0, 3, 3),
                (1, 2, 4),
                (1, 3, 5),
                (2, 3, 6),
                # tons of extra heavy edges
                (0, 1, 1000),
                (2, 3, 2000),
                (1, 2, 3000),
                (0, 3, 4000)
            ],
            4,
            [(0, 1, 1), (0, 2, 2), (0, 3, 3)],
            [(0, 1, 1), (0, 2, 2), (0, 3, 3)],
            6
        ),
    ]

    for edges, n, expected_edges_prim, expected_edges_kruskal, expected_cost in mst_tcs:
        output_edges, output_cost = mst_prim(edges, n)
        if output_edges:
            assert sorted(expected_edges_prim) == sorted(output_edges)
        assert expected_cost == output_cost

        output_edges, output_cost = mst_kruskal(edges, n)
        print(output_edges)
        assert sorted(expected_edges_kruskal) == sorted(output_edges)
        assert expected_cost == output_cost

if __name__ == "__main__":
    test_simple()
