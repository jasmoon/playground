# Number of Islands
# Given a 2D grid of 1s (land) and 0s (water), count the number of islands.
# (Tests: DFS/BFS traversal, visited set)
def islands(grid: list[list[str]]):
    seen: set[tuple[int, int]] = set()
    rows = len(grid)
    cols = len(grid[0])
    moves: list[tuple[int, int]] = [(0, 1), (0, -1), (1, 0), (-1, 0)]
    ans = 0
    def dfs(row: int, col: int) -> None:
        seen.add((row, col))
        for drow, dcol in moves:
            nrow, ncol = row + drow, col + dcol
            if (
                nrow < 0 or nrow >= rows or
                ncol < 0 or ncol >= cols or
                (nrow, ncol) in seen or
                grid[nrow][ncol] == "0"
            ):
                continue
            dfs(nrow, ncol)


    for row in range(rows):
        for col in range(cols):
            if grid[row][col] == "1" and (row, col) not in seen:
                ans += 1
                dfs(row, col)
    return ans
                

from collections import deque
# Flood Fill
# Given an image matrix and a starting pixel, change its color and spread to connected neighbors.
# (Tests: Graph traversal in grids)
def flood_fill(grid: list[list[int]], source: tuple[int, int], new_color: int) -> list[list[int]]:
    queue = deque([source])
    rows = len(grid)
    cols = len(grid[0])
    moves: list[tuple[int, int]] = [(0, 1), (0, -1), (1, 0), (-1, 0)]
    grid[source[0]][source[1]] = new_color

    while queue:
        row, col = queue.popleft()
        for drow, dcol in moves:
            nrow, ncol = row + drow, col + dcol
            if (
                nrow < 0 or nrow >= rows or
                ncol < 0 or ncol >= cols or
                grid[nrow][ncol] == 0 or
                grid[nrow][ncol] == new_color
            ):
                continue
            grid[nrow][ncol] = new_color
            queue.append((nrow, ncol))

    return grid

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
            return True # cycle detected

        if self.rank[px] < self.rank[py]:
            self.parent[px] = py
        elif self.rank[py] < self.rank[px]:
            self.parent[py] = px
        else:
            # promote px to parent of py
            self.parent[py] = px
            self.rank[px] += 1
        return False

# Question
# Given an undirected graph as adjacency list, check if it contains a cycle.
def detect_cycle_undirected_graph(edges: list[list[int]], n: int):
    ufds = UnionFind(n)
    for u, v in edges:
        if ufds.union(u, v):
            return True
    return False

def test_simple():
    island_tcs = [
        (
            [
                ["1", "1", "0", "0", "0"],
                ["1", "1", "0", "0", "0"],
                ["0", "0", "1", "0", "0"],
                ["0", "0", "0", "1", "1"],
            ],
            3,
        )
    ]
    for grid, expected in island_tcs:
        output = islands(grid)
        print(output)
        assert expected == output

    flood_fill_tcs = [
        (
            [
                [1,1,1],
                [1,1,0],
                [1,0,1]
            ],
            (1, 1),
            2,
            [
                [2,2,2],
                [2,2,0],
                [2,0,1]
            ]
        )
    ]
    for grid, source, new_color, expected in flood_fill_tcs:
        output = flood_fill(grid, source, new_color)
        print(output)
        assert expected == output

    detect_cycle_tcs = [
        (
            [
                [0,1], [1,2], [2,3], [3,4]   # just a line
            ],
            5,
            False,
        ),
        (
            [
                [0,1], [1,2], [2,0], [3,4]   # triangle 0-1-2 forms a cycle
            ],
            5,
            True,
        )
    ]
    for grid, n, expected in detect_cycle_tcs:
        output = detect_cycle_undirected_graph(grid, n)
        print(output)
        assert expected == output


if __name__ == "__main__":
    test_simple()
