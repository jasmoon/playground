import heapq

def climb_k_stairs(costs: list[int], k: int) -> int:
    minHeap: list[tuple[int, int]] = []
    n: int = len(costs)

    for i in range(n):
        while minHeap and i - minHeap[0][1] > k:
            heapq.heappop(minHeap)
        if minHeap and i > 1:
            costs[i] += minHeap[0][0]
        heapq.heappush(minHeap, (costs[i], i))
    return min(costs[-k:])

tcs = [
    ([1,100,1,1,1,100,1,1,100,1], 1, 306),
    ([1,100,1,1,1,100,1,1,100,1], 2, 6),
    ([1,100,1,1,1,100,1,1,100,1], 3, 4),
    ([10, 5, 15, 1, 2, 3, 4, 5, 10, 15], 4, 10),
    ([1, 2, 3], 2, 2),
    ([1, 2, 3], 5, 1),
]

if __name__ == "__main__":
    for tc in tcs:
        output = climb_k_stairs(tc[0], tc[1])
        assert tc[2] == output
