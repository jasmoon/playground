from typing import Any, Iterator


import heapq

dataSource: list[int] = [0, 1, 2, 4, 4, 5, 7, 8]



def chunkAndDeduplicate(dataSource: list[int], chunkSize: int):
    source: Iterator[int] = iter(dataSource)
    minHeap = []
    res: list[int] = []
    prevNum = None
    while True:
        chunk: list[Any] = []
        try:
            for _ in range(chunkSize):
                num = next(source)
                heapq.heappush(minHeap, num)
        except StopIteration:
            pass

        if not minHeap:
            break
        
        while minHeap:
            num = heapq.heappop(minHeap)
            if num != prevNum:
                res.append(num)
                prevNum = num

    return res



print(chunkAndDeduplicate(dataSource, 3))
