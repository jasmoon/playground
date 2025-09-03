






from pprint import pprint
from typing import Any, Generator


from dataclasses import dataclass
from collections.abc import Iterator
import heapq

@dataclass
class Record:
    id: int
    content: str



class MasterNode:
    def mergeDataSources(self, dataSources: list[list[Record]]) -> list[Record]:
        n = len(dataSources)
        minHeap: list[tuple[int, Record, int]] = []
        its = [self.poll(dataSource) for dataSource in dataSources]
        res: list[Record] = []
        
        for i in range(n):
            try:
                record = next(its[i])
                heapq.heappush(minHeap, (record.id, record, i))
            except StopIteration:
                pass
        
        prevYielded = None
        
        while minHeap:
            recordId, record, sourceId = heapq.heappop(minHeap)
            if prevYielded is None or recordId != prevYielded:
                res.append(record)
                prevYielded = recordId

            try:
                nextRecord = next(its[sourceId])
                heapq.heappush(minHeap, (nextRecord.id, nextRecord, sourceId))
            except StopIteration:
                pass
        return res


    def poll(self, dataSource: list[Record]) -> Iterator[Record]:
        yield from dataSource


def test_simple():
    dataSourceA = [
        Record(id=1, content="content 1"),
        Record(id=3, content="content 3"),
        Record(id=4, content="content 4")

    ]

    dataSourceB = [
        Record(id=3, content="content 3"),
        Record(id=5, content="content 5")
    ]

    dataSourceC = [
        Record(id=4, content="content 4"),
        Record(id=8, content="content 8")
    ]

    masterNode = MasterNode()
    pprint(masterNode.mergeDataSources([
        dataSourceA, dataSourceB, dataSourceC
    ]))


class PagedSource:
    def __init__(self, data: list[Record], batchSize: int) -> None:
        self.data: list[Record] = data
        self.offset: int = 0
        self.batchSize: int = batchSize

    def fetch(self) -> Generator[Record, None, None]:
        if self.offset >= len(self.data):
            return
        yield from self.data[self.offset:self.offset + self.batchSize]
        self.offset += self.batchSize


def mergeDataSources(dataSources: list[PagedSource]):
    iters: list[Generator[Record, None, None]] = [dataSource.fetch() for dataSource in dataSources]
    minHeap: list[tuple[int, Record, int]] = []
    for i in range(len(dataSources)):
        try:
            record: Record = next(iters[i])
            heapq.heappush(minHeap, (record.id, record, i))
        except StopIteration:
            pass
    
    prev = None
    res: list[Record] = []
    while minHeap:
        id, record, sourceId = heapq.heappop(minHeap)
        if prev is None or id != prev:
            res.append(record)
            prev = id

        try:
            nextRecord = next(iters[sourceId])
            heapq.heappush(minHeap, (nextRecord.id, nextRecord, sourceId))
        except StopIteration:
            pass
    return res

def test_paged_source():
    pagedSourceA = PagedSource([
        Record(id=1, content="content 1"),
        Record(id=3, content="content 3"),
        Record(id=4, content="content 4")
    ], 2)

    pagedSourceB  = PagedSource([
        Record(id=3, content="content 3"),
        Record(id=5, content="content 5")
    ], 2)

    pagedSourceC = PagedSource([
        Record(id=4, content="content 4"),
        Record(id=8, content="content 8")
    ], 2)

    pprint(mergeDataSources([
        pagedSourceA, pagedSourceB, pagedSourceC
    ]))

if __name__ == "__main__":
    test_simple()
    test_paged_source()
