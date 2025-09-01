






from pprint import pprint
from typing import Any


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

if __name__ == "__main__":
    test_simple()