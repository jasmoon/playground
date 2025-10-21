import heapq
from datetime import datetime, timedelta

from pybloom_live import BloomFilter


def deduplicate(transactions):
    seen = BloomFilter(capacity=10_000_000, error_rate=0.001)
    result = []

    for transaction in transactions:
        transactionId = transaction["transaction_id"]
        if transactionId not in seen:
            seen.add(transactionId)
            result.append(transaction)

    return result


transactions1: list[dict[str, str | int]] = [
    {"transaction_id": "tx1", "amount": 100, "timestamp": "2025-08-28T10:00:00"},
    {"transaction_id": "tx2", "amount": 50, "timestamp": "2025-08-28T10:01:00"},
    {"transaction_id": "tx1", "amount": 100, "timestamp": "2025-08-28T10:02:00"},
]

tcs = [
    (
        transactions1,
        [
            {
                "transaction_id": "tx1",
                "amount": 100,
                "timestamp": "2025-08-28T10:00:00",
            },
            {"transaction_id": "tx2", "amount": 50, "timestamp": "2025-08-28T10:01:00"},
        ],
    )
]

if __name__ == "__main__":
    for tc in tcs:
        expected = tc[1]
        output = deduplicate(tc[0])
        print(output)
        assert expected == output


class WindowDeduplicator:
    def __init__(self, ttl=timedelta(hours=24)) -> None:
        self.storage = {}
        self.ttlMinHeap = []
        self.ttl = ttl

    def process(self, transaction):
        tid = transaction["transaction_id"]
        ts = datetime.fromisoformat(transaction["timestamp"])
        while self.ttlMinHeap and self.ttlMinHeap[0][0] < ts:
            _, oldTid = heapq.heappop(self.ttlMinHeap)
            if oldTid in self.storage:
                del self.storage[oldTid]

        if tid in self.storage:
            return False
        expiryTime = ts + self.ttl
        heapq.heappush(self.ttlMinHeap, (expiryTime, tid))
        self.storage[tid] = transaction
        return True

    def activeTransactions(self):
        return list(self.storage.values())


transactions2 = [
    {"transaction_id": "tx1", "amount": 100, "timestamp": "2025-08-28T10:00:00"},
    {
        "transaction_id": "tx2",
        "amount": 50,
        "timestamp": "2025-08-28T09:00:00",
    },  # earlier timestamp
    {
        "transaction_id": "tx1",
        "amount": 100,
        "timestamp": "2025-08-28T10:05:00",
    },  # duplicate
]

dedup = WindowDeduplicator()
for transaction in transactions2:
    dedup.process(transaction)
print(dedup.activeTransactions())
