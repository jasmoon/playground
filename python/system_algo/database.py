from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pprint import pprint
from typing import Any, Callable

@dataclass
class Row:
    id: str
    data: dict[str, Any]

class SimpleDatabase:
    def __init__(self) -> None:
        self.storage: dict[str, Row] = {}
        self.indexes: dict[str, defaultdict[str, list[str]]] = {} # col -> value -> row id

    def create(self, key: str, value: dict[str, Any]) -> None:
        if key in self.storage:
            raise ValueError(f"{key} already exist")
        row = Row(id=key, data=value)
        self.storage[key] = row
        self._update_indexes(row)

    def read(self, key: str) -> Row | None:
        return self.storage.get(key)

    def query_by_index(self, col: str, value: Any) -> list[Row]:
        if col not in self.indexes:
            raise ValueError(f"No index on col {col}")
        rowIds = self.indexes[col].get(value, [])
        return [self.storage[rid] for rid in rowIds]

    def update(self, key: str, value: dict[str, Any]) -> None:
        if key not in self.storage:
            raise KeyError(f"missing key: {key}")
        row = self.storage[key]
        self._remove_from_indexes(row)
        self.storage[key].data.update(value)
        self._update_indexes(row)

    def delete(self, key: str) -> None:
        row = self.storage.pop(key, None)
        if row:
            self._remove_from_indexes(row)

    def filter(self,
        eqFilters: dict[str, Any] | None = None,
        cols: list[str] | None = None,
        where: Callable[..., bool] | None = None,
        rangeFilters: dict[str, tuple[Any, Any]] | None = None, # and condition
        pageSize: int = 0,
        pageNumber: int = 1,
        orderBy: list[tuple[str, bool]] | None= None,
    ) -> list[Row]:
        candidates: set[str] = set()

        if eqFilters:
            for col, value in eqFilters.items():
                if col in self.indexes:
                    ids = set(self.indexes[col].get(value, []))
                    candidates = candidates & ids if candidates else ids
        rows = (
            [self.storage[rowId] for rowId in candidates]
            if candidates
            else list(self.storage.values())
        )

        if eqFilters:
            for col, val in eqFilters.items():
                if col not in self.indexes:
                    rows = [r for r in rows if r.data.get(col) == val]

        if rangeFilters:
            for col, (low, high) in rangeFilters.items():
                rows = [r for r in rows if low <= r.data.get(col)  <= high]

        if where:
            rows = [r for r in rows if where(r)]

        if len(rows) == 0:
            return rows

        if orderBy:
            for col, ascending in reversed(orderBy):  # sort last key first
                rows.sort(
                    key=lambda r: (col not in r.data, r.data.get(col)),
                    reverse=not ascending
                )

        if pageSize > 0:
            offset = (pageNumber - 1) * pageSize
            rows = rows[offset: offset + pageSize]

        if cols:
            rows = [
                Row(id=r.id, data={col: r.data[col] for col in cols if col in r.data})
                for r in rows
            ]

        return rows

    def create_index(self, col: str) -> None:
        index: defaultdict[Any, list[str]] = defaultdict(list)
        for row in self.storage.values():
            value = row.data.get(col)
            if value is not None:
                if row.id not in index[value]:
                    index[value].append(row.id)
        self.indexes[col] = index

    def _update_indexes(self, row: Row):
        for col, index in self.indexes.items():
            value = row.data.get(col)
            if value is not None and row.id not in index[value]:
                index[value].append(row.id)

    def _remove_from_indexes(self, row: Row) -> None:
        for col, index in self.indexes.items():
            value = row.data.get(col)
            if value is not None and value in index:
                if row.id in index[value]:
                    index[value].remove(row.id)
                if not index[value]:
                    del index[value]


def test_simple():
    db = SimpleDatabase()

    db.create("1", {"name": "Alice", "city": "SG", "amount": 100})
    db.create("2", {"name": "Bob", "city": "MY", "amount": 200})
    db.create("3", {"name": "Charlie", "city": "SG", "amount": 150})

    print("Bulk read where city is SG:")
    rows = db.filter(where=lambda row: row.data["city"] == "SG")
    for r in rows:
        print(r)

    # Indexing example
    db.create_index("city")
    print("\nQuery by index city='SG':")
    for r in db.query_by_index("city", "SG"):
        print(r)

def test_complex():
    db = SimpleDatabase()

    # Create 10 orders
    cities = ["SG", "MY", "TH"]
    statuses = ["pending", "completed", "cancelled"]

    for i in range(1, 11):
        db.create(
            str(i),
            {
                "user_id": f"user_{i%3 + 1}",
                "city": cities[i % len(cities)],
                "amount": 50 * i,
                "status": statuses[i % len(statuses)],
                "created_at": datetime(2025, 8, 31, 9, 0) + timedelta(minutes=5 * i)
            }
        )

    # Bulk read: all orders in SG
    sg_orders = db.filter(where=lambda row: row.data["city"] == "SG")
    print("Orders in SG:")
    pprint([r for r in sg_orders])

    # Bulk read: all pending orders above 200
    pending_big_orders = db.filter(where=lambda row: row.data["status"] == "pending" and row.data["amount"] > 200)
    print("\nPending orders with amount > 200:")
    pprint([r for r in pending_big_orders])

    # Update: mark order 3 as completed
    db.update("3", {"status": "completed"})
    print("\nAfter updating order 3 status:")
    row = db.read("3")
    if row:
        pprint(row.data)

    # Delete: remove order 5
    db.delete("5")
    print("\nAfter deleting order 5:")
    pprint([r for r in db.filter()])

    # Create an index on city and query
    db.create_index("city")
    print("\nQuery using index city='TH':")
    th_orders = db.query_by_index("city", "TH")
    pprint([r for r in th_orders])

def test_filter():
    db = SimpleDatabase()
    db.create_index("city")
    db.create_index("amount")

    db.create("1", {"city": "SG", "amount": 200, "type": "condo"})
    db.create("2", {"city": "SG", "amount": 600, "type": "condo"})
    db.create("3", {"city": "NY", "amount": 150, "type": "condo"})
    db.create("4", {"city": "SG", "amount": 400, "type": "condo"})

    # Indexed equality + range + sorting + pagination
    results = db.filter(
        eqFilters={"city": "SG"},
        rangeFilters={"amount": (100, 500)},
        orderBy=[("amount", True)],
        pageNumber=1,
        pageSize=2,
        cols=["city", "amount"],
    )

    for r in results:
        print(r)
if __name__ == "__main__":
    test_simple()
    test_complex()
    test_filter()


    