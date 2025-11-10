# Inventory system with Multi-Warehouse and Audit APIs

# Design an inventory system that tracks items across multiple warehouses, supports real-time stock
# updates and deletions, and provides global and analytical queries — but without relying on time windows.

# Implement the following APIs:

# add_stock(item_id: int, quantity: int, warehouse_id: int, timestamp: int) -> None
#   """
#   Add quantity of an item to the given warehouse.
#   Each addition is recorded in an audit log.
#   """

# remove_stock(item_id: int, quantity: int, warehouse_id: int, timestamp: int) -> bool
#   """
#   Remove quantity from the given warehouse.
#   If stock goes below 0, rollback and return False.
#   Otherwise, record the operation in the audit log.
#   """

# transfer_stock(item_id: int, from_warehouse: int, to_warehouse: int, quantity: int, timestamp: int) -> bool
#   """
#   Move stock between warehouses atomically.
#   If insufficient stock at the source, rollback and return False.
#   """

# get_global_stock(item_id: int) -> int
#   """
#   Return total stock of an item across all warehouses.
#   """

# get_warehouse_stock(item_id: int, warehouse_id: int) -> int
#   """
#   Return stock for a specific item in a specific warehouse.
#   """

# get_most_transferred_items(k: int) -> list[int]
#   """
#   Return the top-k items that have been transferred between warehouses the most.
#   """

# get_most_active_warehouses(k: int) -> list[int]
#   """
#   Return top-k warehouses ranked by total stock movement (adds + removes + transfers).
#   """

# get_stock_distribution(item_id: int) -> dict[int, int]
#   """
#   Return {warehouse_id: quantity} for a given item.
#   """

# get_audit_log(item_id: int, limit: int) -> list[tuple]
#   """
#   Return the most recent operations for that item:
#   Each entry = (timestamp, operation, warehouse_id(s), quantity)
#   """

# Constraints
# - Each operation is thread-safe and idempotent per (item_id, warehouse_id, timestamp).
# - Max warehouses = 10^4
# - Max items = 10^6
# - Max stock per item per warehouse = 10^9

# Audit logs should be bounded (e.g., keep last 1000 ops per item).
import heapq
from collections import OrderedDict, defaultdict, deque
from enum import Enum
from threading import Lock, RLock


class InventoryStockOperation(Enum):
    ADD = 1
    DEL = 2
    TFR = 3

ItemOp = tuple[int, InventoryStockOperation, tuple[int, ...], int]

class InventorySystem:
    def __init__(self,
        ops_max_len:int = 1000,
        num_locks:int = 128,
        max_requests: int = 100000,
    ):

        # item level
        self.item_warehouse_stocks: dict[int, dict[int, int]] = {}     # item id -> warehouse id -> count
        self.item_transfers = defaultdict(int)            # item id -> count
        self.item_latest_ops: dict[int, deque[ItemOp]] = {}           # item id -> [(timestamp, operation, warehouse_id(s), quantity)]

        # warehouse level
        self.warehouse_stock_movement = defaultdict(int)  # warehouse id -> stock movement count

        self.ops_max_len = ops_max_len
        self.num_locks = num_locks
        self.max_requests_per_cache = max_requests // num_locks
        self.request_cache = [OrderedDict() for _ in range(num_locks)]

        self.analytics_lock = Lock()
        self.request_locks = [Lock() for _ in range(num_locks)]
        self.item_locks = [Lock() for _ in range(num_locks)]
        self.item_warehouse_locks = [RLock() for _ in range(num_locks)]

    def _get_item_lock(self, item_id: int):
        lock_index = hash(item_id) % self.num_locks
        return self.item_locks[lock_index]

    def _get_item_warehouse_lock(self, item_id: int, warehouse_id: int):
        lock_index = hash((item_id, warehouse_id)) % self.num_locks
        return self.item_warehouse_locks[lock_index]

    def _add_item_audit_op(self,
        item_id: int,
        quantity: int,
        timestamp: int,
        op: InventoryStockOperation,
        *warehouse_ids: int,
    ):
        with self._get_item_lock(item_id):
            item_latest_ops = self.item_latest_ops.setdefault(item_id, deque())
            item_latest_ops.append(
                (timestamp, op, warehouse_ids, quantity)
            )
            while len(item_latest_ops) > self.ops_max_len:
                item_latest_ops.popleft()

    def _dedup_request(self, op: InventoryStockOperation, item_id: int,  timestamp:int, *warehouse_ids: int) -> bool:
        key = (op, item_id, timestamp, warehouse_ids)
        lock_index = hash(key) % self.num_locks

        with self.request_locks[lock_index]:
            cache = self.request_cache[lock_index]
            if key in self.request_cache[lock_index]:
                cache.move_to_end(key)
                return True

            cache[key] = True
            if len(cache) > self.max_requests_per_cache:
                cache.popitem(last=False)

            return False

    def _add_warehouse_stock_movement(self,
        warehouse_id: int,
        quantity: int,
    ):
        with self.analytics_lock:
            self.warehouse_stock_movement[warehouse_id] += quantity

    def _add_stock(self, item_id: int, quantity: int, warehouse_id: int):
        """
        assumes that caller holds _get_item_warehouse_lock(item_id, warehouse_id)
        """
        with self._get_item_lock(item_id):
            item_warehouse_stocks = self.item_warehouse_stocks.setdefault(item_id, {})
            item_warehouse_stocks[warehouse_id] = item_warehouse_stocks.get(warehouse_id, 0) + quantity

    def add_stock(self, item_id: int, quantity: int, warehouse_id: int, timestamp: int) -> bool:
        """
        Add quantity of an item to the given warehouse.
        Each addition is recorded in an audit log.
        """
        op = InventoryStockOperation.ADD
        if self._dedup_request(op, item_id, timestamp, warehouse_id):
            return True

        with self._get_item_warehouse_lock(item_id, warehouse_id):
            self._add_stock(item_id, quantity, warehouse_id)

        self._add_warehouse_stock_movement(warehouse_id, quantity)
        self._add_item_audit_op(item_id, quantity, timestamp, op, warehouse_id)
        return True

    def _remove_stock(self, item_id: int, quantity: int, warehouse_id: int):
        """
        assumes that caller holds _get_item_warehouse_lock(item_id, warehouse_id)
        """
        with self._get_item_lock(item_id):
            if item_id not in self.item_warehouse_stocks:
                return False

            stock = self.item_warehouse_stocks[item_id].get(warehouse_id, 0)
            if stock < quantity:
                return False

            new_stock = stock - quantity
            if new_stock == 0:
                del self.item_warehouse_stocks[item_id][warehouse_id]
                if not self.item_warehouse_stocks[item_id]:
                    del self.item_warehouse_stocks[item_id]
            else:
                self.item_warehouse_stocks[item_id][warehouse_id] = new_stock

            return True

    def remove_stock(self, item_id: int, quantity: int, warehouse_id: int, timestamp: int) -> bool:
        """
        Remove quantity from the given warehouse.
        If stock goes below 0, rollback and return False.
        Otherwise, record the operation in the audit log.
        """
        op = InventoryStockOperation.DEL
        if self._dedup_request(op, item_id, timestamp, warehouse_id):
            return True

        with self._get_item_warehouse_lock(item_id, warehouse_id):
            if not self._remove_stock(item_id, quantity, warehouse_id):
                return False

        self._add_item_audit_op(item_id, quantity, timestamp, op, warehouse_id)
        self._add_warehouse_stock_movement(warehouse_id, quantity)
        return True

    def _transfer_stock(self, item_id: int, from_warehouse: int, to_warehouse: int, quantity: int):
        if not self._remove_stock(item_id, quantity, from_warehouse):
            return False

        self._add_stock(item_id, quantity, to_warehouse)
        # Increment without lock - minor race acceptable for analytics
        with self.analytics_lock:
            self.item_transfers[item_id] += quantity
        return True

    def transfer_stock(self, item_id: int, from_warehouse: int, to_warehouse: int, quantity: int, timestamp: int) -> bool:
        """
        Move stock between warehouses atomically.
        If insufficient stock at the source, rollback and return False.
        """
        op = InventoryStockOperation.TFR
        if self._dedup_request(op, item_id, timestamp, from_warehouse, to_warehouse):
            return True

        if from_warehouse == to_warehouse:
            return False

        lock1 = self._get_item_warehouse_lock(item_id, from_warehouse)
        lock2 = self._get_item_warehouse_lock(item_id, to_warehouse)
        locks = sorted([lock1, lock2], key=id)
        success = False
        if lock1 is lock2:
            with lock1:
                success = self._transfer_stock(item_id, from_warehouse, to_warehouse, quantity)
        else:
            with locks[0], locks[1]:
                success = self._transfer_stock(item_id, from_warehouse, to_warehouse, quantity)
        if not success:
            return False
        self._add_item_audit_op(item_id, quantity, timestamp, InventoryStockOperation.TFR, from_warehouse, to_warehouse)
        self._add_warehouse_stock_movement(from_warehouse, quantity)
        self._add_warehouse_stock_movement(to_warehouse, quantity)
        return True

    def get_global_stock(self, item_id: int) -> int: # O(I_w)
        """
        Return total stock of an item across all warehouses.

        Note: This query may show briefly inconsistent values during
        concurrent transfers, as transfers use item_warehouse_locks,
        and it is inefficient to acquire all item_warehouse_locks,
        for this analytics use case.
        This is acceptable for analytics/reporting use cases.
        """
        with self._get_item_lock(item_id):
            item_warehouse_stocks = list(self.item_warehouse_stocks.get(item_id, {}).values())
        return sum(item_warehouse_stocks)

    def get_warehouse_stock(self, item_id: int, warehouse_id: int) -> int:
        """
        Return stock for a specific item in a specific warehouse.
        """
        with self._get_item_warehouse_lock(item_id, warehouse_id):
            return self.item_warehouse_stocks.get(item_id, {}).get(warehouse_id, 0)

    def get_most_transferred_items(self, k: int) -> list[int]:
        """
        Return the top-k items that have been transferred between warehouses the most.
        """
        with self.analytics_lock:
            snapshot = dict(self.item_transfers)
        return heapq.nlargest(
            k,
            snapshot.keys(),
            key=lambda item_id: snapshot[item_id],
        )

    def get_most_active_warehouses(self, k: int) -> list[int]:
        """
        Return top-k warehouses ranked by total stock movement (adds + removes + transfers).
        """
        with self.analytics_lock:
            snapshot = dict(self.warehouse_stock_movement)
        return heapq.nlargest(
            k,
            snapshot.keys(),
            key=lambda warehouse_id: snapshot[warehouse_id],
        )

    def get_stock_distribution(self, item_id: int) -> dict[int, int]:
        """
        Return {warehouse_id: quantity} for a given item.
        """
        with self._get_item_lock(item_id):
            return dict(self.item_warehouse_stocks.get(item_id, {}))
        
    def get_audit_log(self, item_id: int, limit: int) -> list[ItemOp]:
        """
        Return the most recent operations for that item:
        Each entry = (timestamp, operation, warehouse_id(s), quantity)
        """
        with self._get_item_lock(item_id):
            item_latest_ops = list(self.item_latest_ops.get(item_id, []))
        return item_latest_ops[-limit:]
    