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
from typing import Any


from collections import deque
from enum import Enum

class InventoryStockOperation(Enum):
    ADD = 1
    DEL = 2
    TFR = 3

class InventorySystem:
    def __init__(self,
        ops_max_len:int = 1000,
    ):
        # item level
        self.item_warehouse_stocks: dict[int, dict[int, int]] = {}     # item id -> warehouse id -> count
        self.item_transfers = {}            # item id -> count
        self.item_latest_ops: dict[int, deque[tuple[int, InventoryStockOperation, tuple[int, ...], int]]] = {}           # item id -> [(timestamp, operation, warehouse_id(s), quantity)]

        # warehouse level
        self.warehouse_item_stocks: dict[int, dict[int, int]] = {}     # warehouse id -> item id -> count
        self.warehouse_stock_movement = {}  # warehouse id -> stock movement count

        self.ops_max_len = ops_max_len

    def _add_item_audit_op(self,
        item_id: int,
        quantity: int,
        timestamp: int,
        op: InventoryStockOperation,
        *warehouse_ids: int,
    ):
        item_latest_ops = self.item_latest_ops.setdefault(item_id, deque())
        item_latest_ops.append(
            (timestamp, op, tuple(warehouse_ids), quantity)
        )
        while len(item_latest_ops) > self.ops_max_len:
            item_latest_ops.popleft()

    def _add_warehouse_stock_movement(self,
        warehouse_id: int,
        quantity: int,
    ):
        if warehouse_id not in self.warehouse_stock_movement:
            self.warehouse_stock_movement[warehouse_id] = 0
        self.warehouse_stock_movement[warehouse_id] += quantity


    def add_stock(self, item_id: int, quantity: int, warehouse_id: int, timestamp: int) -> None
        """
        Add quantity of an item to the given warehouse.
        Each addition is recorded in an audit log.
        """
        item_warehouse_stocks = self.item_warehouse_stocks.setdefault(item_id, {})
        if warehouse_id not in item_warehouse_stocks:
            item_warehouse_stocks[warehouse_id] = 0
        item_warehouse_stocks[warehouse_id] += quantity

        self._add_item_audit_op(item_id, quantity, timestamp, InventoryStockOperation.ADD, warehouse_id)

        warehouse_item_stocks = self.warehouse_item_stocks.setdefault(warehouse_id, {})
        if item_id not in warehouse_item_stocks:
            warehouse_item_stocks[item_id] = 0
        warehouse_item_stocks[item_id] += quantity

        self._add_warehouse_stock_movement(warehouse_id, quantity)

    def remove_stock(self, item_id: int, quantity: int, warehouse_id: int, timestamp: int) -> bool
        """
        Remove quantity from the given warehouse.
        If stock goes below 0, rollback and return False.
        Otherwise, record the operation in the audit log.
        """
        if (
            item_id not in self.item_warehouse_stocks or
            warehouse_id not in self.warehouse_item_stocks
        ):
            return False

        item_warehouse_stock = self.item_warehouse_stocks[item_id].get(warehouse_id, 0)
        warehouse_item_stock = self.warehouse_item_stocks[warehouse_id].get(item_id, 0)
        if item_warehouse_stock < quantity or warehouse_item_stock < quantity:
            return False

        self.item_warehouse_stocks[item_id][warehouse_id] = max(0, item_warehouse_stock - quantity)
        self.warehouse_item_stocks[warehouse_id][item_id] = max(0, warehouse_item_stock - quantity)

        self._add_item_audit_op(item_id, quantity, timestamp, InventoryStockOperation.ADD, warehouse_id)
        self._add_warehouse_stock_movement(warehouse_id, quantity)

    def transfer_stock(self, item_id: int, from_warehouse: int, to_warehouse: int, quantity: int, timestamp: int) -> bool
    """
    Move stock between warehouses atomically.
    If insufficient stock at the source, rollback and return False.
    """

    def get_global_stock(self, item_id: int) -> int
    """
    Return total stock of an item across all warehouses.
    """

    def get_warehouse_stock(self, item_id: int, warehouse_id: int) -> int
    """
    Return stock for a specific item in a specific warehouse.
    """

    def get_most_transferred_items(self, k: int) -> list[int]
    """
    Return the top-k items that have been transferred between warehouses the most.
    """

    def get_most_active_warehouses(self, k: int) -> list[int]
    """
    Return top-k warehouses ranked by total stock movement (adds + removes + transfers).
    """

    def get_stock_distribution(self, item_id: int) -> dict[int, int]
    """
    Return {warehouse_id: quantity} for a given item.
    """

    def get_audit_log(self, item_id: int, limit: int) -> list[tuple]
    """
    Return the most recent operations for that item:
    Each entry = (timestamp, operation, warehouse_id(s), quantity)
    """
    