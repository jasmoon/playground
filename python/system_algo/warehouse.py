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

class InventorySystem:
    def add_stock(self, item_id: int, quantity: int, warehouse_id: int, timestamp: int) -> None
    """
    Add quantity of an item to the given warehouse.
    Each addition is recorded in an audit log.
    """

    def remove_stock(self, item_id: int, quantity: int, warehouse_id: int, timestamp: int) -> bool
    """
    Remove quantity from the given warehouse.
    If stock goes below 0, rollback and return False.
    Otherwise, record the operation in the audit log.
    """

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
    