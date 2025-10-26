# Question: Memory-Efficient Rolling Order Book

# You are designing a real-time order book for a financial exchange. The system must handle a high volume of orders where:
# - Each order has: order_id (unique), price (float), quantity (int), timestamp (int, seconds since epoch)
# - Orders can be created, updated, or canceled.
# - Multiple orders can have the same price, and price may fluctuate within a narrow range.
# Due to memory constraints, you cannot store every single order at very fine-grained prices.

# The system should support analytics queries efficiently:
# - Total quantity at a given price or price range
# - Top-K price levels by total quantity
# - Rolling statistics like average price, total quantity over last T seconds

# APIs to Implement

# record_order(order_id: str, price: float, quantity: int, timestamp: int)
# Create a new order. If the same order_id exists, it’s considered an update.

# update_order(order_id: str, new_price: float, new_quantity: int, timestamp: int)
# Change the price or quantity of an existing order.

# cancel_order(order_id: str, timestamp: int)
# Remove the order from the order book.

# get_total_quantity(price: float) -> int
# Return total quantity at the exact price level.

# get_total_quantity_in_range(low: float, high: float) -> int
# Return total quantity for all orders in the price range [low, high].

# get_top_k_prices(k: int) -> List[Tuple[float, int]]
# Return top-K price levels by total quantity.

# Constraints

# - There can be hundreds of thousands of orders per second.
# - Price changes are continuous within a narrow range, so storing every individual order may not be feasible.
# - Must support rolling analytics over a window of the last T seconds.
# - Memory usage must remain bounded.
# - Approximation is acceptable for total quantity and top-K queries if necessary.

from typing import Any


from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from heapdict import heapdict
from sortedcontainers import SortedDict
from threading import Lock

@dataclass
class Order:
    id: str
    price: float
    rounded_price: Decimal
    quantity: int
    timestamp: int

class OrderBook:
    def __init__(self,
        default_tick_size: str,
        num_stripes: int=128,
        K: int=5,
        ) -> None:
        self.ledger = SortedDict(list) # price -> (timestamp, order_id)
        self.storage: dict[str, Order] = {}   # order_id -> order
        self.default_tick_size = default_tick_size

        self.global_heap = heapdict()
        self.K = K

        self.global_lock = Lock()
        self.num_stripes = num_stripes
        self.price_locks = [Lock() for _ in range(num_stripes)]
        self.order_locks = [Lock() for _ in range(num_stripes)]


    def _round_price(self, price: float):
        d_price = Decimal(str(price))
        d_tick = Decimal(self.default_tick_size) 
        return (d_price // d_tick) * d_tick

    def _get_rounded_price_lock(self, price: Decimal):
        lock_index = hash(price) % self.num_stripes
        return self.price_locks[lock_index]
    
    def _get_order_lock(self, order_id: str):
        lock_index = hash(order_id) % self.num_stripes
        return self.order_locks[lock_index]

    def _update_global_heap(self, rounded_price: Decimal, count: int):
        with self.global_lock:
            if (
                len(self.global_heap) < self.K or
                self.global_heap and rounded_price in self.global_heap
            ):
                self.global_heap[rounded_price] = count
            else:
                _, lowest_count = self.global_heap.peekitem()
                if count > lowest_count:
                    self.global_heap.popitem()
                    self.global_heap[rounded_price] = count

    def record_order(self, order_id: str, price: float, quantity: int, timestamp: int):
        rounded_price = self._round_price(price)
        order = Order(
            order_id, price, rounded_price,
            quantity, timestamp
        )

        with self._get_rounded_price_lock(rounded_price):
            self.ledger[rounded_price].append((timestamp, order_id))
            self.storage[order_id] = order
            count = len(self.ledger[rounded_price])

        self._update_global_heap(rounded_price, count)

    def update_order(self, order_id: str, new_price: float, new_quantity: int, timestamp: int):
        if order_id not in self.storage:
            return

        with self._get_order_lock(order_id):
            order = self.storage[order_id]
            if timestamp <= order.timestamp:
                return

        new_rounded_price = self._round_price(new_price)
        if order.rounded_price == new_rounded_price:
            pass

        order.price = new_price
        order.quantity = new_quantity



    
    def cancel_order(order_id: str, timestamp: int):
        pass

    def get_total_quantity(price: float) -> int:
        """
        Return total quantity at the exact price level.
        """

    def get_total_quantity_in_range(low: float, high: float) -> int:
        pass

    def get_top_k_prices(k: int) -> List[Tuple[float, int]]:
        """
        Return top-K price levels by total quantity.
        """

