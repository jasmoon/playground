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
    cancelled: bool

class OrderBook:
    def __init__(self,
        default_tick_size: str,
        bucket_size: int=60 * 10, # 10 minutes in seconds
        num_stripes: int=128,
        K: int=5,
        ) -> None:
        self.buckets = defaultdict(lambda: defaultdict(int)) # price -> time bucket -> quantity
        self.bucket_size = bucket_size
        self.storage: dict[str, Order] = {}   # order_id -> order
        self.default_tick_size = default_tick_size

        self.global_heap = heapdict()
        self.K = K

        self.global_lock = Lock()
        self.num_stripes = num_stripes
        self.lock_stripes = [Lock() for _ in range(num_stripes)]
        self.order_locks = [Lock() for _ in range(num_stripes)]

    def _round_price(self, price: float):
        d_price = Decimal(str(price))
        d_tick = Decimal(self.default_tick_size) 
        return (d_price / d_tick).to_integral_value(rounding="ROUND_HALF_UP") * d_tick

    def _get_time_bucket_index(self, timestamp: int):
        return timestamp // self.bucket_size

    def _get_bucket_lock(self, rounded_price: Decimal):
        lock_index = hash(rounded_price) % self.num_stripes
        return self.lock_stripes[lock_index]
    
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
                price_with_lowest_count, lowest_count = self.global_heap.peekitem()
                if count > lowest_count:
                    self.global_heap.pop(price_with_lowest_count)
                    self.global_heap[rounded_price] = count

    def record_order(self, order_id: str, price: float, quantity: int, timestamp: int):
        rounded_price = self._round_price(price)
        order = Order(
            order_id, price, rounded_price,
            quantity, timestamp, False
        )

        time_bucket_index = self._get_time_bucket_index(timestamp)
        with self._get_bucket_lock(rounded_price):
            self.buckets[rounded_price][time_bucket_index] += quantity
            self.storage[order_id] = order
            count = self.buckets[rounded_price][time_bucket_index]

        self._update_global_heap(rounded_price, count)

    def update_order(self, order_id: str, new_price: float, new_quantity: int, timestamp: int):
        if order_id not in self.storage:
            return

        with self._get_order_lock(order_id):
            order = self.storage[order_id]
            if timestamp <= order.timestamp:
                return

            old_quantity = order.quantity
            old_rounded_price = order.rounded_price
            old_time_bucket_index = self._get_time_bucket_index(order.timestamp)

            order.price = new_price
            order.rounded_price = self._round_price(new_price)
            order.quantity = new_quantity
            order.timestamp = timestamp
                
        with self._get_bucket_lock(old_rounded_price):
            self.buckets[old_rounded_price][old_time_bucket_index] -= old_quantity
            count = self.buckets[old_rounded_price][old_time_bucket_index]
            self._update_global_heap(old_rounded_price, count)

        new_time_bucket_index = self._get_time_bucket_index(timestamp)
        with self._get_bucket_lock(order.rounded_price):
            self.buckets[order.rounded_price][new_time_bucket_index] += new_quantity
            count = self.buckets[order.rounded_price][new_time_bucket_index]
            self._update_global_heap(order.rounded_price, count)

    
    def cancel_order(self, order_id: str, timestamp: int):
        if order_id not in self.storage:
            return

        with self._get_order_lock(order_id):
            order = self.storage[order_id]
            if timestamp <= order.timestamp:
                return
            time_bucket_index = self._get_time_bucket_index(order.timestamp)
            rounded_price = order.rounded_price
            quantity = order.quantity
            order.cancelled = True

        with self._get_bucket_lock(rounded_price):
            self.buckets[rounded_price][time_bucket_index] -= quantity
            count = self.buckets[rounded_price][time_bucket_index]
            self._update_global_heap(rounded_price, count)

    def get_total_quantity(self, price: float) -> int:
        """
        Return total quantity at the exact price level.
        """
        rounded_price = self._round_price(price)
        quantities = list(self.buckets[rounded_price].values())
        return sum(quantity for quantity in quantities)

    def get_total_quantity_in_range(self, low: float, high: float) -> int:
        """
        Return total quantity for all orders in the price range [low, high].
        """
        low_rounded_price = self._round_price(low)
        high_rounded_price = self._round_price(high)
        items = list(self.buckets.items())
        time_buckets = [
            time_bucket
            for price, time_bucket in items
            if low_rounded_price <= price <= high_rounded_price
        ]
        return sum(
            quantity
            for time_bucket in time_buckets
            for quantity in time_bucket.values()
        )



    def get_top_k_prices(self, k: int) -> list[tuple[str, int]]:
        """
        Return top-K price levels by total quantity.
        """
        k = min(self.K, k)
        return sorted([
            (f"{rounded_price:.2f}", count)
            for rounded_price, count in list(self.global_heap.items())
            ],
            key=lambda price_cnt: -price_cnt[1]
        )[:k]


def test_simple():
    book = OrderBook("0.01")
    book.record_order(order_id="order1", timestamp=100, price=100.5, quantity=10)
    assert book.get_total_quantity(100.504) == 10
    assert book.get_total_quantity_in_range(100, 150) == 10

    book.record_order(order_id="order2", timestamp=120, price=100.5, quantity=10)
    book.record_order(order_id="order3", timestamp=240, price=101.0, quantity=5)
    assert book.get_total_quantity(100.504) == 20
    assert book.get_top_k_prices(5) == [("100.50", 20), ("101.00", 5)]

    book.record_order(order_id="order4", timestamp=200, price=100.5, quantity=100)
    book.record_order(order_id="order5", timestamp=150, price=101.5, quantity=15)
    assert book.get_top_k_prices(2) == [("100.50", 120), ("101.50", 15)]

    book.update_order(order_id="order5", new_price=102, new_quantity=200, timestamp=180)
    assert book.get_top_k_prices(2) == [("102.00", 200), ("100.50", 120)]

    book.cancel_order(order_id="order5", timestamp=200)
    assert book.get_top_k_prices(5) == [("100.50", 120), ("101.00", 5), ("101.50", 0), ("102.00", 0)]



if __name__ == "__main__":
    test_simple()