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
from threading import Lock, RLock

@dataclass
class Order:
    id: str
    price: float
    rounded_price: Decimal
    quantity: int
    timestamp: int

class AtomicBucket:
    """Thread-safe bucket using atomic operations"""
    def __init__(self) -> None:
        self.start_time = 0
        self.quantity = 0
        self.lock = Lock()

    def reset_and_add(self, start_time: int, quantity: int) -> None:
        """Reset bucket to new time period and add quantity"""
        with self.lock:
            self.start_time = start_time
            self.quantity = quantity

    def add_if_same_time(self, start_time: int, quantity: int) -> bool:
        """Add quantity if bucket is for same time period"""
        with self.lock:
            if self.start_time == start_time:
                self.quantity += quantity
                return True
            return False

    def subtract_if_same_time(self, start_time: int, quantity: int) -> bool:
        """Subtract quantity from bucket"""
        with self.lock:
            if self.start_time == start_time:
                self.quantity -= quantity
                return True
            return False

    def get_quantity_if_valid(self, cutoff_time: int) -> int:
        """Get quantity if bucket is still valid"""
        with self.lock:
            if self.start_time >= cutoff_time: # approximate will be an under estimate
                return max(0, self.quantity)
        return 0

    def read(self) -> tuple[int, int]:
        with self.lock:
            return (self.start_time, self.quantity)

class RingBuffer:
    def __init__(self,
        window_seconds:int=600,
        bucket_size:int=10,
        num_stripes:int=64,
        ) -> None:
        """
        window_seconds: total time to keep
        bucket_size: time per bucket
        """
        self.window_seconds = window_seconds
        self.bucket_size = bucket_size
        self.num_buckets = window_seconds // bucket_size

        self.buckets =  [AtomicBucket() for _ in range(self.num_buckets)]


    def _get_bucket_index(self, timestamp: int) -> int:
        return (timestamp // self.bucket_size) % self.num_buckets

    def _get_bucket_start_time(self, timestamp: int):
        return (timestamp // self.bucket_size) * self.bucket_size

    def add(self, timestamp: int, count: int=1):
        index = self._get_bucket_index(timestamp)
        bucket_start_time = self._get_bucket_start_time(timestamp)
        bucket = self.buckets[index]

        # Try to add to existing bucket
        if not bucket.add_if_same_time(bucket_start_time, count):
            # Bucket is for different time period, reset it
            bucket.reset_and_add(bucket_start_time, count) 
        return self.total()

    def minus(self, timestamp: int, count: int=1) -> int:
        index = self._get_bucket_index(timestamp)
        bucket_start_time = self._get_bucket_start_time(timestamp)
        _ = self.buckets[index].subtract_if_same_time(bucket_start_time, count)
        return self.total()

    def sum_last_t_seconds(self, t: int, current_time: int):
        cutoff_time = current_time - t
        total = 0
        for bucket in self.buckets:
            total += bucket.get_quantity_if_valid(cutoff_time)
        return total

    def total(self):
        total = 0
        for bucket in self.buckets:
            _, quantity = bucket.read()
            if quantity > 0:
                total += quantity
        return total

class OrderBook:
    def __init__(self,
        default_tick_size: str,
        window_seconds:int=600,
        bucket_size: int=10,        # 10 minutes in seconds
        num_locks: int=128,
        K: int=5,
        ) -> None:
        # price -> RingBuffer
        self.price_buckets: defaultdict[Decimal, RingBuffer] = defaultdict(
            lambda: RingBuffer(window_seconds=window_seconds, bucket_size=bucket_size)
        )
        self.window_seconds = window_seconds
        self.bucket_size = bucket_size
        self.storage: dict[str, Order] = {}   # order_id -> order

        self.default_tick_size = Decimal(default_tick_size)
        self.current_time = 0

        self.global_heap = heapdict()
        self.K = K

        self.global_lock = RLock()
        self.num_locks = num_locks
        self.price_locks = [RLock() for _ in range(num_locks)]
        self.order_locks = [RLock() for _ in range(num_locks)]

    def _round_price(self, price: float):
        d_price = Decimal(str(price))
        return (d_price / self.default_tick_size).quantize(Decimal("1")) * self.default_tick_size

    def _get_bucket_lock(self, rounded_price: Decimal):
        lock_index = hash(rounded_price) % self.num_locks
        return self.price_locks[lock_index]
    
    def _get_order_lock(self, order_id: str):
        lock_index = hash(order_id) % self.num_locks
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
            quantity, timestamp
        )

        with self.global_lock:
            self.current_time = max(self.current_time, timestamp)

        with self._get_order_lock(order_id):
            self.storage[order_id] = order

        with self._get_bucket_lock(rounded_price):
            count = self.price_buckets[rounded_price].add(timestamp, quantity)

        self._update_global_heap(rounded_price, count)

    def update_order(self, order_id: str, new_price: float, new_quantity: int, timestamp: int):
        if order_id not in self.storage:
            return

        with self._get_order_lock(order_id):
            order = self.storage[order_id]
            if timestamp < order.timestamp: 
                return

            old_quantity = order.quantity
            old_rounded_price = order.rounded_price
            old_timestamp = order.timestamp

            order.price = new_price
            order.rounded_price = self._round_price(new_price)
            order.quantity = new_quantity
            order.timestamp = timestamp
                
            with self._get_bucket_lock(old_rounded_price):
                count = self.price_buckets[old_rounded_price].minus(old_timestamp, old_quantity)
                self._update_global_heap(old_rounded_price, count)

            with self._get_bucket_lock(order.rounded_price):
                count = self.price_buckets[order.rounded_price].add(timestamp, new_quantity)
                self._update_global_heap(order.rounded_price, count)

    
    def cancel_order(self, order_id: str, timestamp: int):
        if order_id not in self.storage:
            return

        with self._get_order_lock(order_id):
            order = self.storage[order_id]
            if timestamp < order.timestamp:
                return
            rounded_price = order.rounded_price
            quantity = order.quantity
            del self.storage[order_id]

        with self._get_bucket_lock(rounded_price):
            count = self.price_buckets[rounded_price].minus(order.timestamp, quantity)
            self._update_global_heap(rounded_price, count)

    def get_total_quantity(self, price: float) -> int:
        """
        Return total quantity at the exact price level.
        """
        rounded_price = self._round_price(price)
        return self.price_buckets[rounded_price].total()

    def get_total_quantity_last_t_seconds(self, now: int, t: int) -> int:
        """Get total quantity across all prices in last t seconds"""
        return sum(
            ring_buffer.sum_last_t_seconds(t, now)
            for ring_buffer in list(self.price_buckets.values())
        )

    def get_total_quantity_in_range(self, low: float, high: float) -> int:
        """
        Return total quantity for all orders in the price range [low, high].
        """
        low_rounded_price = self._round_price(low)
        high_rounded_price = self._round_price(high)
        ring_buffers = [
            ring_buffer
            for price, ring_buffer in list(self.price_buckets.items())
            if low_rounded_price <= price <= high_rounded_price
        ]
        return sum(
            ring_buffer.total()
            for ring_buffer in ring_buffers
        )

    def format_price(self, price: Decimal):
        return str(price.quantize(Decimal("0.00")))

    def get_top_k_prices(self, k: int) -> list[tuple[str, int]]:
        """
        Return top-K price levels by total quantity.
        """
        k = min(self.K, k)
        return sorted([
            (self.format_price(rounded_price), count)
            for rounded_price, count in list(self.global_heap.items())
            ],
            key=lambda price_cnt: -price_cnt[1]
        )[:k]


    def get_average_price(self, now: int, t: int):
        total_quantity = total_value = 0
        for price, ring_buffer in list(self.price_buckets.items()):
            quantity = ring_buffer.sum_last_t_seconds(t, now)
            total_quantity += quantity
            total_value += float(price) * quantity

        res = Decimal("0.0")
        if total_quantity > 0:
            res = Decimal(str(total_value / total_quantity))
        return self.format_price(res)

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

    print("✓ Simple tests passed")

def test_rolling_window():
    """Test rolling window behavior"""
    book = OrderBook("0.01", window_seconds=60, bucket_size=10)
    
    # Add orders at different times
    book.record_order("o1", 100.0, quantity=10, timestamp=0)
    book.record_order("o2", 100.0, 20, timestamp=30)
    book.record_order("o3", 100.0, 30, timestamp=70)  # Outside window from time 0
    
    # At time 70, first order should be expired
    assert book.get_total_quantity_last_t_seconds(70, 60) == 50  # o2 + o3
    assert book.get_average_price(70, 60) == "100.00"

    book.update_order("o1", 100.0, 100, 80) # update outside window
    assert book.get_total_quantity_last_t_seconds(80, 60) == 150  # o1 + o2 + o3
    assert (
        [(0, 0), (70, 30), (80, 100), (30, 20), (0, 0), (0, 0)] ==
        [bucket.read() for bucket in book.price_buckets[Decimal("100.00")].buckets]
    )
    assert book.get_average_price(70, 60) == "100.00"

    
    print("✓ Rolling window tests passed")

def test_concurrent_updates():
    """Test update and cancel behavior"""
    book = OrderBook("0.01")
    
    book.record_order("o1", 100.0, 10, timestamp=100)
    assert book.get_total_quantity(100.0) == 10
    
    # Update to different price
    book.update_order("o1", 101.0, 20, timestamp=110)
    assert book.get_total_quantity(100.0) == 0
    assert book.get_total_quantity(101.0) == 20
    
    # Old timestamp should be ignored
    book.update_order("o1", 102.0, 30, timestamp=105)
    assert book.get_total_quantity(101.0) == 20
    
    # Cancel
    book.cancel_order("o1", timestamp=120)
    assert book.get_total_quantity(101.0) == 0
    
    print("✓ Concurrent update tests passed")


def test_ringbuffer_concurrency():
    """Test that RingBuffer handles concurrent updates correctly"""
    import threading
    
    buffer = RingBuffer(window_seconds=60, bucket_size=10)
    
    def add_orders(thread_id, count):
        for i in range(count):
            timestamp = 100 + (i % 6) * 10  # Spread across 6 buckets
            buffer.add(timestamp, 1)
    
    threads = []
    for i in range(10):
        t = threading.Thread(target=add_orders, args=(i, 100))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    # Should have 1000 total
    total = buffer.total()
    assert total == 1000, f"Expected 1000, got {total}"
    print("✓ RingBuffer concurrency tests passed")


if __name__ == "__main__":
    test_simple()
    test_rolling_window()
    test_concurrent_updates()
    test_ringbuffer_concurrency()
    print("\n✅ All tests passed!")