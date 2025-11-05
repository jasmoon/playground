
# You are designing a real-time parking occupancy tracker for a city’s network of parking lots.

# Each parking lot can receive two types of events:
# - "enter" — a car enters the parking lot.
# - "exit" — a car leaves the parking lot.

# Each event has:
# - lot_id: str — the unique parking lot ID
# - car_id: str — the car’s license plate number
# - timestamp: int — seconds since epoch

# You need to implement a system that supports the following APIs:

# def record_event(self, lot_id: str, car_id: str, event_type: str, timestamp: int) -> None:
#     """
#     Record that a car has either entered or exited a parking lot at the given timestamp.
#     Events may arrive slightly out-of-order.
#     """

# def get_current_occupancy(self, lot_id: str) -> int:
#     """
#     Return the current number of cars inside the given parking lot.
#     """

# def get_occupancy_rate_rb(self, lot_id: str, last_t_seconds: int) -> float:
#     """
#     Return the average occupancy (in percentage of capacity) for this parking lot over the last t seconds.
#     """

# def get_citywide_trending_lots(self, last_t_seconds: int, k: int) -> List[str]:
#     """
#     Return the top k parking lots that have experienced the largest *rate of occupancy change*
#     (either increase or decrease) in the last t seconds.
#     """

# ⚙️ Constraints and Requirements
# - Each parking lot has a fixed maximum capacity known at initialization.
# - Events can arrive out of order by up to 30 seconds.
# - The system must be thread-safe.
# - You can assume there are hundreds of parking lots, and thousands of events per second.
# - Memory is limited — you cannot store all historical events.
# - Approximation within 2–5% error is acceptable for analytics queries.
import heapq
from enum import Enum
from threading import Lock


class AtomicBucket:
    def __init__(self) -> None:
        self.start_time = 0
        self.quantity = 0
        self.lock = Lock()

    def add(self, start_time: int, quantity: int):
        with self.lock:
            if start_time == self.start_time:
                self.quantity += quantity
            elif start_time > self.start_time:
                self.start_time = start_time
                self.quantity = quantity

    def read(self):
        with self.lock:
            return (self.start_time, self.quantity)

class RingBuffer:
    def __init__(self,
        window_seconds: int=3600,  # 1 hour
        bucket_size: int=10,        # 10 seconds
        num_shards=8
    ) -> None:
        self.window_seconds = window_seconds
        self.bucket_size = bucket_size
        self.num_buckets = window_seconds // bucket_size + 1

        
        self.buckets = [AtomicBucket() for _ in range(self.num_buckets)]

    def _get_bucket_start_time(self, timestamp: int):
        return (timestamp // self.bucket_size) * self.bucket_size # [ts1-ts2)

    def _get_bucket_index(self, timestamp: int):
        return (timestamp // self.bucket_size) % self.num_buckets

    def add(self, timestamp: int, quantity: int=1):
        start_time = self._get_bucket_start_time(timestamp)
        bucket_index = self._get_bucket_index(timestamp)
        bucket = self.buckets[bucket_index]
        bucket.add(start_time, quantity)

    def total(self, cutoff: int):
        total = 0
        cutoff_start_time = self._get_bucket_start_time(cutoff)
        for bucket in self.buckets:
            start_time, quantity = bucket.read()
            if start_time >= cutoff_start_time:
                total += quantity
        return total


class CarparkEventType(Enum):
    ENTER = 1
    EXIT = 2

class CarparkTracker:
    def __init__(self,
        capacities: dict[str, int],
        max_delay: int=30,
        window_seconds:int = 3600,  # 1 hour
        bucket_size: int=10,        # 10 seconds
        num_locks:int=128,
    ) -> None:
        self.capacities = capacities
        self.window_seconds = window_seconds
        self.bucket_size = bucket_size
        self.current_time = 0
        self.max_delay = max_delay

        self.lot_occupancy: dict[str, set[str]] =  {}           # lot_id -> set(car_id)
        self.enter_time_buckets: dict[str, RingBuffer] = {}     # lot_id -> hash ring
        self.exit_time_buckets: dict[str, RingBuffer] = {}      # lot_id -> hash ring
        self.occupancy_snapshots: dict[str, list[tuple[int, int]]] = {}            # lot_id -> SortedList[(timestamp, occupancy)]

        for lot_id in capacities.keys():
            self.enter_time_buckets[lot_id] = RingBuffer(window_seconds, bucket_size)
            self.exit_time_buckets[lot_id] = RingBuffer(window_seconds, bucket_size)

        self.global_lock = Lock()
        self.num_locks = num_locks
        self.lot_locks = [Lock() for _ in range(num_locks)]

    def _get_lot_lock(self, lot_id: str):
        index = hash(lot_id) % self.num_locks
        return self.lot_locks[index]

    def _record_occupancy(self, lot_id: str, car_id: str, event_type: CarparkEventType) -> bool:
        with self._get_lot_lock(lot_id):
            if lot_id not in self.lot_occupancy:
                self.lot_occupancy[lot_id] = set()
            lot = self.lot_occupancy[lot_id]

            if event_type == CarparkEventType.ENTER:
                if len(lot) >= self.capacities[lot_id] or car_id in lot:
                    return False
                lot.add(car_id)
                return True

            else:
                if car_id not in lot:
                    return False

                lot.discard(car_id)
                return True
    
    def _record_occupancy_snapshot(self, lot_id: str, timestamp: int):
        if timestamp % 60 == 0:
            with self._get_lot_lock(lot_id):
                occupancy = len(self.lot_occupancy[lot_id])
                if lot_id not in self.occupancy_snapshots:
                    self.occupancy_snapshots[lot_id] = []
                self.occupancy_snapshots[lot_id].append((timestamp, occupancy))

                cutoff = self.current_time - self.window_seconds
                self.occupancy_snapshots[lot_id] = [
                    (timestamp, occupancy)
                    for timestamp, occupancy in self.occupancy_snapshots[lot_id]
                    if timestamp > cutoff
                ]

    def record_event(self, lot_id: str, car_id: str, event_type: CarparkEventType, timestamp: int) -> None:
        """
        Record that a car has either entered or exited a parking lot at the given timestamp.
        Events may arrive slightly out-of-order.
        """
        if lot_id not in self.capacities:
            return # reject, but can actually return error here

        if timestamp < self.current_time - self.max_delay:
            return

        if not self._record_occupancy(lot_id, car_id, event_type):
            return
        
        self._record_occupancy_snapshot(lot_id, timestamp)
        if event_type == CarparkEventType.ENTER:
            self.enter_time_buckets[lot_id].add(timestamp)
        else:
            self.exit_time_buckets[lot_id].add(timestamp)


        with self.global_lock:
            self.current_time = max(self.current_time, timestamp)

    def get_current_occupancy(self, lot_id: str) -> int:
        """
        Return the current number of cars inside the given parking lot.
        """
        with self._get_lot_lock(lot_id):
            if lot_id in self.lot_occupancy:
                return len(self.lot_occupancy[lot_id]) 
            else:
                return 0

    def get_occupancy_rate_snapshot(self, lot_id: str, last_t_seconds: int) -> float:
        now = self.current_time
        cutoff = now - last_t_seconds
        with self._get_lot_lock(lot_id):
            capacity = self.capacities.get(lot_id, 0)
            if capacity == 0:
                return 0.

            snapshot = self.occupancy_snapshots.get(lot_id, [])
        
        snapshots = [
            occupancy
            for timestamp, occupancy in snapshot
            if timestamp >= cutoff
        ]

        if not snapshots:
            return self.get_current_occupancy(lot_id) / capacity

        return sum(snapshots) / len(snapshots) / capacity
            

    def get_occupancy_rate_rb(self, lot_id: str, last_t_seconds: int) -> float:
        """
        Return the average occupancy (in percentage of capacity) for this parking lot over the last t seconds.
        """
        now = self.current_time
        last_t_seconds = max(self.bucket_size, last_t_seconds)
        last_t_seconds = min(self.window_seconds, last_t_seconds)
        cutoff = now - last_t_seconds

        with self._get_lot_lock(lot_id):
            window_occupancies = [len(self.lot_occupancy[lot_id])]
            capacity = self.capacities[lot_id]

        prev_change = 0
        curr = now
        while curr - self.bucket_size >= cutoff:
            curr_change = (
                self.enter_time_buckets[lot_id].total(cutoff=curr) -
                self.exit_time_buckets[lot_id].total(cutoff=curr)
            )

            diff = curr_change - prev_change
            window_occupancies.append(max(0, window_occupancies[-1] - diff))
            prev_change = curr_change
            curr -= self.bucket_size

        return sum(window_occupancies) / len(window_occupancies) / capacity


    def get_citywide_trending_lots(self, last_t_seconds: int, k: int) -> list[str]:
        """
        Return the top k parking lots that have experienced the largest *rate of occupancy change*
        (either increase or decrease) in the last t seconds.
        """
        now = self.current_time
        last_t_seconds = max(self.bucket_size, last_t_seconds)
        last_t_seconds = min(self.window_seconds, last_t_seconds)
        cutoff = now - last_t_seconds

        rates: dict[str, float] = {}
        
        for lot_id in self.capacities.keys():
            net_change = (
                self.enter_time_buckets[lot_id].total(cutoff) - 
                self.exit_time_buckets[lot_id].total(cutoff)
            )
            rates[lot_id] = net_change / last_t_seconds
        
        return heapq.nlargest(
            k,
            rates,
            key=lambda lot_id: abs(rates[lot_id]),
        )

def test_simple():
    tracker = CarparkTracker(capacities={"A": 100, "B": 200})

    tracker.record_event("A", "SGB1234K", CarparkEventType.ENTER, 100)  # {"A": 1}
    tracker.record_event("A", "SGB5678M", CarparkEventType.ENTER, 102)  # {"A": 2}
    tracker.record_event("A", "SGB1234K", CarparkEventType.EXIT, 105)   # {"A": 1}

    tracker.record_event("B", "SKX9876C", CarparkEventType.ENTER, 110)  # {"A": 1, "B": 1}
    tracker.record_event("A", "SGB5678K", CarparkEventType.ENTER, 103)  # {"A": 2, "B": 1}
    tracker.record_event("A", "SGB5678Q", CarparkEventType.ENTER, 125)  # {"A": 3, "B": 1}
    tracker.record_event("A", "SGB5678Q", CarparkEventType.ENTER, 125)  # {"A": 3, "B": 1}


    print(tracker.get_current_occupancy("A"))
    print(tracker.get_occupancy_rate_rb("A", 10))
    print(tracker.get_occupancy_rate_snapshot("A", 10))

    print(tracker.get_citywide_trending_lots(60, 1))  # ["A"]

def test_occupancy_rate():
    tracker = CarparkTracker(capacities={"A": 100}, bucket_size=10)
    
    # Time 90-100: 5 cars enter
    for i in range(5):
        tracker.record_event("A", f"CAR{i}", CarparkEventType.ENTER, 95)
    
    # Current occupancy at time 100: 5 cars
    assert tracker.get_current_occupancy("A") == 5
    
    # Average over last 20 seconds [80, 100]:
    # - [80, 90): 0 cars
    # - [90, 100): 0 → 5 cars (let's say avg ~2.5)
    # - [100, now): 5 cars
    rate = tracker.get_occupancy_rate_rb("A", 20)
    
    # With your current (buggy) code: window_occupancies = [5, 10, 10]
    # → avg = 8.33 / 100 = 0.083 ❌
    
    # With fixed code: window_occupancies = [5, 0, 0]
    # → avg = 1.67 / 100 = 0.0167 ✓
    print(f"Rate: {rate}")

def test_capacity_limit():
    """Test that capacity limits are enforced"""
    print("\n=== Test 3: Capacity Limit ===")
    tracker = CarparkTracker(capacities={"A": 2})
    
    tracker.record_event("A", "CAR1", CarparkEventType.ENTER, 100)
    tracker.record_event("A", "CAR2", CarparkEventType.ENTER, 105)
    tracker.record_event("A", "CAR3", CarparkEventType.ENTER, 110)  # Should be rejected
    
    assert tracker.get_current_occupancy("A") == 2, "Should not exceed capacity"
    print("✓ Capacity limits enforced")

def test_occupancy_rate_snapshot():
    """Test occupancy rate calculation using snapshots"""
    print("\n=== Test 4: Occupancy Rate (Snapshots) ===")
    tracker = CarparkTracker(capacities={"A": 100}, bucket_size=10)
    
    # Time 0-60: Empty
    tracker.record_event("A", "CAR1", CarparkEventType.ENTER, 0)
    tracker.record_event("A", "CAR1", CarparkEventType.EXIT, 10)
    
    # Time 60: Snapshot with 0 cars
    tracker.record_event("A", "DUMMY", CarparkEventType.ENTER, 60)
    tracker.record_event("A", "DUMMY", CarparkEventType.EXIT, 61)
    
    # Time 120: Add 5 cars, snapshot with 5 cars
    for i in range(5):
        tracker.record_event("A", f"CAR{i}", CarparkEventType.ENTER, 120)
    
    # Time 180: Add 5 more cars, snapshot with 10 cars
    for i in range(5, 10):
        tracker.record_event("A", f"CAR{i}", CarparkEventType.ENTER, 180)
    
    # Time 240: Remove 5 cars, snapshot with 5 cars
    for i in range(5):
        tracker.record_event("A", f"CAR{i}", CarparkEventType.EXIT, 240)
    
    # Average over last 240 seconds
    # Snapshots: [60: 0, 120: 5, 180: 10, 240: 5]
    # Average: (0 + 5 + 10 + 5) / 4 = 5 cars
    # Rate: 5 / 100 = 0.05 = 5%
    rate = tracker.get_occupancy_rate_snapshot("A", 240)
    expected_rate = (0 + 5 + 10 + 5) / 4 / 100
    
    print(f"Snapshots: {tracker.occupancy_snapshots.get('A', [])}")
    print(f"Calculated rate: {rate:.4f}, Expected: {expected_rate:.4f}")
    assert abs(rate - expected_rate) < 0.01, f"Rate should be ~{expected_rate}"
    print("✓ Snapshot-based occupancy rate works")


def test_occupancy_rate_ringbuffer():
    """Test occupancy rate calculation using ring buffer reconstruction"""
    print("\n=== Test 5: Occupancy Rate (Ring Buffer) ===")
    tracker = CarparkTracker(capacities={"A": 100}, bucket_size=10)
    
    # Time 100: 5 cars enter
    for i in range(5):
        tracker.record_event("A", f"CAR{i}", CarparkEventType.ENTER, 100)
    
    # Time 110: 3 more cars enter (total 8)
    for i in range(5, 8):
        tracker.record_event("A", f"CAR{i}", CarparkEventType.ENTER, 110)
    
    # Time 120: 2 cars exit (total 6)
    for i in range(2):
        tracker.record_event("A", f"CAR{i}", CarparkEventType.EXIT, 120)
    
    # Current time is 120, occupancy is 6
    current = tracker.get_current_occupancy('A')
    print(f"Current occupancy at t=120: {current}")
    
    # Debug: Check what's in the buckets
    print("\nBucket contents:")
    for t in [120, 110, 100, 90]:
        enters = tracker.enter_time_buckets["A"].total(cutoff=t)
        exits = tracker.exit_time_buckets["A"].total(cutoff=t)
        print(f"  From t={t} onwards: {enters} enters, {exits} exits, net={enters-exits}")
    
    # Calculate rate over last 30 seconds [90, 120]
    rate = tracker.get_occupancy_rate_rb("A", 30)

    # Expected reconstruction:
    # Time 120: 6 cars (current)
    # Bucket [110, 120): 0 enters, 2 exits → net = -2
    #   → Time 110: 6 - (-2) = 8 cars
    # Bucket [100, 110): 3 enters, 0 exits → net = +3
    #   → Time 100: 8 - 3 = 5 cars
    # Bucket [90, 100): 5 enters, 0 exits → net = +5
    #   → Time 90: 5 - 5 = 0 cars
    # Average: (6 + 8 + 5 + 0) / 4 = 4.75 cars
    # Rate: 4.75 / 100 = 0.0475
    
    print(f"\nCalculated rate: {rate:.4f}")
    expected_rate = 4.75 / 100
    print(f"Expected rate: {expected_rate:.4f}")
    assert abs(rate - expected_rate) < 0.01, f"Rate should be ~{expected_rate}, got {rate}"

    rate = tracker.get_occupancy_rate_rb("A", 35)
    print(f"\nCalculated rate: {rate:.4f}")
    assert abs(rate - expected_rate) < 0.01, f"Rate should be ~{expected_rate}, got {rate}"

    tracker = CarparkTracker(capacities={"A": 100}, bucket_size=10)
    
    # Time 100: 5 cars enter
    for i in range(5):
        tracker.record_event("A", f"CAR{i}", CarparkEventType.ENTER, 100)
    
    # Time 110: 3 more cars enter (total 8)
    for i in range(5, 8):
        tracker.record_event("A", f"CAR{i}", CarparkEventType.ENTER, 110)
    
    # Time 119: 2 cars exit (total 6)
    for i in range(2):
        tracker.record_event("A", f"CAR{i}", CarparkEventType.EXIT, 119)
    
    # Current time is 119, occupancy is 6
    current = tracker.get_current_occupancy('A')
    print(f"Current occupancy at t=119: {current}")
    
    # Debug: Check what's in the buckets
    print("\nBucket contents:")
    for t in [119, 110, 100, 90]:
        enters = tracker.enter_time_buckets["A"].total(cutoff=t)
        exits = tracker.exit_time_buckets["A"].total(cutoff=t)
        print(f"  From t={t} onwards: {enters} enters, {exits} exits, net={enters-exits}")
    
    # Calculate rate over last 30 seconds [89, 119]
    rate = tracker.get_occupancy_rate_rb("A", 30)

    # Expected reconstruction:
    # Time 119: 6 cars (current)
    # Bucket [110, 120): 3 enters, 2 exits → net = +1
    #   → Time 110: 6 - 1 = 5 cars
    # Bucket [100, 110): 5 enters, 0 exits → net = +5
    #   → Time 100: 5 - 5 = 0 cars
    # Bucket [90, 100): 0 enters, 0 exits → net = 0
    #   → Time 90: 0 - 0 = 0 cars
    # Average: (6 + 5 + 0 + 0) / 4 = 2.75 cars
    # Rate: 2.75 / 100 = 0.0275
    
    print(f"\nCalculated rate: {rate:.4f}")
    expected_rate = 2.75 / 100
    print(f"Expected rate: {expected_rate:.4f}")
    assert abs(rate - expected_rate) < 0.01, f"Rate should be ~{expected_rate}, got {rate}"

    rate = tracker.get_occupancy_rate_rb("A", 35)
    print(f"\nCalculated rate: {rate:.4f}")
    assert abs(rate - expected_rate) < 0.01, f"Rate should be ~{expected_rate}, got {rate}"

    print("✓ Ring buffer reconstruction works")



def test_trending_lots():
    """Test citywide trending lots detection"""
    print("\n=== Test 6: Trending Lots ===")
    tracker = CarparkTracker(capacities={"A": 100, "B": 100, "C": 100}, bucket_size=10)
    
    # Lot A: +10 cars
    for i in range(10):
        tracker.record_event("A", f"CAR_A{i}", CarparkEventType.ENTER, 100)
    
    # Lot B: +5 cars
    for i in range(5):
        tracker.record_event("B", f"CAR_B{i}", CarparkEventType.ENTER, 100)
    
    # Lot C: -3 cars (simulate exits from pre-existing cars)
    tracker.record_event("C", "CAR_C1", CarparkEventType.ENTER, 50)
    tracker.record_event("C", "CAR_C2", CarparkEventType.ENTER, 50)
    tracker.record_event("C", "CAR_C3", CarparkEventType.ENTER, 50)
    tracker.record_event("C", "CAR_C1", CarparkEventType.EXIT, 100)
    tracker.record_event("C", "CAR_C2", CarparkEventType.EXIT, 100)
    tracker.record_event("C", "CAR_C3", CarparkEventType.EXIT, 100)
    
    trending = tracker.get_citywide_trending_lots(60, 2)
    print(f"Top 2 trending lots: {trending}")
    
    # A should be #1 (rate = 10/60 = 0.167)
    # C should be #2 (rate = |-3|/60 = 0.05)
    assert trending[0] == "A", "Lot A should be most trending"
    print("✓ Trending lots detection works")

if __name__ == "__main__":
    test_simple()
    test_occupancy_rate()
    test_capacity_limit()
    test_occupancy_rate_snapshot()
    test_occupancy_rate_ringbuffer()
    test_trending_lots()