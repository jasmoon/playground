
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

# def get_occupancy_rate(self, lot_id: str, last_t_seconds: int) -> float:
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
        self.num_buckets = window_seconds // bucket_size

        
        self.buckets = [AtomicBucket() for _ in range(self.num_buckets)]

    def _get_bucket_start_time(self, timestamp: int):
        return (timestamp // self.bucket_size) * self.bucket_size

    def _get_bucket_index(self, timestamp: int):
        return (timestamp // self.bucket_size) % self.num_buckets

    def add(self, timestamp: int, quantity: int=1):
        start_time = self._get_bucket_start_time(timestamp)
        bucket_index = self._get_bucket_index(timestamp)
        bucket = self.buckets[bucket_index]
        bucket.add(start_time, quantity)

    def total(self, cutoff_time: int):
        total = 0
        for bucket in self.buckets:
            start_time, quantity = bucket.read()
            if start_time + self.bucket_size > cutoff_time:
                total += quantity
        return total


class CarparkEventType(Enum):
    ENTER = 1
    EXIT = 2

class CarparkTracker:
    def __init__(self,
        capacities: dict[str, int],
        window_seconds:int = 3600,  # 1 hour
        bucket_size: int=10,        # 10 seconds
        num_locks:int=128,
    ) -> None:
        self.capacities = capacities
        self.window_seconds = window_seconds
        self.bucket_size = bucket_size
        self.current_time = 0

        self.lot_occupancy: dict[str, set[str]] =  {}   # lot_id -> set(car_id)
        self.enter_time_buckets: dict[str, RingBuffer] = {}    # lot_id -> (time bucket -> count) OR rolling CMS or hash ring
        self.exit_time_buckets: dict[str, RingBuffer] = {}     # lot_id -> (time bucket -> count) OR rolling CMS or hash ring

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


    def record_event(self, lot_id: str, car_id: str, event_type: CarparkEventType, timestamp: int) -> None:
        """
        Record that a car has either entered or exited a parking lot at the given timestamp.
        Events may arrive slightly out-of-order.
        """
        if lot_id not in self.capacities:
            return # reject, but can actually return error here

        if not self._record_occupancy(lot_id, car_id, event_type):
            return
        
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
            return len(self.lot_occupancy[lot_id])


    def get_occupancy_rate(self, lot_id: str, last_t_seconds: int) -> float:
        """
        Return the average occupancy (in percentage of capacity) for this parking lot over the last t seconds.
        """
        now = self.current_time
        last_t_seconds = max(self.bucket_size, last_t_seconds)
        last_t_seconds = min(self.window_seconds, last_t_seconds)
        cutoff_time = now - last_t_seconds

        with self._get_lot_lock(lot_id):
            window_occupancies = [len(self.lot_occupancy[lot_id])]
            capacity = self.capacities[lot_id]
        prev_total = (
                self.enter_time_buckets[lot_id].total(cutoff_time=now) -
                self.exit_time_buckets[lot_id].total(cutoff_time=now)
            )
        curr = now - self.bucket_size

        while curr >= cutoff_time:
            curr_total = (
                self.enter_time_buckets[lot_id].total(cutoff_time=curr) -
                self.exit_time_buckets[lot_id].total(cutoff_time=curr)
            )
            
            diff = prev_total - curr_total
            window_occupancies.append(max(0, window_occupancies[-1] - diff))
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
        cutoff_time = now - last_t_seconds

        largest_lot_diffs: dict[str, int] = {}
        
        for lot_id in self.capacities.keys():
            diffs = []
            curr = now
            prev_diff = 0

            while curr >= cutoff_time:
                diff = (
                        self.enter_time_buckets[lot_id].total(cutoff_time=curr) -
                        self.exit_time_buckets[lot_id].total(cutoff_time=curr)
                    )
                diffs.append(diff - prev_diff)
                prev_diff = diff
                curr -= self.bucket_size
            largest_lot_diffs[lot_id] = max(diffs, key=abs)
        
        top_k = heapq.nlargest(
            k,
            largest_lot_diffs.items(),
            key=lambda lot_id_diff: abs(lot_id_diff[1]),
        )
        return [lot_id for lot_id, _ in top_k]

def test_simple():
    tracker = CarparkTracker(capacities={"A": 100, "B": 200})

    tracker.record_event("A", "SGB1234K", CarparkEventType.ENTER, 100)
    tracker.record_event("A", "SGB5678M", CarparkEventType.ENTER, 102)
    tracker.record_event("A", "SGB1234K", CarparkEventType.EXIT, 105)
    tracker.record_event("B", "SKX9876C", CarparkEventType.ENTER, 110)
    tracker.record_event("A", "SGB5678K", CarparkEventType.ENTER, 103)
    tracker.record_event("A", "SGB5678Q", CarparkEventType.ENTER, 125)
    tracker.record_event("A", "SGB5678Q", CarparkEventType.ENTER, 125)


    print(tracker.get_current_occupancy("A"))
    print(tracker.get_occupancy_rate("A", 10))
    print(tracker.get_citywide_trending_lots(60, 1))  # ["A"]

if __name__ == "__main__":
    test_simple()