from collections import defaultdict
from threading import Lock
from hyperloglog import HyperLogLog

# Question: Unique Visitors in a Rolling Window

# Problem Statement:
# You are designing analytics for a high-traffic website. The system needs to support the following operations in real-time:

# record_visit(user_id: str, timestamp: int)
# Record that a user visited the site at the given timestamp. User IDs are arbitrary strings.

# get_unique_visitors_last_t_seconds(t: int) -> int
# Return the approximate number of unique users who visited the site in the last t seconds.

# Constraints:
# - The site can have hundreds of millions of visits per day.
# - Exact counting of unique users is too memory-intensive.
# - The system should support real-time queries for arbitrary t within the last 7 days.
# - Memory usage should remain bounded.

class RollingHLL:
    def __init__(self,
        window_seconds:int =7*24*60*60,
        bucket_size:int = 60*60,         # default 1 hour
        num_stripes:int = 128
    ) -> None:
        self.window_seconds = window_seconds
        self.bucket_size = bucket_size
        self.num_buckets = self.window_seconds // self.bucket_size + 1
        self.error_percent = 0.01 # 1%
        self.buckets = defaultdict(lambda: HyperLogLog(self.error_percent))

        self.num_stripes = num_stripes
        self.lock_stripes = [Lock() for _ in range(num_stripes)]
        self.global_lock = Lock()

    def _get_bucket_index(self, timestamp: int):
        return timestamp // self.bucket_size

    def _get_bucket_lock(self, bucket_index) -> Lock:
        lock_index = hash(bucket_index) % self.num_stripes
        return self.lock_stripes[lock_index]

    def cleanup_old_data(self, now: int):
        if not self.buckets:
            return

        cutoff_time = max(0, now - self.window_seconds)
        cutoff_index = self._get_bucket_index(cutoff_time)
        expired_indexes = [
            index
            for index in list(self.buckets.keys())
            if index < cutoff_index # exclude cutoff_index
        ]
        for index in expired_indexes: 
            with self._get_bucket_lock(index):
                self.buckets.pop(index, None)

    def _get_bucket(self, bucket_index: int) -> HyperLogLog:
        if bucket_index not in self.buckets:
            new_hll = HyperLogLog(self.error_percent)
            self.buckets[bucket_index] = new_hll
        return self.buckets[bucket_index]

    def add(self, item: str, timestamp: int):
        bucket_index = self._get_bucket_index(timestamp)
        with self._get_bucket_lock(bucket_index):
            bucket = self._get_bucket(bucket_index)
            bucket.add(item)

    def count_from(self, cutoff_time: int, now: int):
        cutoff_time = max(0, cutoff_time)
        cutoff_index = self._get_bucket_index(cutoff_time)
        now_index = self._get_bucket_index(now)
        merged = HyperLogLog(self.error_percent)
        for index in range(cutoff_index, now_index+1):
            with self._get_bucket_lock(index):
                if index in self.buckets:
                    merged.update(self.buckets[index])
        return len(merged)

class VisitTracker:
    def __init__(self,
        window_seconds: int =7*24*60*60,
        bucket_size: int = 60*60,         # default 1 hour
        cleanup_threshold: int = 5000
    ) -> None:
        self.current_time = 0 # for easy testing

        self.window_seconds = window_seconds
        self.bucket_size = bucket_size
        self.rolling_hll = RollingHLL(window_seconds, bucket_size)
        
        self.global_lock = Lock()
        self.events_since_cleanup = 0
        self.cleanup_threshold = cleanup_threshold

    def _cleanup_old_data(self):
        if not self.global_lock.acquire(blocking=False):
            return

        try:
            self.rolling_hll.cleanup_old_data(self.current_time)
            self.events_since_cleanup = 0
        finally:
            self.global_lock.release()

    def record_visit(self, user_id: str, timestamp: int):
        self.current_time = max(self.current_time, timestamp)
        self.rolling_hll.add(user_id, timestamp)

        events_since_cleanup = self.events_since_cleanup
        self.events_since_cleanup += 1
        if events_since_cleanup >= self.cleanup_threshold:
            self._cleanup_old_data()

    def get_unique_visitors_last_t_seconds(self, t: int) -> int:
        now = self.current_time
        cutoff_time = max(0, now - t)
        return self.rolling_hll.count_from(cutoff_time, now)
    

def test_simple():
    tracker = VisitTracker(cleanup_threshold=3)

    tracker.record_visit("user1", 1)
    tracker.record_visit("user2", 2)
    tracker.record_visit("user3", timestamp=3)

    assert tracker.get_unique_visitors_last_t_seconds(1000) == 3

    tracker.record_visit("user4", timestamp=8*24*60*60)
    tracker.record_visit("user5", timestamp=8*24*60*60+1)

    assert tracker.get_unique_visitors_last_t_seconds(t=1000) == 2

    tracker.record_visit("user1", timestamp=8*24*60*60+2)
    assert tracker.get_unique_visitors_last_t_seconds(t=1000) == 3

if __name__ == "__main__":
    test_simple()
