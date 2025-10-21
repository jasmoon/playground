from bisect import bisect_right
from collections import defaultdict
from typing import Any
from heapdict import heapdict
from threading import Lock
import heapq
import mmh3
import numpy as np


# Problem statement
# You’re building the backend for a social media platform that tracks trending hashtags in real time.
# Implement a class TrendingTracker with the following methods:
# class TrendingTracker:
#     def record_post(self, hashtag: str, timestamp: int) -> None:
#         """Record that a post with this hashtag was created at the given timestamp (seconds since epoch)."""

#     def get_top_k_trending(self, k: int, t: int) -> list[tuple[str, int]]:
#         """Return the top k hashtags used in the last t seconds, ranked by frequency."""

# Assumptions made:
# - Timestamps received are monotonic (strictly increasing)

class CountMinSketch:
    def __init__(self, width=1000, depth=5) -> None:
        """
        - Higher width → lower error, more memory
        - Higher depth → better accuracy, slower updates
        """
        self.width = width
        self.depth = depth
        self.table = np.zeros((depth, width), dtype=int)
        self.seeds = [i * 31 for i in range(depth)]

    def add(self, item: str, count: int=1) -> int:
        for i in range(self.depth):
            idx = mmh3.hash(item, self.seeds[i]) % self.width
            self.table[i][idx] += count
        return self.estimate(item)
        

    def estimate(self, item: str) -> int:
        return int(min(
            self.table[i][mmh3.hash(item, self.seeds[i]) % self.width]
            for i in range(self.depth)
        ))

    def subtract(self, other: "CountMinSketch"):
        assert self.width == other.width and self.depth == other.depth
        self.table -= other.table
        self.table = np.maximum(self.table, 0)

class RollingCMS:
    def __init__(self,
        bucket_size: int=10,
        max_window_duration:int = 24 * 60 * 60,
        width: int=1000,
        depth: int=5,
        num_stripes: int=128) -> None:

        self.bucket_size = bucket_size # seconds that each bucket hold
        self.num_buckets = max_window_duration // bucket_size + 1
        self.width = width
        self.depth = depth

        self.buckets: defaultdict[int, CountMinSketch] = defaultdict(
            lambda: CountMinSketch(self.width, self.depth)
        )
        self.merged = CountMinSketch(self.width, self.depth)

        self.global_lock = Lock()
        self.lock_stripes = [Lock() for _ in range(num_stripes)]

    def _get_bucket_index(self, timestamp: int) -> int:
        return timestamp // self.bucket_size

    def _get_bucket_lock(self, bucket_index: int) -> Lock:
        stripe_index = hash(bucket_index) % len(self.lock_stripes)
        return self.lock_stripes[stripe_index]

    def cleanup_old_data(self, cutoff_time: int) -> None:
        if not self.buckets:
            return

        start_index = min(self.buckets.keys())
        cutoff_index = self._get_bucket_index(cutoff_time)
        to_subtract: list[CountMinSketch] = []
        for expired_index in range(start_index, cutoff_index): # exclude cutoff_index
            with self._get_bucket_lock(expired_index):
                expired_cms = self.buckets[expired_index]
                to_subtract.append(expired_cms)
                del self.buckets[expired_index]
        
        if to_subtract:
            with self.global_lock:
                for cms in to_subtract:
                    self.merged.subtract(cms)
                
    def _get_now_bucket(self, now_bucket_index: int) -> CountMinSketch:
        with self.global_lock:
            if now_bucket_index not in self.buckets:
                new_cms = CountMinSketch(self.width, self.depth)
                self.buckets[now_bucket_index] = new_cms
        return self.buckets[now_bucket_index]

    def add(self, key: str, timestamp: int, count:int=1) -> int:
        bucket_index = self._get_bucket_index(timestamp)
        lock  = self._get_bucket_lock(bucket_index)
        with lock:
            bucket = self._get_now_bucket(bucket_index)
            bucket.add(key, count)
        with self.global_lock:
            return self.merged.add(key, count)

    def estimate(self, key: str) -> int:
        return self.merged.estimate(key)

    def estimate_from(self, keys: list[str], cutoff_time: int, now: int) -> defaultdict[str, int]:
        approx_counts: defaultdict[str, int] = defaultdict(int)
        cutoff_index = self._get_bucket_index(cutoff_time)
        latest_index = self._get_bucket_index(now)
        for key in keys:
            for bucket_index in range(cutoff_index, latest_index+1):
                with self._get_bucket_lock(bucket_index):
                    if bucket_index in self.buckets:
                        approx_counts[key] += self.buckets[bucket_index].estimate(key)
        return approx_counts
        
class TrendingTracker:
    def __init__(self,
        window_seconds: int=24*60*60,
        K: int=10,
        clean_every_n_posts: int=5000,
        num_stripes: int=128) -> None:

        # hashtag -> timestamps
        self.hashtag_timestamps: defaultdict[str, list[int]] = defaultdict(list)
        self.top_k_global_heap: heapdict = heapdict()

        self.window_seconds: int = window_seconds
        # bucket_id -> cms
        self.bucket_size = 10 # 10 seconds
        self.rolling_cms = RollingCMS(
            bucket_size=self.bucket_size, max_window_duration=window_seconds
        )

        self.global_lock = Lock()
        self.lock_stripes = [Lock() for _ in range(num_stripes)]

        self.K = K
        self.current_time = 0 # for easier testing purpose

        self.posts_since_cleanup = 0
        self.clean_every_n_posts = clean_every_n_posts

    def _get_hashtag_lock(self, hashtag: str) -> Lock:
        stripe_idx = hash(hashtag) % len(self.lock_stripes)
        return self.lock_stripes[stripe_idx]
        
    def _cleanup_old_data(self, cutoff_time: int) -> None:
        if not self.global_lock.acquire(blocking=False): # only 1 thread does cleanup
            return
        
        try:
            if not self.hashtag_timestamps:
                return

            for hashtag in list(self.hashtag_timestamps.keys()):
                lock: Lock = self._get_hashtag_lock(hashtag)
                with lock:
                    timestamps = self.hashtag_timestamps[hashtag]
                    idx = bisect_right(timestamps, cutoff_time)
                    if idx > 0:
                        self.hashtag_timestamps[hashtag] = timestamps[idx:] # include idx

                    if not self.hashtag_timestamps[hashtag]:
                        self.hashtag_timestamps.pop(hashtag, None)

            self.rolling_cms.cleanup_old_data(cutoff_time)

            self.posts_since_cleanup = 0
        finally:
            self.global_lock.release()

    def _update_global_heap(self, hashtag: str, count: int) -> None:
        with self.global_lock:

            if (
                len(self.top_k_global_heap) < self.K or # add to heap
                hashtag in self.top_k_global_heap       # update item in heap
            ):  
                self.top_k_global_heap[hashtag] = count

            else:
                _, smallest_count = self.top_k_global_heap.peekitem()
                if count > smallest_count:
                    _  = self.top_k_global_heap.popitem()
                    self.top_k_global_heap[hashtag] = count

    def _get_bucket_index(self, timestamp: int) -> int:
        return timestamp // self.bucket_size

    def record_post(self, hashtag: str, timestamp: int) -> None:
        """
        Record that a post with this hashtag was created at the given timestamp (seconds since epoch).
        """

        self.current_time = max(self.current_time, timestamp)
        with self._get_hashtag_lock(hashtag):
            self.hashtag_timestamps[hashtag].append(timestamp)

        # record in rolling cms
        count = self.rolling_cms.add(hashtag, timestamp)
        
        # update global heap
        self._update_global_heap(hashtag, count)

        # non blocking increment
        posts = self.posts_since_cleanup
        self.posts_since_cleanup += 1

        if posts >= self.clean_every_n_posts:
            self._cleanup_old_data(timestamp - self.window_seconds)

    def _count_timestamps_in_window(self, hashtag: str, cutoff_time: int) -> int:
        with self._get_hashtag_lock(hashtag):
            timestamps = self.hashtag_timestamps[hashtag]
            if not timestamps:
                return 0

            idx = bisect_right(timestamps, cutoff_time)
            return len(timestamps) - idx # exclude idx

    def get_top_k_global_fast(self) -> list[tuple[str, int]]:
        with self.global_lock:
            return sorted(self.top_k_global_heap.items(), key=lambda hashtag_count: -hashtag_count[1])

    def get_top_k_trending_slow(self, k: int, t: int) -> list[tuple[str, int]]:
        """
        Return the top k hashtags used in the last t seconds, ranked by frequency.
        """

        now = self.current_time
        cleanup_threshold = max(t, self.window_seconds)
        self._cleanup_old_data(now - cleanup_threshold)

        cutoff_time = now - t
        counts: defaultdict[str, int] = defaultdict(int)

        if not self.hashtag_timestamps:
            return []

        for hashtag in list(self.hashtag_timestamps.keys()):
            counts[hashtag] += self._count_timestamps_in_window(hashtag, cutoff_time)

        return heapq.nlargest(k, counts.items(), key=lambda hashtag_count: hashtag_count[1])

    def get_top_k_trending_approximate(self, k: int, t: int) -> list[tuple[str, int]]:
        now = self.current_time
        cutoff_time = now - t

        cleanup_threshold = max(t, self.window_seconds)
        self._cleanup_old_data(now - cleanup_threshold)
        if not self.hashtag_timestamps:
            return []

        hashtags = list(self.hashtag_timestamps.keys())
        approx_counts = self.rolling_cms.estimate_from(hashtags, cutoff_time, now)

        return heapq.nlargest(k, approx_counts.items(), key=lambda hashtag_count: hashtag_count[1])

    def get_post_rate(self, hashtag: str, t: int) -> float:
        now = self.current_time
        cleanup_threshold = max(t, self.window_seconds)
        self._cleanup_old_data(now - cleanup_threshold)

        cutoff_time = now - t

        posts_num = self._count_timestamps_in_window(hashtag, cutoff_time)
        return round(posts_num / t, 3)

def test_simple():
    tracker = TrendingTracker(K=1)
    tracker.record_post("#ai", 100)
    tracker.record_post("#ml", 110)
    tracker.record_post("#ai", 115)
    tracker.record_post("#go", 160)
    tracker.record_post("#ai", 400)

    print(tracker.get_top_k_trending_slow(2, 300))  # -> [("#ai", 2), ("#ml", 1)]
    print(tracker.get_top_k_trending_approximate(2, 300))  # -> [("#ai", >= 2), ("#ml", >=1)]
    print(tracker.get_top_k_global_fast())  # -> [("#ai", >=2)]
    print(tracker.get_post_rate("#ai", 4))

if __name__ == "__main__":
    test_simple()




            

