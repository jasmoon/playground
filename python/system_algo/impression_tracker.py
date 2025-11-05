# Problem: Real-Time Ad Impression Tracker

# System description
# - Advertisers buy ad slots and want to know how many impressions (views) their ads get over time, both globally and per region.
# - Your system should process a continuous stream of ad impressions and support real-time queries for statistics in sliding time windows.

# APIs
# record_impression(ad_id: str, region: str, timestamp: int)
# Record that an ad with ad_id was viewed by a user in the given region at the given timestamp (in seconds).

# get_impression_count(ad_id: str, last_t_seconds: int) -> int
# Return the total number of impressions for the given ad within the past last_t_seconds seconds (inclusive of current time).

# get_top_ads_by_region(region: str, last_t_seconds: int, k: int) -> list[str]
# Return the top k ads in that region based on the number of impressions within the last last_t_seconds seconds.

# Constraints
# - record_impression() will be called very frequently (up to 100k QPS globally).
# - get_* APIs will be called less frequently but must be low-latency (<100ms).
# - Impressions must expire automatically outside the time window.
# - The system must handle hundreds of ads and regions efficiently.
# - The solution should be thread-safe and allow concurrent updates.
# - Approximation is acceptable if it greatly improves performance.
from typing import Any


import heapq
from threading import Lock
import mmh3
import numpy as np

class CountMinSketch:
    def __init__(self,
        width: int = 1000,
        depth: int = 5,
    ) -> None:
        self.width = width
        self.depth = depth
        self.table = np.zeros((depth, width), dtype=np.uint64)
        self.seeds = [30*i for i in range(depth)]

        self.start_time = 0
        self.lock = Lock()

    def get_col_index(self, row_index: int, item: str):
        return mmh3.hash(item, self.seeds[row_index]) % self.width


    def add(self, item: str, start_time: int | None, count: int=1) -> None:
        with self.lock:
            if start_time and start_time < self.start_time:
                return
            if start_time and start_time > self.start_time:
                self._clear()
                self.start_time = start_time

            for i in range(self.depth):
                col_index = self.get_col_index(i, item)
                self.table[i, col_index] += count

    def minus(self, other: "CountMinSketch"):
        if self.width != other.width or self.depth != other.depth:
            return

        first, second = (self, other) if id(self) < id(other) else (other, self)
        with first.lock, second.lock:
            self.table -= other.table
            self.table = np.maximum(self.table, 0)

        # OR do the following which has
        # - extra memory allocation
        # - but shorter critical section (only 1 lock held at once)
        # with other.lock:
        #     other_copy = other.table.copy()
        
        # with self.lock:
        #     self.table -= other_copy
        #     self.table = np.maximum(self.table, 0)
        

    def estimate(self, item: str) -> int:
        with self.lock:
            candidates: list[int] = [
                self.table[i, self.get_col_index(i, item)]
                for i in range(self.depth)
            ]
        return min(candidates)

    def _clear(self):
        self.table.fill(0)

class RollingCMS:
    def __init__(self,
        window_seconds: int=24*60*60,
        bucket_size: int=30,
        width: int=1000,
        depth: int=5
    ) -> None:
        self.window_seconds = window_seconds
        self.bucket_size = bucket_size
        self.num_buckets = window_seconds // bucket_size + 1

        self.width = width
        self.depth = depth

        self.buckets = [CountMinSketch(width, depth) for _ in range(self.num_buckets)]
        self.merged = CountMinSketch(width, depth)

    def _get_start_time(self, timestamp: int):
        return timestamp // self.bucket_size * self.bucket_size

    def _get_bucket_index(self, start_time: int):
        return (start_time // self.bucket_size) % self.num_buckets

    def add(self, item: str, timestamp: int, count:int=1):
        start_time = self._get_start_time(timestamp)
        bucket_index = self._get_bucket_index(start_time)

        bucket = self.buckets[bucket_index]
        if bucket.start_time != 0 and start_time > bucket.start_time:
            self.merged.minus(bucket)

        bucket.add(item, start_time, count)
        self.merged.add(item, None, count)

    def estimate_from(self, item: str, now: int, cutoff: int):
        if now - cutoff >= self.window_seconds:
            return self.merged.estimate(item)

        cutoff_start_time = self._get_start_time(cutoff)
        now_start_time = self._get_start_time(now)
        # can actually pass cutoff and now directly to `_get_bucket_index`
        # but for exact consistency, better to use start_time
        cutoff_index = self._get_bucket_index(cutoff_start_time)
        now_index = self._get_bucket_index(now_start_time)

        total = 0
        while cutoff_index != (now_index + 1) % self.num_buckets:
            total += self.buckets[cutoff_index].estimate(item)
            cutoff_index = (cutoff_index + 1) % self.num_buckets
        return total

class ImpressionTracker:
    def __init__(self,
        window_seconds: int=24*60*60,
        bucket_size: int=30,
    ) -> None:
        self.window_seconds = window_seconds
        self.bucket_size = bucket_size

        self.region_cms: dict[str, RollingCMS] = {} # region -> ad id -> ad id
        self.region_ads: dict[str, set[str]] = {} # region -> set(ad_id)
        self.ad_cms = RollingCMS(self.window_seconds, self.bucket_size) # ad_id -> count
        self.ads = set()
        
        self.current_time = 0 # for easy testing
        self.global_lock = Lock()
        self.region_lock = {} # region -> Lock

    def _get_region_lock(self, region: str):
        with self.global_lock:
            if region not in self.region_lock:
                self.region_lock[region] = Lock()
            return self.region_lock[region]


    def record_impression(self, ad_id: str, region: str, timestamp: int):
        """
        Record that an ad with ad_id was viewed by a user in the given region at the given timestamp (in seconds).
        """
        with self._get_region_lock(region):
            if region not in self.region_cms:
                self.region_cms[region] = RollingCMS(self.window_seconds, self.bucket_size)
            if region not in self.region_ads:
                self.region_ads[region] = set()

            cms = self.region_cms[region]
            ad_ids = self.region_ads[region]
            ad_ids.add(ad_id)

        cms.add(ad_id, timestamp)
        self.ad_cms.add(ad_id, timestamp)

        with self.global_lock:
            self.ads.add(ad_id)
            self.current_time = max(self.current_time, timestamp)

    def get_impression_count(self, ad_id: str, last_t_seconds: int) -> int:
        """
        Return the total number of impressions for the given ad within the past last_t_seconds seconds (inclusive of current time).
        """
        with self.global_lock:
            if ad_id not in self.ads:
                return 0

            now = self.current_time
        last_t_seconds: int = min(self.window_seconds, last_t_seconds)
        cutoff = now - last_t_seconds

        return self.ad_cms.estimate_from(ad_id, now, cutoff)

        
    def get_top_ads_by_region(self, region: str, last_t_seconds: int, k: int) -> list[str]:
        """
        Return the top k ads in that region based on the number of impressions within the last last_t_seconds seconds.
        """
        with self._get_region_lock(region):
            if region not in self.region_cms:
                return []

            cms = self.region_cms[region]
            ad_ids = list(self.region_ads[region])

        with self.global_lock:
            now = self.current_time

        last_t_seconds: int = min(self.window_seconds, last_t_seconds)
        cutoff = now - last_t_seconds

        cnts: dict[str, int] = {}
        for ad_id in ad_ids:
            cnts[ad_id] = cms.estimate_from(ad_id, now, cutoff)

        return heapq.nlargest(
            k,
            cnts.keys(),
            key=lambda key: cnts[key],
        )
        