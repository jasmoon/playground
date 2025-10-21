from typing import Any


from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass
import heapq

# Practice Problem: Real-Time Video Watch Tracker
# Problem Statement
# You are tasked with designing a system to track video views on a video platform in real-time. Users can watch multiple videos, and you need to efficiently support the following operations:
# 1. Record a video watch:
#    * record_watch(user_id: str, video_id: str, timestamp: int)
# 2. Get the top k most watched videos in the last T seconds:
#    * get_top_videos(T: int, k: int) -> List[str]
# 3. Get the number of unique videos watched by a user in the last T seconds:
#    * get_unique_videos(user_id: str, T: int) -> int
# Constraints
# * The system should handle high write throughput (many record_watch events per second).
# * All timestamps are given as seconds since epoch.

# The solution below makes use of BUCKETS to answer queries in a faster manner.

@dataclass
class VideoWatchEvent:
    id: str
    user_id: str
    video_id: str
    timestamp: int


class VideoWatchTracker:

    def __init__(self, bucket_size: int = 60, window_seconds: int=24 * 60 * 60) -> None:
        # self.storage = {}
        # self.watch_events = deque()

        self.videos_watches: defaultdict[str, list[int]] = defaultdict(list) # vid -> timestamps
        self.users_watches: defaultdict[str, defaultdict[str, int]] = defaultdict(lambda: defaultdict(int)) # uid -> vid -> last_watched_time

        # bucket_id -> {video_id -> {'count': int, 'timestamps': sorted list}}
        self.bucket_size = bucket_size
        self.video_buckets: defaultdict[int, defaultdict[str, dict[str, int | list[int]]]] = defaultdict(lambda: defaultdict(lambda: {'count': 0, 'timestamps': []}))
        self.active_buckets: set[int] = set() # track which buckets have data
        # Track current time for efficient cleanup
        self.current_time = 0
        self.window_seconds = window_seconds

    def _get_bucket_id(self, timestamp: int):
        return timestamp // self.bucket_size

    def _cleanup_old_data(self, cutoff_time: int):
        cutoff_bucket_id = self._get_bucket_id(cutoff_time)
        expired_buckets = [b for b in self.active_buckets if b < cutoff_bucket_id]
        for bucket_id in expired_buckets:
            del self.video_buckets[bucket_id]
            self.active_buckets.discard(bucket_id)

        for video_id in list(self.videos_watches.keys()):
            watches = self.videos_watches[video_id]

            idx = bisect_right(watches, cutoff_time)
            if idx > 0:
                self.videos_watches[video_id] = watches[idx:]

            if not self.videos_watches[video_id]:
                del self.videos_watches[video_id]

        for user_id in list(self.users_watches.keys()):
            expired_videos = [
                vid for vid, ts in self.users_watches[user_id].items()
                if ts <= cutoff_time
            ]
            for vid in expired_videos:
                del self.users_watches[user_id][vid]

            if not self.users_watches[user_id]:
                del self.users_watches[user_id]

    def _count_watches_in_window(self, video_id: str, cutoff_time: int) -> int:
        """
        Count watches for a video after cutoff_time using binary search.
        Time Complexity: O(log W) where W is total watches for video
        """
        watches = self.videos_watches[video_id]
        if not watches:
            return 0
        
        # Binary search to find first timestamp > cutoff_time
        idx = bisect_right(watches, cutoff_time)
        return len(watches) - idx

    def record_watch(self, user_id: str, video_id: str, timestamp: int):
        """
        Record a video watch event. Assumes that timestamp is always received in order.
        Time Complexity: O(1) where W is watches for this video
        """
        self.current_time = max(self.current_time, timestamp)
        # self.watch_events.append((timestamp, user_id, video_id))

        self.videos_watches[video_id].append(timestamp) # assuming that timestamp is always received in order
        self.users_watches[user_id][video_id] = timestamp

        # Update time bucket with both count and timestamp
        bucket_id = self._get_bucket_id(timestamp)
        bucket_data = self.video_buckets[bucket_id][video_id]
        bucket_data['count'] += 1
        bucket_data['timestamps'].append(timestamp)
        self.active_buckets.add(bucket_id)

    def get_top_videos(self, t: int, k: int) -> list[str]:
        """
        Get top k most watched videos in the last T seconds.
        Uses time bucketing for fast aggregation when T is large.
        
        Time Complexity: 
        - Bucket approach: O(B*V + E*V*log(W_b)) where:
          * B = buckets in window, V = videos
          * E = edge buckets (2), W_b = avg watches per bucket (much smaller than total W)
          * Middle buckets use O(1) aggregation, only edges need binary search
        - Binary search approach: O(V*log(W)) where W = total watches per video
        
        The benefit: With bucketing, we binary search small lists (W_b << W) only at edges,
        while middle buckets use pre-aggregated counts. This is much faster for large T.
        """
        now = self.current_time # int(time.time())
        cutoff_time = now - t

        cleanup_threshold = max(t, self.window_seconds)
        self._cleanup_old_data(cutoff_time=now - cleanup_threshold)

        # Decide strategy based on number of buckets in the time window
        # Use bucketing when we have at least 3 full buckets to amortize the boundary overhead
        num_buckets_in_window = (t // self.bucket_size) + 1

        if num_buckets_in_window >= 3:
            return self._get_top_videos_bucketed(cutoff_time, k)
        else:
            return self._get_top_videos_binary_search(cutoff_time, k)

    def _get_top_videos_bucketed(self, cutoff_time: int, k: int) -> list[str]:
        cutoff_bucket = self._get_bucket_id(cutoff_time)
        current_bucket = self._get_bucket_id(self.current_time) # for easy testing

        video_counts: defaultdict[str, int] = defaultdict(int)

        for bucket_id in range(cutoff_bucket, current_bucket + 1):
            if bucket_id not in self.active_buckets:
                continue

            for video_id, bucket_data in self.video_buckets[bucket_id].items():
                if bucket_id == cutoff_bucket:
                    timestamps= bucket_data["timestamps"]
                    idx = bisect_right(timestamps, cutoff_time)
                    video_counts[video_id] += len(timestamps) - idx
                else:
                    video_counts[video_id] += bucket_data["count"]

        if not video_counts:
            return []

        top_k = heapq.nlargest(k, video_counts.items(), key=lambda video_count: video_count[1])
        return [video_id for video_id, _ in top_k]

    def _get_top_videos_binary_search(self, cutoff_time: int, k: int):
        video_counts = []
        for video_id in self.videos_watches.keys():
            count = self._count_watches_in_window(video_id, cutoff_time)
            if count > 0:
                video_counts.append((count, video_id))

        if not video_counts:
            return []

        top_k = heapq.nlargest(k, video_counts)
        return [video_id for _, video_id in top_k]

    def get_unique_videos(self, user_id: str, t: int) -> int:
        """
        Get number of unique videos watched by user in last T seconds.
        Time Complexity: O(V_u) where V_u is unique videos watched by user
        """
        if user_id not in self.users_watches:
            return 0

        now = self.current_time # int(time.time())
        cutoff_time = now - t

        self._cleanup_old_data(now - self.window_seconds)

        unique_count = sum(
            1
            for ts in self.users_watches[user_id].values()
            if ts > cutoff_time
        )

        return unique_count


# Example usage and testing
if __name__ == "__main__":
    import random
    import time as time_module
    
    print("=== Basic Functionality Test ===")
    tracker = VideoWatchTracker(bucket_size=60)
    
    # Simulate watch events
    base_time = 1000
    tracker.record_watch("user1", "video1", base_time)
    tracker.record_watch("user1", "video2", base_time + 1)
    tracker.record_watch("user2", "video1", base_time + 2)
    tracker.record_watch("user3", "video1", base_time + 3)
    tracker.record_watch("user1", "video1", base_time + 4)  # user1 watches video1 again
    tracker.record_watch("user2", "video3", base_time + 5)
    tracker.record_watch("user4", "video2", base_time + 100)
    tracker.record_watch("user5", "video1", base_time + 101)
    
    print("\nTop 3 videos in last 200 seconds:")
    print(tracker.get_top_videos(200, 3))
    
    print("\nTop 2 videos in last 10 seconds:")
    print(tracker.get_top_videos(10, 2))
    
    print("\nUnique videos watched by user1 in last 200 seconds:")
    print(tracker.get_unique_videos("user1", 200))
    
    print("\nUnique videos watched by user1 in last 5 seconds:")
    print(tracker.get_unique_videos("user1", 5))
    
    # Performance test
    print("\n=== Performance Test ===")
    tracker2 = VideoWatchTracker(bucket_size=60)
    
    # Generate 100k watch events
    num_events = 100000
    num_users = 10000
    num_videos = 1000
    
    print(f"Generating {num_events} watch events...")
    start = time_module.time()
    
    for i in range(num_events):
        user_id = f"user{random.randint(1, num_users)}"
        video_id = f"video{random.randint(1, num_videos)}"
        timestamp = 1000 + i
        tracker2.record_watch(user_id, video_id, timestamp)
    
    record_time = time_module.time() - start
    print(f"Recorded {num_events} events in {record_time:.2f}s ({num_events/record_time:.0f} events/sec)")
    
    # Query performance
    print("\nQuery performance:")
    start = time_module.time()
    top_videos = tracker2.get_top_videos(3600, 10)  # Last hour
    query_time = time_module.time() - start
    print(f"get_top_videos(T=3600, k=10): {query_time*1000:.2f}ms")
    print(f"Top 5 videos: {top_videos[:5]}")
    
    start = time_module.time()
    unique = tracker2.get_unique_videos("user1", 3600)
    query_time = time_module.time() - start
    print(f"get_unique_videos(T=3600): {query_time*1000:.2f}ms")
    print(f"Unique videos: {unique}")

        