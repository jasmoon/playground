# Event Stream Tracker with Mutable Events

# Design a system that tracks user events (like video views, likes, etc.) in real time, supports retroactive updates and deletions,
# and provides approximate analytics.

# APIs
# Part 1, implement the following methods:
# def record_event(self, user_id: str, event_type: str, timestamp: int) -> None:
#     """
#     Record that a user triggered an event (e.g. 'view', 'like') at the given timestamp (seconds).
#     """

# def update_event_timestamp(self, user_id: str, old_timestamp: int, new_timestamp: int) -> bool:
#     """
#     Update the timestamp of a previously recorded event.
#     Return False if the old event doesn’t exist or is too old to update.
#     """

# def delete_event(self, user_id: str, timestamp: int) -> bool:
#     """
#     Delete a previously recorded event.
#     Return False if the event doesn’t exist or is outside the current tracking window.
#     """

# def get_event_count(self, event_type: str, last_t_seconds: int) -> int:
#     """
#     Return the approximate number of events of the given type within the last T seconds.
#     """

# def get_top_event_types(self, last_t_seconds: int, k: int) -> list[str]:
#     """
#     Return the top-k event types by frequency in the last T seconds.
#     """

# System Constraints
# - Time-based rolling window: You only need to keep the last 24 * 60 * 60 seconds (24h) of data.
# - Memory optimization: You can’t keep all events in memory — use an approximation data structure for counting.
# - Mutable events
#   - Events can arrive out of order (timestamp smaller than current time).
#   - Events can be updated or deleted, which means your data structure must support subtracting counts or invalidating buckets.
# - Analytics must remain monotonic within each bucket
# - When an event is deleted or moved, the aggregate for that bucket must decrease accordingly.
# - Concurrency awareness (for extra credit): Discuss or design where locks would be placed if this were a multi-threaded tracker.

# Part 2, implement the following APIs:


# def delete_user(self, user_id: str) -> bool:
#     """
#     Delete an existing user and all user related data.
#     Return False if the user doesn’t exist.
#     """

# def get_top_active_users(self, k: int):
#     """
#     Return the top-k users by frequency of events
#     """

# def get_top_active_users_by_event_type(self, event_type: str, k: int):
#     """
#     Return the top-k users by frequency of event of event type
#     """
from dataclasses import dataclass
import heapq
from threading import Lock

@dataclass
class Event:
    event_id: str
    user_id: str
    event_type: str
    timestamp: int

class AtomicBucket:
    def __init__(self) -> None:
        self.count = 0
        self.start_time = 0
        self.lock = Lock()

    def add(self, start_time: int, count:int = 1):
        with self.lock:
            if start_time < self.start_time:
                return
            elif start_time > self.start_time:
                self.start_time = start_time
                self.count = count
            elif start_time == self.start_time:
                self.count += count

    def minus(self, start_time: int, count:int =1):
        with self.lock:
            if start_time < self.start_time:
                return
            elif start_time == self.start_time:
                self.count = max(self.count - count, 0)

    def read(self):
        with self.lock:
            return (self.count, self.start_time)

class RingBuffer:
    def __init__(self,
        window_seconds: int=24*60*60,
        bucket_size:int = 10,
    ) -> None:
        self.window_seconds = window_seconds
        self.bucket_size = bucket_size
        self.num_buckets = (window_seconds // bucket_size) + 1

        self.buckets = [AtomicBucket() for _ in range(self.num_buckets)]

    def _get_start_time(self, timestamp: int):
        return (timestamp // self.bucket_size) * self.bucket_size

    def _get_bucket_index(self, timestamp: int):
        return (timestamp // self.bucket_size) % self.num_buckets

    def add(self, timestamp: int, count: int=1):
        start_time = self._get_start_time(timestamp)
        bucket_index = self._get_bucket_index(timestamp)

        bucket = self.buckets[bucket_index]
        bucket.add(start_time, count)

    def minus(self, timestamp: int, count: int=1):
        start_time = self._get_start_time(timestamp)
        bucket_index = self._get_bucket_index(timestamp)

        bucket = self.buckets[bucket_index]
        bucket.minus(start_time, count)

    def total_from(self, now: int, cutoff: int):
        now_index = self._get_bucket_index(now)
        cutoff_index = self._get_bucket_index(cutoff)
        cutoff_start_time = self._get_start_time(cutoff)

        visited = 0
        total = 0
        while cutoff_index != (now_index + 1) % self.num_buckets and visited < self.num_buckets:
            bucket = self.buckets[cutoff_index]
            count, start_time = bucket.read()
            if count > 0 and start_time >= cutoff_start_time:
                total += count
            cutoff_index = (cutoff_index + 1) % self.num_buckets
            visited += 1
        return total


class EventTracker:
    def __init__(self,
        window_seconds: int=24*60*60,
        bucket_size:int = 10,
    ) -> None:
        self.window_seconds = window_seconds
        self.bucket_size = bucket_size

        # user related data structures
        self.users: dict[str, set[str]] = {} # user id -> set of event ids

        # event related data structures
        self.events: dict[str, Event] = {} # event id -> Event
        self.event_type_rbs: dict[str, RingBuffer] = {} # event type -> rb -> count
        self.event_type_user_event_counts: dict[str, dict[str, int]] = {} # event type -> user id -> count
        self.event_types: set[str] = set()

        self.global_lock = Lock()
        self.current_time = 0 # for easy testing
        

    def record_event(self, event_id: str, user_id: str, event_type: str, timestamp: int) -> None:
        """
        Record that a user triggered an event (e.g. 'view', 'like') at the given timestamp (seconds).
        """
        event = Event(
            event_id, user_id,
            event_type, timestamp
        )

        with self.global_lock:
            if event_id in self.events: # ignore duplicate events
                return
                
            self.events[event_id] = event
            rb = self.event_type_rbs.setdefault(event_type, RingBuffer(self.window_seconds, self.bucket_size))
            self.event_types.add(event_type)

            user_events = self.users.setdefault(user_id, set())
            new_user_event = event_id not in user_events
            if new_user_event:
                user_events.add(event_id)

            _ = self.event_type_user_event_counts.setdefault(event_type, {}).setdefault(user_id, 0)
            if new_user_event:
                self.event_type_user_event_counts[event_type][user_id] += 1

            self.current_time = max(self.current_time, timestamp)

        rb.add(timestamp)

    def update_event_timestamp(self, event_id: str, user_id: str, new_timestamp: int) -> bool:
        """
        Update the timestamp of a previously recorded event.
        Return False if the old event doesn’t exist or is too old to update.
        How far new_timestamp can be into the future?
        """
        with self.global_lock:
            if event_id not in self.events:
                return False

            now = self.current_time
            event = self.events[event_id]
            rb = self.event_type_rbs[event.event_type]
            old_timestamp = event.timestamp
            event.timestamp = new_timestamp
            self.current_time = max(self.current_time, new_timestamp)

        if old_timestamp < now - self.window_seconds:
            return False
        
        rb.minus(old_timestamp)
        rb.add(new_timestamp)

        return True

    def delete_event(self, event_id: str) -> bool:
        """
        Delete a previously recorded event.
        Return False if the event doesn’t exist or is outside the current tracking window.
        If event's timestamp lies outside the current tracking window
        - Count in the ring buffer's will not be adjusted
        - Event metadata will still be deleted
        """
        with self.global_lock:
            if event_id not in self.events:
                return False

            now = self.current_time
            event = self.events[event_id]
            old_timestamp = event.timestamp
            rb = self.event_type_rbs[event.event_type]
            del self.events[event_id]

            user_id = event.user_id
            self.users[event.user_id].discard(event_id)
            if event.event_type in self.event_type_user_event_counts:
                user_event_counts = self.event_type_user_event_counts[event.event_type]
                if user_id in user_event_counts:
                    user_event_counts[user_id] = max(0, user_event_counts[user_id]-1)

        if old_timestamp < now - self.window_seconds:
            return False

        rb.minus(old_timestamp)
        return True

    def delete_user(self, user_id: str) -> bool:
        """
        Delete an existing user and all user related data.
        Return False if the user doesn’t exist.
        """
        with self.global_lock:
            if user_id not in self.users:
                return False
                
            now = self.current_time
            cutoff = now - self.window_seconds
            user_events = self.users.pop(user_id, set())
            events = [self.events.pop(event_id, None) for event_id in user_events]

        event_types: set[str] = set()
        events = [event for event in events if event is not None]
        for event in events:
            rb = self.event_type_rbs.get(event.event_type)
            if rb and event.timestamp >= cutoff:
                rb.minus(event.timestamp, 1)
            event_types.add(event.event_type)

        with self.global_lock:
            for event_type in event_types:
                if event_type in self.event_type_user_event_counts:
                    self.event_type_user_event_counts[event_type].pop(user_id, None)
                if not self.event_type_user_event_counts[event_type]:
                    self.event_type_user_event_counts.pop(event_type, None)
                    self.event_type_rbs.pop(event_type, None)
                    self.event_types.discard(event_type)
        return True
                    
    def get_event_count(self, event_type: str, last_t_seconds: int) -> int:
        """
        Return the approximate number of events of the given type within the last T seconds.
        """
        now = self.current_time
        last_t_seconds = min(self.window_seconds, last_t_seconds)
        cutoff = now - last_t_seconds

        rb = self.event_type_rbs[event_type]
        return rb.total_from(now, cutoff)

    def get_top_event_types(self, last_t_seconds: int, k: int) -> list[str]:
        """
        Return the top-k event types by frequency in the last T seconds.
        """
        with self.global_lock:
            now = self.current_time
            event_type_rbs = dict(self.event_type_rbs)

        last_t_seconds = min(self.window_seconds, last_t_seconds)
        cutoff = now - last_t_seconds

        top_k = heapq.nlargest(
            k,
            event_type_rbs.items(),
            key=lambda event_type_rb: event_type_rb[1].total_from(now, cutoff),
        )

        return [event_type for event_type, _ in top_k]

    def get_global_top_active_users(self, k: int) -> list[str]:
        """
        Return the top-k users by frequency of events
        """
        with self.global_lock:
            user_events = dict(self.users)

        return heapq.nlargest(
            k,
            user_events.keys(),
            key=lambda user_id: len(user_events[user_id]),
        )
        

    def get_global_top_active_users_by_event_type(self, event_type: str, k: int) -> list[str]:
        """
        Return the top-k users by frequency of event of event type
        """
        with self.global_lock:
            if event_type not in self.event_type_user_event_counts:
                return []
            user_event_counts: dict[str, int] = dict(self.event_type_user_event_counts[event_type])

        return heapq.nlargest(
            k,
            user_event_counts.keys(),
            key=lambda user_id: user_event_counts[user_id],
        )
