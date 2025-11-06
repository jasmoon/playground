# Event Stream Tracker with Mutable Events

# Design a system that tracks user events (like video views, likes, etc.) in real time, supports retroactive updates and deletions,
# and provides approximate analytics.

# APIs
# Implement the following methods:
# def record_event(self, user_id: str, event_type: str, timestamp: int) -> None:
#     """
#     Record that a user triggered an event (e.g. 'view', 'like') at the given timestamp (seconds).
#     """

# def update_event(self, user_id: str, old_timestamp: int, new_timestamp: int) -> bool:
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

class EventTracker:
    def record_event(self, user_id: str, event_type: str, timestamp: int) -> None:
        """
        Record that a user triggered an event (e.g. 'view', 'like') at the given timestamp (seconds).
        """

    def update_event(self, user_id: str, old_timestamp: int, new_timestamp: int) -> bool:
        """
        Update the timestamp of a previously recorded event.
        Return False if the old event doesn’t exist or is too old to update.
        """

    def delete_event(self, user_id: str, timestamp: int) -> bool:
        """
        Delete a previously recorded event.
        Return False if the event doesn’t exist or is outside the current tracking window.
        """

    def get_event_count(self, event_type: str, last_t_seconds: int) -> int:
        """
        Return the approximate number of events of the given type within the last T seconds.
        """

    def get_top_event_types(self, last_t_seconds: int, k: int) -> list[str]:
        """
        Return the top-k event types by frequency in the last T seconds.
         """