from dataclasses import dataclass
from enum import Enum
import heapq
from threading import Lock

"""
Design a distributed cache system that manages cached items across multiple regions, supports different eviction policies,
and provides usage analytics.
"""

@dataclass
class CacheRecord:
    value: str
    created_at: int
    expires_at: int
    last_access_time: int
    access_count: int = 0

class CacheOp(Enum):
    PUT = 1
    GET = 2
    DEL = 3

class MultiRegionCache:
    def __init__(self,
        num_locks=32,
        max_heap_size=100,
        recent_access_history_max_len=100,
    ) -> None:
        self.cache: dict[tuple[int, str], CacheRecord] = {}                                 # (region, key) -> CacheRecord
        self.recent_access_history: dict[tuple[int, str], list[tuple[int, CacheOp]]] = {}   # (region, key) -> min heap of (timestamp, op)

        self.region_ttl_heaps = {}          # region -> min heap of (expired_at, key)
        self.region_hot_keys = {}           # region -> min heap of (count, key)
        self.region_access_time_heaps = {}  # region -> min heap of (access time, key)

        # region -> {
        #   total_keys: int,
        #   total_size_bytes: int,
        #   hit_count: int,
        #   miss_count: int,
        #   eviction_count: int
        # }
        self.region_info = {}   

        self.num_locks = num_locks
        self.region_locks = [Lock() for _ in range(num_locks)]
        self.key_locks = [Lock() for _ in range(num_locks)]

        self.heap_size = max_heap_size
        self.recent_access_history_max_len = recent_access_history_max_len

    def _get_region_lock(self, region_id:int):
        return self.region_locks[hash(region_id) % self.num_locks]

    def _get_key_lock(self, region_id:int, key: str):
        return self.key_locks[hash((region_id, key)) % self.num_locks]

    def _maintain_key_recent_access_history(self, cache_key: tuple[int, str], timestamp: int, op: CacheOp):
        """
        this function assumes that the cache key lock has been acquired
        """
        history = self.recent_access_history.setdefault(cache_key, [])
        while history and len(history) >= self.recent_access_history_max_len:
            heapq.heappop(history)

        heapq.heappush(history, (timestamp, op))

    def put(self, key: str, value: str, region_id: int, ttl_seconds: int, timestamp: int) -> bool:
        """
        Store a key-value pair in the specified region with TTL.
        If key already exists in this region, update it.
        Returns True on success.
        Each put is logged in access history.
        """
        with self._get_key_lock(region_id, key):
            cache_key = (region_id, key)
            expires_at = timestamp + ttl_seconds
            diff_bytes = len(value.encode("utf-8"))
            record = self.cache.get(cache_key, None)
            if record:
                diff_bytes -= self._record_value_size_bytes(record)

            self.cache[cache_key] = CacheRecord(
                value, timestamp,
                expires_at, timestamp,
            )
            self._maintain_key_recent_access_history(cache_key, timestamp, CacheOp.PUT)

        with self._get_region_lock(region_id):
            ttl_min_heap = self.region_ttl_heaps.setdefault(region_id, [])
            heapq.heappush(ttl_min_heap, (expires_at, key))

        self._update_region_key_stats(
            region_id,
            0 if record else 1,
            diff_bytes)
        return True

    def _update_hot_keys(self, region_id: int, key: str, access_count: int):
        """
        this function assumes the region lock for `region_id` has be acquired.
        """
        if access_count <= 0:
            return

        hot_keys = self.region_hot_keys.setdefault(region_id, [])
        for i, (_, k) in enumerate(hot_keys):
            if k == key:
                hot_keys[i] = (access_count, k)
                heapq.heapify(hot_keys)
                return

        if len(hot_keys) < self.heap_size:
            heapq.heappush(hot_keys, (access_count, key))
        else:
            if hot_keys[0][0] < access_count:
                heapq.heapreplace(hot_keys, (access_count, key))
    
    def _inc_region_access_stats(self, region_id: int, field: str):
        with self._get_region_lock(region_id):
            region_info = self.region_info.setdefault(region_id, {})
            self.region_info[region_id][field] = (
                region_info.get(field, 0) +
                1
            )


    def get(self, key: str, region_id: int, timestamp: int) -> str | None:
        """
        Retrieve value from the specified region.
        Returns None if key doesn't exist or has expired.
        Updates last_access_time for LRU tracking.
        Logs the access in history.
        """
        access_count = 0
        hit = False
        with self._get_key_lock(region_id, key):
            cache_key = (region_id, key)
            record = self.cache.get(cache_key, None)
            if record and timestamp < record.expires_at:
                record.last_access_time = timestamp
                value = record.value

                access_count = record.access_count + 1
                record.access_count = access_count
                self._maintain_key_recent_access_history(cache_key, timestamp, CacheOp.GET)
                hit = True

        if not hit:
            with self._get_region_lock(region_id):
                region_info = self.region_info.setdefault(region_id, {})
                self.region_info[region_id]["miss_count"] = (
                    region_info.get("miss_count", 0) + 1
                )
                return None

        with self._get_region_lock(region_id):
            access_time_heap = self.region_access_time_heaps.setdefault(region_id, [])
            heapq.heappush(access_time_heap, (timestamp, key))
        
            self._update_hot_keys(region_id, key, access_count)
            region_info = self.region_info.setdefault(region_id, {})
            self.region_info[region_id]["hit_count"] = (
                region_info.get("hit_count", 0) + 1
            )

        return value

    def _update_region_key_stats(self,
        region_id: int,
        total_keys_diff: int,
        total_size_bytes_diff: int,
        evicted: int=0
    ):
        """
        Note if you want to deduct by `total_keys_diff`, it should be a negative integer.
        Same for `total_size_bytes_diff`.
        `evicted` is expected to be a positive integer
        """

        with self._get_region_lock(region_id):
            region_info = self.region_info.setdefault(region_id, {})

            if total_keys_diff != 0:
                total_keys = (
                    region_info.get("total_keys", 0) +
                    total_keys_diff
                )
                self.region_info[region_id]["total_keys"] = max(
                    0,
                    total_keys,
                )

            if total_size_bytes_diff != 0:
                total_size_bytes = (
                    region_info.get("total_size_bytes", 0) + 
                    total_size_bytes_diff
                )
                self.region_info[region_id]["total_size_bytes"] = max(
                    0,
                    total_size_bytes,
                )

            if evicted > 0:
                self.region_info[region_id]["eviction_count"] = (
                    region_info.get("eviction_count", 0) +
                    evicted
                )
    
    def _record_value_size_bytes(self, record: CacheRecord) -> int:
        return len(record.value.encode("utf-8"))

    def delete(self, key: str, region_id: int, timestamp: int) -> bool:
        """
        Delete a key from the specified region.
        Returns False if key doesn't exist.
        """
        with self._get_key_lock(region_id, key):
            cache_key = (region_id, key)
            if cache_key not in self.cache:
                return False

            size_bytes = self._record_value_size_bytes(self.cache[cache_key])
            self.cache.pop(cache_key, None)
            self._maintain_key_recent_access_history(cache_key, timestamp, op=CacheOp.DEL)

        self._update_region_key_stats(
            region_id,
            -1,
            -size_bytes)

        return True

    def replicate(self, key: str, from_region: int, to_region: int, timestamp: int) -> bool:
        """
        Copy a key-value pair from one region to another.
        Returns False if key doesn't exist in source region or has expired.
        The replica inherits the same TTL (time remaining).
        """
        if from_region == to_region:
            return False

        from_key = (from_region, key)
        with self._get_key_lock(from_region, key):
            if from_key not in self.cache:
                return False

            record = self.cache[from_key]
            if timestamp >= record.expires_at:
                return False

            value = record.value
            remaining_ttl = record.expires_at - timestamp

        self.put(
            key, value, to_region,
            remaining_ttl, timestamp)
        return True
    
    def evict_expired(self, region_id: int, timestamp: int) -> int:
        """
        Remove all expired keys from the region.
        Returns count of keys evicted.
        """
        with self._get_region_lock(region_id):
            ttl_min_heap = self.region_ttl_heaps.get(region_id, [])

            candidates = []
            while ttl_min_heap and timestamp >= ttl_min_heap[0][0]:
                candidates.append(heapq.heappop(ttl_min_heap))

            if not ttl_min_heap:
                self.region_ttl_heaps.pop(region_id, None)

        evicted = 0
        size_bytes_evicted = 0
        for _, key in candidates:
            cache_key = (region_id, key)
            with self._get_key_lock(region_id, key):
                if cache_key in self.cache and timestamp >= self.cache[cache_key].expires_at:
                    size_bytes_evicted += self._record_value_size_bytes(self.cache[cache_key])
                    self.cache.pop(cache_key, None)
                    self._maintain_key_recent_access_history(cache_key, timestamp, op=CacheOp.DEL)
                    evicted += 1


        self._update_region_key_stats(
            region_id,
            -evicted,
            -size_bytes_evicted,
            evicted
        )

        return evicted

    def evict_lru(self, region_id: int, count: int, timestamp: int) -> int:
        """
        Evict 'count' least recently used keys from the region.
        Returns actual number of keys evicted.
        The current implementation is not optimal.
        The ideal implementation involves the usage of doubly linked list.
        """
        attempts = 0
        evicted = 0
        size_bytes_evicted = 0
        max_attempts = count * 2

        while evicted < count and attempts < max_attempts:
            with self._get_region_lock(region_id):
                access_time_heap = self.region_access_time_heaps.get(region_id, [])
                if not access_time_heap:
                    break

                candidates = []
                while (
                    len(candidates) < count - evicted and # at most count - evicted candidates
                    access_time_heap
                ):
                    candidates.append(heapq.heappop(access_time_heap))

            if not candidates:
                break

            for access_time, key in candidates:
                cache_key = (region_id, key)
                with self._get_key_lock(region_id, key):
                    if cache_key in self.cache and self.cache[cache_key].last_access_time <= access_time:
                        size_bytes_evicted += self._record_value_size_bytes(self.cache[cache_key])
                        self.cache.pop(cache_key, None)
                        self._maintain_key_recent_access_history(cache_key, timestamp, op=CacheOp.DEL)
                        evicted += 1
                    else:
                        # stale entry found, simply ignore it
                        pass

            attempts += 1
            if evicted >= count:
                break

        self._update_region_key_stats(
            region_id,
            -evicted,
            -size_bytes_evicted,
            evicted
        )

        return evicted
            
    def get_region_stats(self, region_id: int) -> dict[str, int] | None:
        """
        Return {
            'total_keys': int,
            'total_size_bytes': int,  # sum of len(value)
            'hit_count': int,
            'miss_count': int,
            'eviction_count': int
        }
        """
        with self._get_region_lock(region_id):
            region_info = self.region_info.get(region_id, {})
            return {
                'total_keys': region_info.get('total_keys', 0),
                'total_size_bytes': region_info.get('total_size_bytes', 0),
                'hit_count': region_info.get('hit_count', 0),
                'miss_count': region_info.get('miss_count', 0),
                'eviction_count': region_info.get('eviction_count', 0)
            }

    def get_key_info(self, key: str, region_id: int, timestamp: int) -> dict[str, int | str] | None:
        """
        Return {
            'value': str,
            'created_at': int,
            'expires_at': int,
            'last_accessed': int,
            'access_count': int,
            'size_bytes': int
        }
        Returns None if key doesn't exist or expired.
        """
        cache_key = (region_id, key)
        with self._get_key_lock(region_id, key):
            if cache_key not in self.cache or timestamp >= self.cache[cache_key].expires_at:
                return None

            record = self.cache[cache_key]
            return {
                "value": record.value,
                "created_at": record.created_at,
                "expires_at": record.expires_at,
                "last_accessed": record.last_access_time,
                "access_count": record.access_count,
                "size_bytes": self._record_value_size_bytes(record)
            }

    def get_hot_keys(self, region_id: int, k: int) -> list[str]:
        """
        Return top-k most frequently accessed keys in the region.
        """
        with self._get_region_lock(region_id):
            hot_keys = list(self.region_hot_keys.get(region_id, [])) # make a copy

        # skip locking, locking is expensive or simply don't check for existence
        try:
            valid_keys = [(count, key) for (count, key) in hot_keys if (region_id, key) in self.cache]
            return [key for _, key in heapq.nlargest(k, valid_keys)]
        except RuntimeError:
            return [key for _, key in heapq.nlargest(k, hot_keys)]

    def get_expiring_soon(self, region_id: int, within_seconds: int, timestamp: int) -> list[str]:
        """
        Return keys that will expire within the next 'within_seconds' seconds.
        """
        cutoff_time = timestamp + within_seconds
        with self._get_region_lock(region_id):
            ttl_min_heap = list(self.region_ttl_heaps.get(region_id, []))
            
        candidates = [(expired_at, key) for expired_at, key in ttl_min_heap if expired_at <= cutoff_time]

        expiring_soon = []
        for _, key in candidates:
            cache_key = (region_id, key)
            with self._get_key_lock(region_id, key):
                if cache_key in self.cache and cutoff_time >= self.cache[cache_key].expires_at:
                    expiring_soon.append(key)
        return expiring_soon

    def get_global_key_locations(self, key: str, timestamp: int) -> list[int]:
        """
        Return all region_ids where this key exists (and hasn't expired).
        """
        region_ids = list(self.region_ttl_heaps)

        exist_in_regions = []
        for region_id in region_ids:
            cache_key = (region_id, key)
            with self._get_key_lock(region_id, key):
                if cache_key in self.cache and timestamp < self.cache[cache_key].expires_at:
                    exist_in_regions.append(region_id)

        return exist_in_regions


    def get_region_size_ranking(self, k: int) -> list[int]:
        """
        Return top-k regions by total_size_bytes.
        regions with 0 bytes are excluded from the response
        """
        region_ids = list(self.region_info)
        res = []
        for region_id in region_ids:
            with self._get_region_lock(region_id):
                res.append(
                    (self.region_info.get(region_id, {}).get("total_size_bytes", 0), region_id)
                )

        return [
            region_id
            for size_bytes, region_id in heapq.nlargest(k, res) if size_bytes > 0
        ]

    def get_access_history(self, key: str, region_id: int, limit: int) -> list[tuple[int, CacheOp,]]:
        """
        Return recent access history for a key:
        (timestamp, operation, details)
        Keep last 100 operations per key.
        """
        cache_key = (region_id, key)
        with self._get_key_lock(region_id, key):
            return sorted(
                self.recent_access_history.get(cache_key, [])
            )[:limit]