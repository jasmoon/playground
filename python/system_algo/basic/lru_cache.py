from collections import OrderedDict
from concurrent.futures import thread
from typing import Dict
import heapq
import time
import threading

class Node:
    def __init__(self, key: int, value: int, ttl: float):
        self.key = key
        self.value = value
        self.expiry = time.time() + ttl
        self.prev = None
        self.next = None
        self.last_access = time.time()

class LRUCache:
    def __init__(self, capacity: int, cleanup_interval: float=5.0):
        self.capacity = capacity
        self.cache = {} # key -> Node

        self.head = Node(0, 0, float('inf'))
        self.tail = Node(0, 0, float('inf'))
        self.head.next = self.tail
        self.tail.prev = self.head

        self.lock = threading.RLock()
        self.cleanup_interval = cleanup_interval

    def get(self, key: int) -> int:
        with self.lock:
            if key in self.cache:
                node = self.cache[key]
                now = time.time()
                if now >= node.expiry: # for thread safe implementation
                    self._remove(node)
                    del self.cache[key]
                    return -1
                node.last_access = now
                self._remove(node)
                self._insert_to_front(node)
                return node.value
        return -1

    def put(self, key: int, value: int, ttl: float) -> None:
        # this step of cleaning up expired entries is optional
        expired_keys = []
        now = time.time()
        for k, node in self.cache.items(): # do not modify cache here, will lead to runtime errors
            if now >= node.expiry:
                expired_keys.append(k)

        for k in expired_keys:
            self._remove(self.cache[k])
            del self.cache[k]

        with self.lock:
            if key in self.cache:
                self._remove(self.cache[key])
            node = Node(key, value, ttl)
            self.cache[key] = node
            self._insert_to_front(node)

            if len(self.cache) > self.capacity:
                lru = self.tail.prev
                self._remove(lru)
                del self.cache[lru.key]

    def _remove(self, node):
        prev = node.prev
        nxt = node.next
        prev.next = nxt
        nxt.prev = prev

    def _insert_to_front(self, node):
        node.prev = self.head
        node.next = self.head.next
        self.head.next.prev = node
        self.head.next = node

    def _start_cleaner_thread(self):
        def cleaner():
            while True:
                time.sleep(self.cleanup_interval)
                expired_keys = []
                now = time.time()
                with self.lock:
                    for k, node in self.cache.items(): # do not modify cache here, will lead to runtime errors
                        if now >= node.expiry:
                            expired_keys.append(k)

                    for k in expired_keys:
                        self._remove(self.cache[k])
                        del self.cache[k]
        
        t = threading.Thread(target=cleaner, daemon=True)
        t.start()

    def get_cache_stats(self) -> Dict:
        with self.lock:
            now = time.time()
            total = expired = ttl_sum = access_age_sum = 0

            for node in self.cache.values():
                if now >= node.expiry:
                    expired += 1
                else:
                    ttl_remaining = node.expiry - now
                    ttl_sum += ttl_remaining
                    access_age = now - node.last_access
                    access_age_sum += access_age
                    total += 1

            avg_ttl_remaining = ttl_sum / total if total else 0
            avg_access_age = access_age_sum / total if total else 0

            return {
                'live_keys': total,
                'expired_keys': expired,
                'average_ttl_remaining': round(avg_ttl_remaining, 2),
                'average_last_access_age': round(avg_access_age, 2),
                'total_capacity': self.capacity,
            }



class LRUCacheOrderedDict:
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.cache = OrderedDict() # key -> (value, expiry_timestamp)
        self.ttl_heap = [] # (expiry_timestamp, key)
        self.lock = threading.RLock()

    def _evict_expired(self):
        now = time.time()
        while self.ttl_heap:
            expiry, key = self.ttl_heap[0]
            if expiry > now:
                break

            heapq.heappop(self.ttl_heap)
            if key in self.cache:
                _, expiry = self.cache[key]
                if expiry >= now:
                    del self.cache[key]
                    
    def get(self, key:int) -> int:
        with self.lock:
            self._evict_expired()

            item = self.cache.get(key)
            if not item:
                return -1
            
            value, expiry = item
            if time.time() >= expiry:
                del self.cache[key]
                return -1
            
            self.cache.move_to_end(key, last=True)
            return value
        
    def put(self, key: int, value: int, ttl: float) -> None:
        with self.lock:
            self._evict_expired()

            now = time.time()

            if key in self.cache:
                del self.cache[key]

            self.cache[key] = (value, now + ttl)

            if len(self.cache) > self.capacity:
                self.cache.popitem(last=False)
                
lru = LRUCache(2)
lru.put(1, 10, ttl=1)     # expires in 1s
lru.put(2, 20, ttl=5)

print(lru.get(1))         # 10
time.sleep(1.1)
print(lru.get(1))         # -1 (expired)

lru.put(1, 15, ttl=1)
time.sleep(0.8)
lru.put(1, 20, ttl=1.5)
time.sleep(0.8)
print(lru.get(1)) # 20

print("##############")
lru2 = LRUCacheOrderedDict(2)
lru2.put(1, 10, ttl=1.0)  # Expires in 1s
lru2.put(2, 20, ttl=5.0)

print(lru2.get(1))  # 10
time.sleep(1.1)
print(lru2.get(1))  # -1 (expired)
lru2.put(3, 30, ttl=5.0)  # Should evict 2 now (LRU)

print(lru2.get(2))  # 20 or -1 depending on TTL
print(lru2.get(3))  # 30
