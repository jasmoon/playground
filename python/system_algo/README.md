
# Leetcode Style System Algorithm Practice

This section covers the different levels of practices for leetcode style system algorithms.

|Recommended order|Lessons|Where To Go|
|--|--|--|
|1|New to this type of questions|`basic` folder
|2|- Bloom Filter <br> - Rate Limiting <br> - Window Deduplication|`intermediate` folder
|3|- Rolling Window <br> - Rolling Buckets|watch_tracker.py
|4|- Count Min Sketch (CMS) <br> - Rolling CMS <br> - Maintaining Global Top K <br> - Concurrency<br> |hashtag_tracker.py
|5|- HyperLogLog (HLL) <br> - Rolling HLL|visit_tracker.py
|6|- Ring Buffer <br> - Atomic Bucket|orderbook.py
|7|- Ring Buffer <br> - Rate Computation|carpark.py
|8|- Ring Buffer in Rolling CMS|impression_tracker.py


## Recommended Practices

### `defaultdict`

- Use `defaultdict` when you *never delete keys* (only accumulate)
- It is best used for simple counters or dictionary with fixed keys initiation like:
```
analytics = defaultdict(lambda: {"size":0, "unique_count": 0})
```
- when *only reading* values, it is best to use `d.get(key)` as `d[key]` calls the `default_factory` if key is missing

### Timestamps
- Use epoch (int) datatype to store timestamps: save time from parsing, manipulation using datetime.
- Use `relativedelta` from dateutil.relativedelta to find earlier / future dates. Look at `payment/recurring.py`

###  Approximation data structures
 -  Count Min Sketch (CMS)
    -  Approximate frequency counting
    -  Library: `from countminsketch import CountMinSketch` (3rd party) or implement manually using hashes
    -  Typical operations: `add(item)`, `query(item)`
    -  Great for streaming data (e.g. top-k queries, heavy hitters, rate limiting)
 -  HyperLogLog (HLL)
    -  Approximate cardinality (number of unique elements)
    -  Library: `from hyperloglog import HyperLogLog`
    -  Typical operations: `add(item)`, `count()`
    -  Used for distinct user counts, unique visitors, analytics metrics
 -  Bloom Filter:
    -  Probabilistic membership testing
    -  Library: `from pybloom_live import BloomFilter`
    -  Typical operations: `add(item)`, `might_contain(item)`
    -  Ideal for cache filtering, deduplication, avoiding expensive lookups
 -  Cuckoo Filter:
    -  Probabilistic membership + deletion
    -  Library: `from cuckoofilter import CuckooFilter`
    -  Typical operations: `add(item)`, `contains(item)`, `delete(item)`
    -  Used in networking, caches, and databases where deletion is required

### Locks
- When introducing a lock, it is good to start simple, but measure and profile before optimizing
- We can first introduce a global lock for simplicity but can create bottlenecks in production.
- When multiple locks is required for a block of code, always lock in sorted order
#### High Concurrency with Locks
Ways to handle contention in high concurrency environments:
- Use lock stripes
- Sharded data structure with locks
- `class` can incorporate lock (atomic data structure)
- Read-write locks for read-heavy workloads
- Lock-free alternatives (atomic operations, concurrent data structures)
#### Inroducing Locks into data structures and algorithm
Recommended way to go about introducing locks:
- Start simple, but measure and profile before optimizing
- Choose lock granularity based on contention patterns
- Use established concurrent data structures when available
- Document your lock ordering to prevent deadlocks
- Consider whether you need locks at all (maybe lock-free structures suffice)
#### Removing locks
If you need to safely remove locks, consider:
- Reference counting
- RCU (Read-Copy-Update) patterns
- Lock-free data structures
- Accepting that some locks live for the program's lifetime

