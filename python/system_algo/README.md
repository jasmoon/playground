
# Leetcode Style System Algorithm Practice

This section covers the different levels of practices for leetcode style system algorithms.

|Recommended order|Lessons|Where To Go|
|--|--|--|
|1|New to this type of questions|`basic` folder
|2|Bloom Filter, Rate Limiting, Window Deduplication|`intermediate` folder
|3|Rolling Window, Rolling Buckets|watch_tracker.py
|4|- Count Min Sketch (CMS) <br> - Rolling CMS <br> - Maintaining Global Top K <br> - Concurrency<br> |hashtag_tracker.py
|5|- HyperLogLog (HLL) <br> - Rolling HLL|visit_tracker.py
|6|- Ring Buffer - Atomic Bucket|orderbook.py
## Recommended Practices

- Timestamps
  -  Use epoch (int) datatype to store timestamps: save time from parsing, manipulation using datetime.
  -  Use `relativedelta` from dateutil.relativedelta to find earlier / future dates. Look at `payment/recurring.py`
-  Approximation data structures
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
  