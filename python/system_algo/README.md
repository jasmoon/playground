
# Leetcode Style System Algorithm Practice

This section covers the different levels of practices for leetcode style system algorithms.

|Recommended order|Lessons|Where To Go|
|--|--|--|
|1|New to this type of questions|`basic` folder
|2|Bloom Filter, Rate Limiting, Window Deduplication|`intermediate` folder
|3|Streaming, Bucketing|watch_tracker.py
|4|- Count Min Sketch (CMS) <br> - Rolling CMS <br> - Maintaining Global Top K <br> - Resource Contention <br> |hashtag_tracker.py

## Recommended Practices

- Timestamps
  -  Use epoch (int) datatype to store timestamps: save time from parsing, manipulation using datetime.
  -  Use `relativedelta` from dateutil.relativedelta to find earlier / future dates. Look at `payment/recurring.py`
  