from sortedcontainers import SortedDict, SortedList, SortedSet
from heapdict import heapdict

print("\n################# heapdict #################\n")

hd = heapdict({
    'b': 10,
    'd': 4,
    'a': 2,
    'c': 3
})

print(hd.peekitem())    # ('a', 2), O(1)
hd['c'] = 1             # O(log n)
print(hd.peekitem())    # ('c', 1)
print([hd.popitem() for _ in range(len(hd))]) # [('c', 1), ('a', 2), ('d', 4), ('b', 10)]

#### ##############
#### Note that bisect_left / right ops does not feel very useful

print("\n################# SortedDict #################\n")

sd = SortedDict({
    'b': 10,
    'd': 4,
    'a': 1,
    'c': 3
})

print({k: sd[k] for k in sd.irange("b", "d", inclusive=(True, False))}) # {'b': 10, 'c': 3}), O(k log n)
print({k: sd[k] for k in sd.irange(maximum="d", inclusive=(True, False))})              # {'b': 10, 'c': 3}), O(k log n)

print("b" in sd)                      # True, O(log n)
print(sd.peekitem())                  # ('d', 4), O(1)
print(sd.bisect_left(("d")))    # 3
print(sd.bisect_right(("d")))   # 4

print("\n################# SortedList #################\n")

sl = SortedList(
    [('a', 5), ('a', 1)]
)

print(sl)               # SortedList([('a', 1), ('a', 5)])
print(sl[1])            # ('a', 5), O(1)

sl.add(("a", 3))                  # O(log n)
print(sl)                               # SortedList([('a', 1), ('a', 3), ('a', 5)])
print(("a", 10) in sl)                  # False, O(log n)
print(sl.bisect_left(("a", 3)))   # 1
print(sl.bisect_right(("a", 3)))  # 2
sl.discard(('a', 1))              # O(log n)
print(sl)                               # SortedList([('a', 3), ('a', 5)])

print([v for v in sl.irange(("a", 4), ("e", 1000), inclusive=(True, False))]) # [('a', 5)]

print("\n################# SortedSet #################\n")

ss = SortedSet(
    [('d', 10), ('d', 5), ('a', 1)]
)

print(ss)                   # SortedSet([('a', 1), ('d', 5), ('d', 10)])
print(ss[1])                # ('d', 5), #O(1)
ss.add(("b", 100))    # O(log n)
print(("b", 100) in ss)     # True, O(log n)

print(ss.bisect_left(("c", 10)))    # 2, O(log n)
ss.discard(("b", 100))              # O(log n)
print(ss)                                 # SortedSet([('a', 1), ('d', 5), ('d', 10)])

print([v for v in ss.irange(("c", 100), ("e", 1000), inclusive=(True, False))]) # [('d', 5), ('d', 10)]

