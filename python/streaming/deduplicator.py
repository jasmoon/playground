from collections import deque


class DuplicateCheckerDynamicK:
    def __init__(self):
        self.stream = []

    def add(self, num):
        self.stream.append(num)

    def check_duplicate(self, k):
        if k > len(self.stream):
            k = len(self.stream)

        window = self.stream[-k:] # can be understood as take last k elements
        return len(window) > len(set(window))
    
print("############## Duplicate dynamic K")
duplicate = DuplicateCheckerDynamicK()
duplicate.add(1)
duplicate.add(2)
print(duplicate.check_duplicate(3))
duplicate.add(2)
print(duplicate.check_duplicate(2))

class DuplicateCheckerStaticK:
    def __init__(self, k):
        self.k = k
        self.window = deque()
        self.seen = set()
        self.has_dup = False

    def add(self, num):
        duplicate = num in self.seen
        self.window.append(num)
        self.seen.add(num)

        if len(self.window) > self.k:
            removed = self.window.popleft()
            if removed not in self.window:
                self.seen.remove(removed)

        return duplicate

    def check_duplicate(self):
        return len(self.seen) < len(self.window)

print("############## Duplicate static K")
dc = DuplicateCheckerStaticK(3)
print(dc.add(1))  # False
print(dc.add(2))  # False
print(dc.add(1))  # True (1 is duplicate in window)
print(dc.add(3))  # False (1 falls out of window)
print(dc.add(1))  # False (1 re-added, but not yet duplicate)
print(dc.add(1))  # True (1 is now duplicate again)