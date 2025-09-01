class PeekableIterator:
    def __init__(self, it):
        self.it = it
        self.buffer = None
        self.has_buffer = False

    def __iter__(self):
        return self

    def __next__(self):
        if self.has_buffer:
            res = self.buffer
            self.buffer = None
            self.has_buffer = False
            return res
        return next(self.it)

    def peek(self):
        if not self.has_buffer:
            self.buffer = next(self.it)
            self.has_buffer = True
        return self.buffer


it = PeekableIterator(iter([10, 20, 30]))

print(it.peek())  # 10
print(next(it))  # 10
print(it.peek())  # 20
print(next(it))  # 20
print(next(it))  # 30

try:
    print(it.peek())  # Should raise StopIteration
except StopIteration:
    print("Reached end of iterator")

from collections import deque

print("ZigzagLists")


class ZigzagLists:
    def __init__(self, *lists):
        self.queue = deque()
        for lst in lists:
            if lst:
                self.queue.append((lst, 0))

    def __iter__(self):
        return self

    def __next__(self):
        while self.queue:
            lst, index = self.queue.popleft()
            if index < len(lst):
                val = lst[index]
                index += 1
                self.queue.append((lst, index))
                return val
        raise StopIteration

    def iterate(self):
        ans = []
        while self.queue:
            lst, index = self.queue.popleft()
            if index < len(lst):
                val = lst[index]
                index += 1
                self.queue.append((lst, index))
                ans.append(val)
        return ans


it = ZigzagLists([1, 4], [2, 5, 6], [3], [])
print(f"iterate {it.iterate()}")
it = ZigzagLists([1, 4], [2, 5, 6], [3], [])
print(f"iterator {list(it)}")

print("ZigzagIterator")


class ZigzagIterator:
    def __init__(self, *lists):
        self.queue = deque()
        for lst in lists:
            self.queue.append(iter(lst))

    def __iter__(self):
        return self

    def __next__(self):
        while self.queue:
            it = self.queue.popleft()
            try:
                val = next(it)
                self.queue.append(it)
                return val
            except StopIteration:
                continue
        raise StopIteration


it = ZigzagIterator([1, 4], [2, 5, 6], [3], [])
print(list(it))


class NestedIterator:
    def __init__(self, nested_list):
        self.stack = [iter(nested_list)]
        self.next_val = None

    def __iter__(self):
        return self

    def __next__(self):
        if self.has_next():
            val = self.next_val
            self.next_val = None
            return val
        raise StopIteration

    def has_next(self):
        while self.stack:
            try:
                curr = next(self.stack[-1])
            except StopIteration:
                self.stack.pop()  # pop current it because it is empty
                continue

            if isinstance(curr, int):
                self.next_val = curr
                return True
            elif isinstance(curr, list):
                self.stack.append(iter(curr))
            else:
                raise TypeError("Expected int or list")
        return False


import time


class Timer:
    def __init__(self, label):
        self.label = label

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        end = time.time()
        duration = end - self.start
        print(f"[{self.label}] took {duration:.2f}s")


with Timer("sleeping"):
    time.sleep(1)


def my_enumerate(iterable, start=0):
    index = start
    for item in iterable:
        yield item, index
        index += 1


class MyEnumerate:
    def __init__(self, it, start=None, reverse=False):
        self.reverse = reverse

        if reverse:
            self.data = deque(it)  # no need to reverse manually
            self.index = len(self.data) - 1 if start is None else start
        else:
            self.it = iter(it)
            self.index = 0 if start is None else start

    def __iter__(self):
        return self

    def __next__(self):
        if self.reverse:
            if not self.data:
                raise StopIteration
            item = self.data.pop()  # O(1)
            idx = self.index
            self.index -= 1
            return idx, item
        else:
            item = next(self.it)
            idx = self.index
            self.index += 1
            return idx, item

    def __getitem__(self, idx):
        if isinstance(idx, slice):
            start, stop, _ = idx.indices(len(self))  # ignore step here

            def generator():
                for i, val in enumerate(self):
                    if i < start:
                        continue
                    if i >= stop:
                        break
                    yield val

            return generator()

        elif isinstance(idx, int):
            if idx < 0:
                idx += len(self)
            if idx < 0 or idx >= len(self):
                raise IndexError("index out of range")
            for i, val in enumerate(self):
                if i == idx:
                    return val
            raise IndexError("index out of range")
        else:
            raise TypeError("indices must be int or slice")

    def __len__(self):
        return len(self.data)


class TreeNode:
    def __init__(self, val, left=None, right=None):
        self.val = val
        self.left = left
        self.right = right


class TreeIterator:
    def __init__(self, root):
        self.root = root

    def __iter__(self):
        yield from self._preorder(self.root)

    def _preorder(self, node):
        if node:
            yield node.val
            yield from self._preorder(node.left)
            yield from self._preorder(node.right)


root = TreeNode(1, TreeNode(2, TreeNode(4), TreeNode(5)), TreeNode(3))
print(list(TreeIterator(root)))
