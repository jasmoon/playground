from collections import deque
import heapq
import random

class RunningAverage:
    def __init__(self, window=None):
        self.queue = deque()
        self.window_sum = 0
        self.window = window

    def add(self, num):
        self.queue.append(num)
        self.window_sum += num
        if self.window and len(self.queue) > self.window:
            self.window_sum -= self.queue.popleft()

    def get_average(self):
        return self.window_sum / len(self.queue)

avg = RunningAverage(window=3)
avg.add(10)
avg.add(20)
avg.add(30)
print(avg.get_average())  # 20.0
avg.add(40)
print(avg.get_average())  # 30.0 (20+30+40)

class StreamMedian:
    def __init__(self):
        self.min_heap = []
        self.max_heap = []

    def add(self, num):
        heapq.heappush(self.max_heap, -num)

        heapq.heappush(self.min_heap, -heapq.heappop(self.max_heap))
        if len(self.min_heap) > len(self.max_heap): # ensure max heap has equal or 1 more than min heap
            heapq.heappush(self.max_heap, -heapq.heappop(self.min_heap))

    def get_median(self):
        if len(self.max_heap) > len(self.min_heap):
            return -self.max_heap[0]
        else:
            return (self.min_heap[0] - self.max_heap[0]) / 2
        
m = StreamMedian()
m.add(1)
print(m.get_median())  # 1

m.add(5)
print(m.get_median())  # (1 + 5)/2 = 3.0

m.add(3)
print(m.get_median())  # 3 (sorted: [1, 3, 5])

class KthLargest:
    def __init__(self, k, stream):
        heapq.heapify(stream)
        self.min_heap = stream
        while len(self.min_heap) > k:
            heapq.heappop(self.min_heap)

    def add(self, num):
        heapq.heappush(self.min_heap, num)
        heapq.heappop(self.min_heap)
        return self.min_heap[0]

print("######### KthLargest")
kth = KthLargest(3, [4, 5, 8, 2])
print(kth.add(3))  # 4
print(kth.add(10)) # 5

########################
# For samplers, the intuition here is to decide whether to store the new element using probability with respect to population size.


class ReservoirSampler:
    def __init__(self):
        self.count = 0
        self.sample = None

    def add(self, value):
        self.count += 1
        if random.randint(1, self.count) == 1:
            self.sample = value

    def get_sample(self):
        return self.sample

class ReservoirSamplerK:
    def __init__(self, k):
        self.k = k
        self.reservoir = []
        self.count = 0

    def add(self, value):
        self.count += 1
        if len(self.reservoir) < self.k:
            self.reservoir.append(value)
            return
        idx = random.randint(0, self.count - 1)
        if idx < self.k:
            self.reservoir[idx] = value

    def get_sample(self):
        return list(self.reservoir)