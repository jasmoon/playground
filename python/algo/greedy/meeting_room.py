import heapq

# Leetcode 252
def meeting_rooms(intervals: list[list[int]]):
    intervals.sort()
    prev_end = intervals[0][1]

    for start, end in intervals[1:]:
        if start < prev_end:
            return False
        prev_end = max(end, prev_end)
    return True

# Leetcode 253
def meeting_rooms_ii_heap(intervals: list[list[int]]):
    intervals.sort(key=lambda interval: interval[0])

    min_heap: list[int] = []
    heapq.heappush(min_heap, intervals[0][1])

    for start, end in intervals[1:]:
        # if earliest ending meeting finished before current starts, reuse
        if min_heap[0] <= start: 
            heapq.heappop(min_heap)
        heapq.heappush(min_heap, end)
    return len(min_heap)

def meeting_rooms_ii_ptr(intervals: list[list[int]]):
    starts: list[int] = sorted(start for start, _ in intervals)
    ends: list[int] = sorted(end for _, end in intervals)

    rooms = end_idx = 0

    for start in starts:
        if start < ends[end_idx]:
            rooms += 1
        else:
            end_idx += 1
    return rooms

# Leetcode 759
def employee_free_time(schedules: list[list[list[int]]]):
    intervals = [
        interval
        for intervals in schedules
        for interval in intervals
    ]
    intervals.sort()
    prev_start = intervals[0][0]
    prev_end = intervals[0][1]
    merged = []

    for start, end in intervals[1:]:
        if start <= prev_end:
            prev_end = max(end, prev_end)
        else:
            merged.append([prev_start, prev_end])
            prev_start, prev_end = start, end
    merged.append([prev_start, prev_end])

    prev_start = merged[0][0]
    prev_end = merged[0][1]
    ans = []
    for i in range(1, len(merged)):
        ans.append([merged[i-1][1], merged[i][0]])

    return ans




def test_simple():
    i_tcs = [
        ([[0,30],[5,10],[15,20]], False),
        ([[7,10],[2,4]], True)

    ]

    for intervals, expected in i_tcs:
        output = meeting_rooms(intervals)
        print(output)
        assert expected == output

    ii_tcs = [
        ([[0,30],[5,10],[15,20]], 2),
    ]
    for intervals, expected in ii_tcs:
        output = meeting_rooms_ii_heap(intervals)
        print(output)
        assert expected == output

        output = meeting_rooms_ii_ptr(intervals)
        print(output)
        assert expected == output

    employee_free_time_tcs = [
        (
            [
                [[1,2],[5,6]],
                [[1,3]],
                [[4,10]]
            ],
            [[3, 4]]
            ),
    ]
    for schedule, expected in employee_free_time_tcs:
        output = employee_free_time(schedule)
        print(output)
        assert expected == output
if __name__ == "__main__":
    test_simple()