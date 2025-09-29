def determineWaterHeightsSlow(heights: list[int], rainFallAmount: int) -> list[int]: # O(rain * len(heights))
    n = len(heights)
    water = [0] * n
    if n == 0 or rainFallAmount <= 0:
        return water

    peakIdx = heights.index(max(heights))
    leftRain = rightRain = 0
    if peakIdx == 0:
        rightRain = rainFallAmount
    elif peakIdx == n - 1:
        leftRain = rainFallAmount
    else:
        leftRain = rainFallAmount // 2
        rightRain = rainFallAmount - leftRain

    def pourSide(start_idx, step, waterAmount):
        for _ in range(waterAmount):
            pos = start_idx
            while True:
                nextPos = pos + step
                if not (0 <= nextPos < n):
                    break

                if heights[nextPos] + water[nextPos] <= heights[pos] + water[pos]:
                    pos = nextPos
                else:
                    break
            if pos <= 0 or pos >= n - 1:
                break
            water[pos] += 1
    
    if peakIdx > 0:
        pourSide(peakIdx, -1, leftRain)
    if peakIdx < n-1:
        pourSide(peakIdx, 1, rightRain)
    return water



def determineWaterHeightsFast(heights: list[int], rainFallAmount: int) -> list[int]: # O(len(heights))
    n = len(heights)
    if n == 0 or rainFallAmount <= 0:
        return [0] * n

    peakIdx = heights.index(max(heights))
    water = [0] * n

    # Split rain
    if peakIdx == 0:
        leftRain, rightRain = 0, rainFallAmount
    elif peakIdx == n - 1:
        leftRain, rightRain = rainFallAmount, 0
    else:
        leftRain = rainFallAmount // 2
        rightRain = rainFallAmount - leftRain

    def fill_side(start: int, end: int, step: int, waterAmount: int):
        """Fill valleys between start (peak side) and end (boundary)."""
        if waterAmount <= 0:
            return

        idxs = list(range(start, end + step, step))
        stack = []  # stores (index, height)
        i = 0
        while i < len(idxs):
            cur_idx = idxs[i]
            cur_h = heights[cur_idx]
            # maintain non-increasing stack
            if not stack or cur_h <= stack[-1][1]:
                stack.append((cur_idx, cur_h))
                i += 1
            else:
                # found a right wall higher than last
                low_idx, low_h = stack.pop()
                if not stack:
                    continue
                left_idx, left_h = stack[-1]
                # Basin bounded by left_h and cur_h
                bound = min(left_h, cur_h)
                width = abs(cur_idx - left_idx) - 1
                if width <= 0:
                    continue
                # Capacity
                capacity = 0
                fill_cells = []
                for j in range(left_idx + step, cur_idx, step):
                    cap = max(0, bound - (heights[j] + water[j]))
                    if cap > 0:
                        capacity += cap
                        fill_cells.append((j, cap))
                if capacity == 0:
                    continue
                if waterAmount >= capacity:
                    # fill fully
                    for j, cap in fill_cells:
                        water[j] += cap
                    waterAmount -= capacity
                else:
                    # partial fill: distribute lowest-first
                    fill_cells.sort(key=lambda x: x[1])
                    for j, cap in fill_cells:
                        add = min(cap, waterAmount)
                        water[j] += add
                        waterAmount -= add
                        if waterAmount == 0:
                            break
                    return
        return

    # Fill left side (toward 0)
    if peakIdx > 0:
        fill_side(peakIdx, 0, -1, leftRain)
    # Fill right side (toward n-1)
    if peakIdx < n - 1:
        fill_side(peakIdx, n - 1, 1, rightRain)

    return water

    
def test_simple():
    heights = [5, 3, 1, 1, 2, 10, 9, 8, 7, 10]
    output = determineWaterHeightsSlow(heights, 10)
    print(output)
    # assert output == [0, 0, 2, 2, 1, 0, 0, 2, 3, 0]

    heights = [5, 3, 1, 5, 1, 10, 9, 8, 7, 10]
    output = determineWaterHeightsSlow(heights, 10)
    print(output)
    assert output == [0, 0, 1, 0, 4, 0, 0, 2, 3, 0]

    # overflow on left and right
    heights = [3, 1, 1, 2, 10, 9, 8, 7, 10]
    output = determineWaterHeightsSlow(heights, 14)
    print(output)
    assert output == [0, 2, 2, 1, 0, 1, 2, 3, 0]


    # insufficient rainfall
    heights = [3, 1, 1, 2, 10, 9, 8, 7, 10]
    output = determineWaterHeightsSlow(heights, 5)
    print(output)
    assert output == [0, 1, 1, 0, 0, 0, 1, 2, 0] or output == [0, 2, 1, 0, 0, 0, 1, 2, 0]

    # heights = [5, 3, 1, 1, 2, 10, 9, 8, 7, 10]
    # output = determineWaterHeightsFast(heights, 10)
    # print(output)
    # # assert output == [0, 0, 2, 2, 1, 0, 0, 2, 3, 0]

    # heights = [5, 3, 1, 5, 1, 10, 9, 8, 7, 10]
    # output = determineWaterHeightsFast(heights, 10)
    # print(output)
    # assert output == [0, 0, 1, 0, 4, 0, 0, 2, 3, 0]

if __name__ == "__main__":
    test_simple()
