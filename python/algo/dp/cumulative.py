
# Question
# LC 198 - House Robber
# but with negative numbers
def no_adjacent_nums(nums: list[int]) -> int:
    take = nums[0]
    skip = float("-inf")
    best = nums[0]
    for num in nums[1:]:
        new_take = skip + num
        new_skip = max(take, skip)
        take, skip = new_take, new_skip
        best = max(best, take, skip, num)
        
    return int(best)

def test_simple():
    tcs = [
        ([1,-1,-1,1], 2),
        ([1,2,3,1], 4),
        ([-2,-1,-1,-3], -1),
    ]

    for nums, expected in tcs:
        output = no_adjacent_nums(nums)
        print(output)
        assert expected == output

if __name__ == "__main__":
    test_simple()