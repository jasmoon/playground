
# Question
# LC 198 - House Robber
# but with negative numbers
def no_adjacent_nums(nums: list[int]) -> int:
    take = skip = 0
    for num in nums:
        new_take = skip + num
        new_skip = max(take, skip)
        take, skip = new_take, new_skip
    return max(take, skip)

def test_simple():
    tcs = [
        ([1,-1,-1,1], 2),
        ([1,2,3,1], 4)
    ]

    for nums, expected in tcs:
        output = no_adjacent_nums(nums)
        print(output)
        assert expected == output

if __name__ == "__main__":
    test_simple()