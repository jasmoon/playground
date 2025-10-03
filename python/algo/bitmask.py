from functools import lru_cache


def subsets_backtrack(nums: list[int]) -> list[list[int]]:
    n = len(nums)
    ans: list[list[int]] = []

    def backtrack(cur_idx: int, arr: list[int]):
        if len(arr) > len(nums):
            return

        ans.append(list(arr))

        for i in range(cur_idx, n):
            backtrack(i + 1, arr + [nums[i]])

    backtrack(0, [])
    return ans


def subsets_bitmask(nums: list[int]) -> list[list[int]]:
    n = len(nums)
    ans: list[list[int]] = []
    for mask in range(1 << n):
        subset: list[int] = []
        for i in range(n):
            if mask & (1 << i):
                subset.append(nums[i])
        ans.append(subset)
    return ans


def max_compatibility_score_bitmask(
    students: list[list[int]], mentors: list[list[int]]
) -> int:
    m, n = len(students), len(students[0])
    scores = [[sum(students[s][k] == mentors[t][k] for k in range(n))
            for t in range(m)] for s in range(m)]

    @lru_cache(None)
    def dp(student_idx: int, mask: int) -> int:
        if student_idx == m:
            return 0

        highest = 0
        for i in range(m):
            if not mask & (1 << i):
                highest = max(highest,
                    scores[student_idx][i] +
                    dp(student_idx+1, mask | 1 << i))
        return highest

    return dp(0, 0)

def partition_k_equal_sum_subsets(
    nums: list[int], k: int
):
    target = sum(nums) // k
    n = len(nums)
    nums.sort(reverse=True)

    @lru_cache(None)
    def dp(mask: int, curr_sum: int):
        if mask == (1 << n) - 1:
            return curr_sum == 0

        for i in range(n):
            if not (mask & (1 << i)): # not used
                next_sum = curr_sum + nums[i]
                if next_sum <= target:
                    next_mask = mask | (1 << i)
                    if dp(next_mask, 0 if next_sum % target == 0 else curr_sum):
                        return True
        return False

    return dp(0, 0)


if __name__ == "__main__":
    print(subsets_backtrack([1, 2, 3]))
    print(subsets_bitmask([1, 2, 3]))
    print(
        max_compatibility_score_bitmask(
            students=[[1, 0, 1], [1, 1, 0],  [0, 0, 1]],
            mentors=[[0, 0, 1], [1, 0, 0], [1, 1, 0]],
        )
    )
    print(partition_k_equal_sum_subsets([4,3,2,3,5,2,1],4))
    print(partition_k_equal_sum_subsets([1,2,3,4],3))
