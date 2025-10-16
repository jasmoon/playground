from typing import Any


from collections import defaultdict

# Problem: Longest Substring with At Most K Distinct Characters
# Given a string s and an integer k, return the length of the longest substring that contains at most k distinct characters.
def longest_substring_k_distinct(s: str, k: int):
    left = 0
    n = len(s)
    ans = 0
    cnts: defaultdict[str, int] = defaultdict(int)

    for right in range(n):
        c = s[right]
        cnts[c] += 1

        while len(cnts) > k:
            if s[left] in cnts:
                cnts[s[left]] -= 1
                if cnts[s[left]] == 0:
                    del cnts[s[left]]
            left += 1
        ans = max(ans, right - left + 1)
    return ans
    


def test_simple():
    tcs = [
        ("eceba", 2, 3),
        ("aa", 1, 2),
        ("a", 0, 0),
        ("abcabcbb", 3, 8),
        ("aaabbbccc", 2, 6),
        ("abaccc", 2, 4),
        ("abcdef", 10, 6),
        ("ababababa", 1, 1),
        ("", 2, 0),
        ("aaabccdeeef", 3, 6)
    ]

    for s, k, expected in tcs:
        output = longest_substring_k_distinct(s, k)
        print(output)
        assert expected == output

if __name__ == "__main__":
    test_simple()