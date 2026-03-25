# - 2 arrays - birth and death

# - birth - [1990, 1995, ...]
# - death - [2025, 2005, ...]
# - means a particular person born in 1990 and died in 2025

# Goal: find the year with the highest population

# - no negative numbers
# - not sorted
# - contain duplicates
# - 0 < size of array < 10^6



from collections import defaultdict


def highest_population(birth: list[int], death: list[int]):
    """
    for the following algo, the year that the death happened is not counted towards the population
    runtime: O(n + k), space: O(k)
    """
    # find minimum year in birth and death arrays
    
    min_year = float("inf")
    max_year = float("-inf")
    n = len(birth)
    
    for i in range(n): # O(n)
        min_year = min(min_year, birth[i], death[i])
        max_year = max(max_year, birth[i], death[i])
        
    diff_per_year = [0] * int(max_year - min_year + 1) # O(k)
    for i in range(n): # O(n)
        index = int(birth[i] - min_year)
        diff_per_year[index] += 1

        index = int(death[i] - min_year)
        diff_per_year[index] -= 1

	# scan population array for index with highest population
    highest = 0
    ans = 0
    curr_population = 0
    
    for i, diff in enumerate(diff_per_year): # O(k)
        curr_population += diff
        
        if curr_population > highest: # return the smallest year
            highest = curr_population
            ans = i + min_year
            
    return ans

def highest_population_dict(birth: list[int], death: list[int]):
    """
    for the following algo, the year that the death happened is not counted towards the population
    runtime: O(n log n), space: O(n)
    """

    diffs = defaultdict(int)
    n = len(birth)

    for i in range(n): # O(n)
        diffs[birth[i]] += 1
        diffs[death[i]] -= 1
    
    ans = 0
    highest = 0
    curr_population = 0

    for year in sorted(diffs.keys()):
        curr_population += diffs[year]
        if curr_population > highest: # return the smallest year
            highest = curr_population
            ans = year
            
    return ans

def test_simple():
    highest_population_tcs = [
        (
            [1990, 2000, 2010],
            [1995, 2005, 2020],
            1990
        ),
        (
            [1990, 1990, 2000, 2001],
            [2000, 1995, 2010, 2005],
            1990
        ),
        (
            [1980, 1990, 2000],
            [1985, 1995, 2010],
            1980
        )
    ]
    for birth, death, expected in highest_population_tcs:
        output = highest_population(birth, death)
        print(output)
        assert expected == output

        output = highest_population_dict(birth, death)
        print(output)
        assert expected == output

if __name__ == "__main__":
    test_simple()