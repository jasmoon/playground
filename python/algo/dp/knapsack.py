
def knapsack_classic(weights, values, W):
    n = len(weights)
    dp = [0] * (W + 1)

    for i in range(n):
        for w in range(W, weights[i]-1, -1):
            dp[w] = max(dp[w], dp[w-weights[i]] + values[i])

    return dp[W]

def test_simple():
    tcs = [
        (
            [2, 3, 4, 5],
            [3, 4, 5, 6],
            5,
            7
        )
    ]
    for weights, values, W, expected in tcs:
        output = knapsack_classic(weights, values, W)
        print(output)
        assert expected == output

if __name__ == "__main__":
    test_simple()
