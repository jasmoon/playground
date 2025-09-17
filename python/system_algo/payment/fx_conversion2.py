from fx_conversion1 import Payment, normaliseTs
from bisect import bisect_right
import numpy as np

FxRate = dict[str, dict[str, dict[int, float]]]

def convertToUsd(
    payments: list[Payment], fxRates: FxRate
) -> list[dict[str, str | float]]:
    res: list[dict[str, str | float]] = []
    currencyToSortedTsList = {}
    for currency, providers in fxRates.items():
        tsRateList = [
            (normaliseTs(ts), fxRate)
            for provider in providers.keys()
            for ts, fxRate in providers[provider].items()
        ]
        tsRateList.sort()
        currencyToSortedTsList[currency] = tsRateList


    for payment in payments:
        pid = payment["payment_id"]
        baseCurrency: str = payment["currency"]
        baseAmount = payment["amount"]
        ts = normaliseTs(payment["timestamp"])
        if baseCurrency not in fxRates:
            raise ValueError("missing fx rate")

        tsList = currencyToSortedTsList[baseCurrency]
        idx = bisect_right(tsList, (ts, float("inf"))) - 1
        if idx < 0:
            raise ValueError(f"no available FX rate before {ts} for {baseCurrency}")

        conversionRate = tsList[idx][1]
        res.append({
            "payment_id": pid,
            "usd_amount": float(np.round(conversionRate * baseAmount, 2)),
        })

    return res

payments1 = [
    Payment(payment_id="p1", currency="EUR", amount=100, timestamp=10),
    Payment(payment_id="p2", currency="GBP", amount=50, timestamp=5),
]

fxRates1 = {
    "EUR": {
        "provider1": {5: 1.1, 10: 1.15},
        "provider2": {3: 1.05, 10: 1.16}
    },
    "GBP": {
        "provider1": {2: 1.3, 5: 1.31},
        "provider2": {1: 1.28, 5: 1.32}
    }

}

tcs = [
    (
        payments1,
        fxRates1,
        [
        {"payment_id": "p1", "usd_amount": 116.0},  # best rate is 1.16
        {"payment_id": "p2", "usd_amount": 66.0}    # best rate is 1.32
        ]
    ),

]

if __name__ == "__main__":
    for tc in tcs:
        expected = tc[2]
        output = convertToUsd(tc[0], tc[1])
        print(output)
        assert expected == output