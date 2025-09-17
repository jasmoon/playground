import heapq
from bisect import bisect_right
from datetime import datetime, timedelta
from typing import TypedDict

import numpy as np

Timestamp = int | str
FxRate = dict[str, dict[Timestamp, float]]


class Payment(TypedDict):
    payment_id: str
    currency: str
    amount: float
    timestamp: Timestamp


class WindowDedup:
    def __init__(self, window: timedelta = timedelta(hours=24)) -> None:
        self.storage: dict[str, dict[str, str | float]] = {}
        self.tsMinHeap: list[tuple[int, str]] = []
        self.window = window

    def addPayment(
        self, payment: Payment, normalisedTs: int, conversionRate: float
    ) -> dict[str, str | float] | None:
        pid = payment["payment_id"]
        baseAmount = payment["amount"]

        while self.tsMinHeap and self.tsMinHeap[0][0] < normalisedTs: 
            _, oldPid = heapq.heappop(self.tsMinHeap) # remove expired transactions
            _ = self.storage.pop(oldPid, None)

        if pid in self.storage:
            return None

        heapq.heappush(self.tsMinHeap, (normalisedTs + int(self.window.total_seconds()), pid))
        transaction: dict[str, str | float] = {
            "payment_id": pid,
            "usd_amount": float(np.round(conversionRate * baseAmount, 2)),
        }
        self.storage[pid] = transaction
        return transaction

    def showPaymentsWithinWindow(self) -> list[dict[str, str | float]]:
        return list(self.storage.values())


def convertToUsd(
    payments: list[Payment], fxRates: FxRate, dedup: WindowDedup
) -> list[dict[str, str | float]]:
    res: list[dict[str, str | float]] = []
    currencyToSortedTsList: dict[str, list[tuple[int, float]]] = {
        currency: sorted(
            ((normaliseTs(ts), rate) for ts, rate in fxRates[currency].items())
        )
        for currency in fxRates.keys()
    }

    for payment in payments:
        pid = payment["payment_id"]
        baseCurrency: str = payment["currency"]
        baseAmount = payment["amount"]
        ts = normaliseTs(payment["timestamp"])
        if baseCurrency not in fxRates:
            raise ValueError("missing fx rate")

        tsList = currencyToSortedTsList[baseCurrency]
        idx = bisect_right(tsList, ts, key=lambda tsToFxRate: tsToFxRate[0]) - 1
        if idx < 0:
            raise ValueError(f"no available FX rate before {ts} for {baseCurrency}")

        transaction: dict[str, str | float] | None = dedup.addPayment(
            payment, ts, tsList[idx][1]
        )
        if not transaction:
            print(f"payment {payment} at ts {datetime.fromtimestamp(ts)} is dropped")
            continue

        res.append(transaction)

    return res


def normaliseTs(ts: str | int) -> int:
    if isinstance(ts, int):
        return ts
    return int(datetime.fromisoformat(ts).timestamp())


payments1 = [
    Payment(
        payment_id="p1", amount=100, currency="EUR", timestamp="2025-08-28T10:00:00"
    ),
    Payment(
        payment_id="p2", amount=200, currency="GBP", timestamp="2025-08-28T10:01:00"
    ),
]

fxRates1 = FxRate(
    EUR={"2025-08-28T10:00:00": 1.1},
    GBP={"2025-08-28T10:01:00": 1.3},
)

payments2 = [
    Payment(payment_id="p1", currency="EUR", amount=100, timestamp=5),
    Payment(payment_id="p2", currency="JPY", amount=10000, timestamp=3),
]

fxRates2 = FxRate(
    EUR={1: 1.05, 4: 1.07, 6: 1.08},
    JPY={2: 0.007, 3: 0.0071},
)

payments3 = [
    Payment(payment_id="p1", currency="EUR", amount=100, timestamp=1),
    Payment(payment_id="p1", currency="EUR", amount=100, timestamp=2),  # duplicate
    Payment(payment_id="p2", currency="JPY", amount=10000, timestamp=3),
]

fxRates3 = FxRate(EUR={1: 1.05, 2: 1.06}, JPY={3: 0.0071})


tcs = [
    (
        payments1,
        fxRates1,
        [
            {"payment_id": "p1", "usd_amount": 110.0},
            {"payment_id": "p2", "usd_amount": 260.0},
        ],
    ),
    (
        payments2,
        fxRates2,
        [
            {"payment_id": "p1", "usd_amount": 107.00},
            {"payment_id": "p2", "usd_amount": 71.00},
        ],
    ),
    (
        payments3,
        fxRates3,
        [
            {"payment_id": "p1", "usd_amount": 105.0},
            {"payment_id": "p2", "usd_amount": 71.0},
        ],
    ),
]

if __name__ == "__main__":
    for tc in tcs:
        dedup = WindowDedup()
        expected = tc[2]
        output = convertToUsd(tc[0], tc[1], dedup)
        print(output)
        assert expected == output
