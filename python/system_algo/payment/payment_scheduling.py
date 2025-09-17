from typing import Any


from collections import defaultdict, deque, namedtuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pprint import pprint


Limit = namedtuple("Limit", ["amount", "seconds"])
@dataclass
class Payment:
    id: str
    amount: int
    scheduled: str
    scheduledDt: datetime = field(init=False)

    def __post_init__(self):
        try:
            self.scheduledDt = datetime.fromisoformat(self.scheduled)
        except ValueError:
            raise ValueError(f"invalid datetime string {self.scheduled}")


class PaymentScheduler:
    def __init__(self, batchWindowMins: int=30, globalLimit: Limit=Limit(amount=2, seconds=60)) -> None:
        self.payments: list[Payment] = []
        self.batchWindowMins: int = batchWindowMins
        self.globalLimit = globalLimit

    def addPayments(self, payments: list[dict[str, str | int]]):
        for payment in payments:
            self.payments.append(
                Payment(
                    id=str(payment["id"]),
                    amount=int(payment["amount"]),
                    scheduled=str(payment["scheduled"]),
                )
            )

    def processingOrder(self) -> list[Payment]:
        return sorted(
            self.payments, key=lambda payment: payment.scheduledDt
        )

    def batchProcessing(self) -> defaultdict[str, list[Payment]]:
        i = 0
        payments = self.processingOrder()
        batches: defaultdict[str, list[Payment]] = defaultdict(list)

        if len(payments) == 0:
            return batches

        start = payments[0].scheduledDt
        batch: list[Payment] = []

        for payment in payments:
            if payment.scheduledDt >= start + timedelta(minutes=self.batchWindowMins):

                if len(batch) > 0:
                    batchTsDuration = (
                        start.strftime("%Y-%m-%dT%H:%M")
                        + " - "
                        + (start + timedelta(minutes=self.batchWindowMins) - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M")
                    )
                    batches[batchTsDuration] = batch[::]
                    batch.clear()

                start = payment.scheduledDt
            batch.append(payment)


        if len(batch) > 0:
            batchTsDuration = (
                start.strftime("%Y-%m-%dT%H:%M")
                + " - "
                + (start + timedelta(minutes=self.batchWindowMins) - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M")
            )
            batches[batchTsDuration] = batch[::]

        return batches

    def rateLimitedProcessing(self):
        paymentsScheduling: defaultdict[str, list[Payment]] = defaultdict(list)
        payments= self.processingOrder()
        if not payments:
            return paymentsScheduling

        window: deque[datetime] = deque()

        for payment in payments:
            scheduledTime = payment.scheduledDt
            while window and (scheduledTime - window[0]).total_seconds() >= self.globalLimit.seconds:
                window.popleft()

            if len(window) >= self.globalLimit.amount:
                scheduledTime = max(
                    window.popleft() + timedelta(seconds=self.globalLimit.seconds),
                    scheduledTime
                )

            window.append(scheduledTime)
            paymentsScheduling[scheduledTime.strftime("%Y-%m-%dT%H:%M:%S")].append(payment)
        return paymentsScheduling





def test_chronological_processing():
    payments: list[dict[str, str | int]] = [
        {"id": "p1", "amount": 100, "scheduled": "2025-08-31T09:00:00"},
        {"id": "p2", "amount": 200, "scheduled": "2025-08-31T08:30:00"},
        {"id": "p3", "amount": 150, "scheduled": "2025-08-31T09:15:00"},
    ]
    scheduler = PaymentScheduler(globalLimit=Limit(amount=2, seconds=1))
    scheduler.addPayments(payments)

    pprint(scheduler.processingOrder())
    # expected
    # [
    #     {id: "p2", amount: 200, processed_at: "2025-08-31T08:30:00"},
    #     {id: "p1", amount: 100, processed_at: "2025-08-31T09:00:00"},
    #     {id: "p3", amount: 150, processed_at: "2025-08-31T09:15:00"}
    # ]


def test_batching():
    payments = [
        {"id": "p1", "amount": 100, "scheduled": "2025-08-31T09:00:00"},
        {"id": "p2", "amount": 200, "scheduled": "2025-08-31T09:05:00"},
        {"id": "p3", "amount": 300, "scheduled": "2025-08-31T09:20:00"},
        {"id": "p4", "amount": 400, "scheduled": "2025-08-31T10:00:00"},
    ]
    scheduler = PaymentScheduler(globalLimit=Limit(amount=2, seconds=1))
    scheduler.addPayments(payments)

    pprint(scheduler.batchProcessing())
    # Batch 1 (09:00–09:29):
    # [{id: "p1", amount: 100}, {id: "p2", amount: 200}, {id: "p3", amount: 300}]

    # Batch 2 (10:00–10:29):
    # [{id: "p4", amount: 400}]


def test_rate_limit():
    payments = [
        {"id": "p1", "amount": 100, "scheduled": "2025-08-31T09:00:00"},
        {"id": "p2", "amount": 200, "scheduled": "2025-08-31T09:00:00"},
        {"id": "p3", "amount": 300, "scheduled": "2025-08-31T09:00:20"},
        {"id": "p4", "amount": 300, "scheduled": "2025-08-31T09:01:10"},
        {"id": "p5", "amount": 300, "scheduled": "2025-08-31T09:01:20"},
        {"id": "p6", "amount": 400, "scheduled": "2025-08-31T09:01:45"},
    ]
    scheduler = PaymentScheduler(globalLimit=Limit(amount=2, seconds=60))
    scheduler.addPayments(payments)

    expected: defaultdict[str, list[Payment]] = defaultdict(list)
    expected.update({
        '2025-08-31T09:00:00': [
            Payment(id='p1', amount=100, scheduled='2025-08-31T09:00:00'),
            Payment(id='p2', amount=200, scheduled='2025-08-31T09:00:00')
        ],
        '2025-08-31T09:01:00': [
            Payment(id='p3', amount=300, scheduled='2025-08-31T09:00:20')
        ],
        '2025-08-31T09:01:10': [
            Payment(id='p4', amount=300, scheduled='2025-08-31T09:01:10')
        ],
        '2025-08-31T09:02:00': [
            Payment(id='p5', amount=300, scheduled='2025-08-31T09:01:20')
        ],
        '2025-08-31T09:02:10': [
            Payment(id='p6', amount=400, scheduled='2025-08-31T09:01:45')
        ]
    })
    assert(scheduler.rateLimitedProcessing() == expected)


if __name__ == "__main__":
    # test_chronological_processing()
    test_batching()
    test_rate_limit()