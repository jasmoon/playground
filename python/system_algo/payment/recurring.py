from datetime import datetime
from dateutil.relativedelta import relativedelta

class RecurringPayment:
    def __init__(self, id: str, interval: str, startDate: datetime | None) -> None:
        self.id = id
        self.interval = interval
        self.startDate = startDate

    def makePayment(self, targetDate: datetime) -> str | None:
        if self.startDate and targetDate < self.startDate:
            return None
        if self.interval == "daily":
            # Daily payments are always due on or after start date
            return self.id

        if self.startDate is None:
            raise ValueError("unexpected missing field: start_date")

        if self.interval == "weekly":
            diff_days = (targetDate - self.startDate).days
            if diff_days % 7 == 0:
                return self.id
        elif self.interval == "monthly":
            diff_months = (targetDate.year - self.startDate.year) * 12 + (targetDate.month - self.startDate.month)
            res = self.startDate + relativedelta(months=diff_months)
            if res == targetDate:
                return self.id
        return None
        

def preprocess(users: list[dict[str, str]]) -> list[RecurringPayment]:
    recurringPayments: list[RecurringPayment] = []
    for user in users:
        if "id" not in user:
            raise ValueError("missing field: id")
        if "interval" not in user:
            raise ValueError("missing field: interval")

        id = user["id"]
        interval = user["interval"]
        if (
            interval in {"weekly", "monthly"} and
            "start_date" not in user
        ):
            raise ValueError("missing field: start_date")

        startDate = None
        if "start_date" in user:
            startDate = datetime.strptime(user["start_date"], "%Y-%m-%d")

        recurringPayments.append(RecurringPayment(
            id, interval, startDate
        ))
    return recurringPayments


def payment_due(users: list[dict[str, str]], targetDate: str) -> list[str]:
    recurringPayments = preprocess(users)
    target = datetime.strptime(targetDate, "%Y-%m-%d")

    targetUsers: list[str] = []
    for recurringPayment in recurringPayments:
        userId = recurringPayment.makePayment(targetDate=target)
        if userId:
            targetUsers.append(userId)
    return targetUsers

users1: list[dict[str, str]] = [
    {"id": "u1", "interval": "daily"},
    {"id": "u2", "interval": "weekly", "start_date": "2025-08-01"},
    {"id": "u3", "interval": "monthly", "start_date": "2025-08-05"}
]

users2 = [
    {"id": "u1", "interval": "monthly", "start_date": "2024-01-31"},
    {"id": "u2", "interval": "monthly", "start_date": "2024-02-29"},
]

users3 = [
    {"id": "u1", "interval": "weekly", "start_date": "2024-12-25"},
    {"id": "u2", "interval": "monthly", "start_date": "2024-12-30"},

]

tcs = [
    (users1, "2025-08-08", ['u1', 'u2']),
    (users1, "2026-04-05", ['u1', 'u3']),

    (users2, "2024-02-29", ['u1', 'u2']),
    (users2, "2025-02-28", ['u1', 'u2']),
    (users2, "2025-02-27", []),

    (users3, "2025-01-01", ['u1']),
    (users3, "2025-01-30", ['u2']),
    (users3, "2024-12-30", ['u2']),
    (users3, "2024-01-01", []),
]

if __name__ == "__main__":
    for i, tc in enumerate(tcs):
        print(f"Processing test case {i}")
        expected = tc[2]
        output = payment_due(tc[0], tc[1])
        assert expected == output , f"expected {expected} but output is {output}"
