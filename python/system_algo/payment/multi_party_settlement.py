from dataclasses import dataclass, field
from collections import defaultdict
from pprint import pprint
from threading import Lock
from datetime import datetime
from enum import Enum
import uuid

class TransactionStatus(Enum):
    PENDING = 1
    SETTLED = 2
    FAILED = 3

@dataclass
class Transaction:
    id: str
    fromAccount: str
    toAccount: str
    amount: float
    currency: str
    status: TransactionStatus = TransactionStatus.PENDING
    timestamp: int = field(default_factory=lambda: int(datetime.now().timestamp()))

@dataclass
class AccountPosition:
    balance: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    incoming: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    outgoing: dict[str, float] = field(default_factory=lambda: defaultdict(float))


class MultiPartySettlement:
    def __init__(self) -> None:
        self.ledger: dict[str, Transaction] = {}
        self.obligations: defaultdict[str, defaultdict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
        self.accountLocks: defaultdict[str, Lock] = defaultdict(Lock)
        self.accountBalances: defaultdict[str, defaultdict[str, float]] = defaultdict(lambda: defaultdict(float))  # actual running balances

    def addBalance(self, account: str, currency: str, amount: float) -> None:
        self.accountBalances[account][currency] += amount

    def addTransaction(self, source: str, target: str, amount: float, currency: str) -> str:
        txnId = str(uuid.uuid4())
        txn = Transaction(
            id=txnId,
            fromAccount=source,
            toAccount=target,
            amount=amount,
            currency=currency,
        )
        self.ledger[txnId] = txn
        first, second = sorted([source, target])
        with self.accountLocks[first], self.accountLocks[second]:
            self.obligations[source][target].append(txnId)
        return txnId

    def computeNetPositions(self):
        positions: defaultdict[str, AccountPosition] = defaultdict(AccountPosition)
        for fromAccount, toAccountToAmount in self.obligations.items():
            for toAccount, txnIds in toAccountToAmount.items():
                for txnId in txnIds:
                    txn = self.ledger[txnId]
                    if txn.status != TransactionStatus.PENDING:
                        continue
                    positions[fromAccount].balance[txn.currency] -= txn.amount
                    positions[fromAccount].outgoing[txn.currency] += txn.amount
                    positions[toAccount].balance[txn.currency] += txn.amount
                    positions[toAccount].incoming[txn.currency] += txn.amount
        return positions

    def executeSettlement(self):
        progress = True
        # positions: defaultdict[str, AccountPosition] = self.computeNetPositions()

        while progress:
            progress = False
            for debtor, toAccountToTxnIds in list(self.obligations.items()):
                for creditor, txnIds in list(toAccountToTxnIds.items()):
                    for txnId in txnIds:
                        txn = self.ledger[txnId]
                        if txn.status != TransactionStatus.PENDING:
                            continue  # already processed

                        first, second = sorted([debtor, creditor])
                        with self.accountLocks[first], self.accountLocks[second]:
                            # Get current net positions


                            debtorBal = self.accountBalances[debtor][txn.currency]
                            if debtorBal >= txn.amount:
                                # ✅ can settle
                                txn.status = TransactionStatus.SETTLED
                                self.accountBalances[debtor][txn.currency] -= txn.amount
                                self.accountBalances[creditor][txn.currency] += txn.amount
                                progress = True
                                
        for txn in self.ledger.values():
            if txn.status == TransactionStatus.PENDING:
                txn.status = TransactionStatus.FAILED

def test_single_currency_settlement():
    s = MultiPartySettlement()

    # Alice pays Bob $100
    txn1 = s.addTransaction("Alice", "Bob", 100, "USD")
    # Bob pays Charlie $50
    txn2 = s.addTransaction("Bob", "Charlie", 50, "USD")

    positions = s.computeNetPositions()

    assert positions["Alice"].balance["USD"] == -100
    assert positions["Bob"].balance["USD"] == 50  # +100 incoming, -50 outgoing
    assert positions["Charlie"].balance["USD"] == 50

    s.addBalance("Alice", "USD", 100)
    # Execute settlement
    s.executeSettlement()
    assert s.accountBalances["Alice"]["USD"] == 0.
    assert s.accountBalances["Bob"]["USD"] == 50.
    assert s.accountBalances["Charlie"]["USD"] == 50.
    assert s.ledger[txn2].status.name == "SETTLED"
    assert s.ledger[txn1].status.name == "SETTLED"
    assert s.ledger[txn2].status.name == "SETTLED"


def test_multi_currency_independence():
    s = MultiPartySettlement()

    # Alice pays Bob $100 USD
    txn1 = s.addTransaction("Alice", "Bob", 100, "USD")
    # Alice pays Bob €200 EUR
    txn2 = s.addTransaction("Alice", "Bob", 200, "EUR")

    positions = s.computeNetPositions()

    # USD balances
    assert positions["Alice"].outgoing["USD"] == 100
    assert positions["Alice"].outgoing["EUR"] == 200
    assert positions["Bob"].incoming["USD"] == 100
    assert positions["Bob"].incoming["EUR"] == 200

    s.addBalance("Alice", "USD", 200)
    s.addBalance("Alice", "EUR", 300)

    # Execute settlement
    s.executeSettlement()
    assert s.accountBalances["Alice"]["USD"] == 100.
    assert s.accountBalances["Alice"]["EUR"] == 100.
    assert s.accountBalances["Bob"]["USD"] == 100.
    assert s.accountBalances["Bob"]["EUR"] == 200.
    assert s.ledger[txn1].currency == "USD"
    assert s.ledger[txn2].currency == "EUR"
    assert s.ledger[txn1].status.name == "SETTLED"
    assert s.ledger[txn2].status.name == "SETTLED"


def test_cross_currency_between_parties():
    s = MultiPartySettlement()

    # Alice pays Bob 100 USD
    s.addTransaction("Alice", "Bob", 100, "USD")
    # Bob pays Alice 200 EUR
    s.addTransaction("Bob", "Alice", 200, "EUR")

    positions = s.computeNetPositions()

    # Alice: -100 USD, +200 EUR
    assert positions["Alice"].balance["USD"] == -100
    assert positions["Alice"].balance["EUR"] == 200
    # Bob: +100 USD, -200 EUR
    assert positions["Bob"].balance["USD"] == 100
    assert positions["Bob"].balance["EUR"] == -200

    s.addBalance("Alice", "USD", 300)
    s.addBalance("Bob", "EUR", 300)
    s.executeSettlement()

    assert s.accountBalances["Alice"]["USD"] == 200.
    assert s.accountBalances["Alice"]["EUR"] == 200.
    assert s.accountBalances["Bob"]["USD"] == 100.
    assert s.accountBalances["Bob"]["EUR"] == 100.
    for txn in s.ledger.values():
        assert txn.status.name == "SETTLED"

def test_cycle_netting():
    s = MultiPartySettlement()

    # Alice → Bob 100
    s.addTransaction("Alice", "Bob", 100, "USD")
    # Bob → Charlie 100
    s.addTransaction("Bob", "Charlie", 100, "USD")
    # Charlie → Alice 100
    s.addTransaction("Charlie", "Alice", 100, "USD")

    positions = s.computeNetPositions()

    assert positions["Alice"].balance["USD"] == 0
    assert positions["Bob"].balance["USD"] == 0
    assert positions["Charlie"].balance["USD"] == 0

    s.addBalance("Alice", "USD", 100)
    s.executeSettlement()

    assert s.accountBalances["Alice"]["USD"] == 100.
    assert s.accountBalances["Bob"]["USD"] == 0.
    assert s.accountBalances["Charlie"]["USD"] == 0.

    for txn in s.ledger.values():
        assert txn.status.name == "SETTLED"


def test_partial_netting_reduction():
    s = MultiPartySettlement()

    # Alice → Bob 100
    s.addTransaction("Alice", "Bob", 100, "USD")
    # Bob → Alice 40
    s.addTransaction("Bob", "Alice", 40, "USD")

    positions = s.computeNetPositions()

    # Netting reduces Alice's obligation to 60
    assert positions["Alice"].balance["USD"] == -60
    assert positions["Bob"].balance["USD"] == 60

    s.addBalance("Alice", "USD", 100)
    s.executeSettlement()

    assert s.accountBalances["Alice"]["USD"] == 40.
    assert s.accountBalances["Bob"]["USD"] == 60.

    for txn in s.ledger.values():
        assert txn.status.name == "SETTLED"


def test_complex_partial_netting():
    s = MultiPartySettlement()

    # Alice → Bob 100
    txn1 = s.addTransaction("Alice", "Bob", 100, "USD")
    # Bob → Charlie 70
    txn2 = s.addTransaction("Bob", "Charlie", 70, "USD")
    # Charlie → Alice 30
    txn3 = s.addTransaction("Charlie", "Alice", 80, "USD")

    positions = s.computeNetPositions()

    # Alice: -100 +30 = -70
    assert positions["Alice"].balance["USD"] == -20
    # Bob: +100 -70 = +30
    assert positions["Bob"].balance["USD"] == 30
    # Charlie: +70 -30 = +40
    assert positions["Charlie"].balance["USD"] == -10

    s.addBalance("Alice", "USD", 100)
    s.executeSettlement()

    assert s.accountBalances["Alice"]["USD"] == 0.
    assert s.accountBalances["Bob"]["USD"] == 30.
    assert s.accountBalances["Charlie"]["USD"] == 70.
    assert s.ledger[txn1].status.name == "SETTLED"
    assert s.ledger[txn2].status.name == "SETTLED"
    assert s.ledger[txn3].status.name == "FAILED"


if __name__ == "__main__":
    test_single_currency_settlement()
    test_multi_currency_independence()
    test_cross_currency_between_parties()
    test_cycle_netting()
    test_partial_netting_reduction()
    test_complex_partial_netting()