import heapq
import uuid
from collections import defaultdict, deque, namedtuple
from dataclasses import dataclass
from datetime import datetime
from threading import Lock
from typing import Any
from pprint import pprint

Timestamp = int | float
Limit = namedtuple("Limit", ["amount", "seconds"])


@dataclass
class Request:
    userId: str
    serviceId: str
    timestamp: Timestamp
    priority: int
    processed: bool = False
    reqId: str = None


class WindowRateLimiter:
    def __init__(self, globalLimit: Limit, burstLimit: int):
        self.usersLimit: dict[str, Limit] = {}
        self.servicesLimit: dict[str, Limit] = {}
        self.globalLimit: Limit = globalLimit
        self.burstLimit = burstLimit

        self.usersLock = defaultdict(Lock)
        self.servicesLock = defaultdict(Lock)
        self.globalLock = Lock()
        self.excessLock = Lock()

        self.usersWindow: defaultdict[str, deque[int]] = defaultdict(deque)
        self.servicesWindow: defaultdict[str, deque[int]] = defaultdict(deque)
        self.globalWindow: list[tuple[Timestamp, str]] = []  # (expiryTime, reqId)
        self.excessHeap: list[
            tuple[int, Timestamp, str]
        ] = []  # (priority, timestamp, reqId)
        self.storage: dict[str, Request] = {}

    def _clean_up_window(self, window: deque[int], now: int):
        while window and window[0] <= now:
            _ = window.popleft()

    def registerUserService(self, userId: str, serviceId: str, limit: Limit):
        self.usersLimit[userId] = limit
        self.servicesLimit[serviceId] = limit

    def request(
        self, userId: str, serviceId: str, timestamp: Timestamp, priority: int
    ) -> bool:
        if userId not in self.usersLimit or serviceId not in self.servicesLimit:
            return False

        timestamp = int(timestamp)
        now = timestamp
        req = Request(
            userId=userId,
            serviceId=serviceId,
            timestamp=timestamp,
            priority=priority,
            reqId=str(uuid.uuid4()),
        )

        with self.usersLock[userId]:
            userWindow = self.usersWindow[userId]
            self._clean_up_window(userWindow, now)

            userAllowed = (
                len(userWindow) < self.usersLimit[userId].amount + self.burstLimit
            )

        with self.servicesLock[serviceId]:
            serviceWindow = self.servicesWindow[serviceId]
            self._clean_up_window(serviceWindow, now)

            serviceAllowed = (
                len(serviceWindow)
                < self.servicesLimit[serviceId].amount + self.burstLimit
            )

        with self.globalLock:
            while self.globalWindow and self.globalWindow[0][0] < now:
                _, oldReqId = heapq.heappop(self.globalWindow)
                self.storage.pop(oldReqId, None)
            globalAllowed = (
                len(self.globalWindow) < self.globalLimit.amount + self.burstLimit
            )

        if not userAllowed or not serviceAllowed or not globalAllowed:
            with self.excessLock:
                heapq.heappush(
                    self.excessHeap, (req.priority, req.timestamp, req.reqId)
                )
                self.storage[req.reqId] = req

            return False

        limitAmount = min(
            self.globalLimit.seconds,
            self.usersLimit[userId].seconds,
            self.servicesLimit[serviceId].seconds,
        )
        expiryTime: Any = now + limitAmount
        with self.usersLock[userId], self.servicesLock[serviceId], self.globalLock:
            userWindow.append(expiryTime)
            serviceWindow.append(expiryTime)
            heapq.heappush(self.globalWindow, (expiryTime, req.reqId))
            req.processed = True
            self.storage[req.reqId] = req
        return True

    def processExcess(self):
        while True:
            with self.excessLock:
                if not self.excessHeap:
                    break

                _, _, reqId = heapq.heappop(self.excessHeap)

            req = self.storage.get(reqId)
            if not req or req.processed:
                continue

            if not self.request(req.userId, req.serviceId, req.timestamp, req.priority):
                now = int(datetime.now().timestamp())
                with self.excessLock:
                    heapq.heappush(
                        self.excessHeap, (req.priority, max(now, req.timestamp), req.reqId)
                    )
                break

    def showRequests(self) -> list[Request]:
        return list(self.storage.values())


limiter = WindowRateLimiter(globalLimit=Limit(5, 60), burstLimit=1)
limiter.registerUserService("user1", "serviceA", Limit(3, 60))

ts = datetime.now().timestamp()
for i in range(5):
    print(limiter.request("user1", "serviceA", ts + i, priority=i))

# Attempt to process excess requests later
limiter.processExcess()
pprint(limiter.showRequests())