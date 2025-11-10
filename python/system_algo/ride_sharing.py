# Ride Sharing Platform Analytics System

# Design a system to track ongoing and historical rides in a large-scale ride-sharing platform.

# class RideSharingAnalytics:
#     def __init__(self):
#         pass

#     def start_ride(self, ride_id: int, driver_id: int, rider_id: int, city: str, start_time: int) -> None:
#         """Records a new ride start event. Each ride_id is unique."""

#     def end_ride(self, ride_id: int, end_time: int, distance_km: float) -> None:
#         """Marks the ride as completed and records total distance."""

#     def cancel_ride(self, ride_id: int, cancel_time: int) -> None:
#         """Cancels a ride before completion."""

#     def get_active_rides(self, city: Optional[str] = None) -> int:
#         """Returns the number of currently active (ongoing) rides, optionally filtered by city."""

#     def get_total_distance(self, driver_id: int) -> float:
#         """Returns the total distance driven by a given driver across all completed rides."""

#     def get_top_drivers_by_distance(self, top_n: int) -> List[Tuple[int, float]]:
#         """Returns top N drivers ranked by total distance driven."""

#     def get_driver_stats(self, driver_id: int) -> Tuple[int, int, float]:
#         """
#         Returns (num_completed_rides, num_canceled_rides, total_distance).
#         """

#     def get_city_summary(self, city: str) -> Tuple[int, int, float]:
#         """
#         Returns (num_completed_rides, num_active_rides, total_distance).
#         """

# ⚙️ Requirements & Constraints
# - Multiple rides can start or end concurrently (consider locking granularity).
# - end_ride and cancel_ride should not apply twice for the same ride.
# - A canceled ride does not contribute to distance or completion stats.
# - get_top_drivers_by_distance() must remain efficient as the number of drivers scales.
# - Memory usage should scale with the number of active rides + drivers, not total ride history.
# - Assume timestamps are increasing but not necessarily strictly (some late events possible).
from collections import defaultdict
from dataclasses import dataclass
from heapdict import heapdict
from threading import Lock
import heapq

@dataclass
class Ride:
    ride_id: int
    driver_id: int
    rider_id: int
    city: str
    start_time: int

class RideSharingAnalytics:
    def __init__(self,
        num_locks: int=128,
        top_k_default: int=100,
    ):
        self.top_k_default = top_k_default
        self.num_locks = num_locks
        self.active_rides_shards: list[dict[int, Ride]] = [{} for _ in range(num_locks)]
        self.driver_stats_shards = [defaultdict(
            lambda: {
                "num_completed_rides": 0,
                "num_canceled_rides": 0,
                "total_distance": 0.,
            }) for _ in range(num_locks)]
        self.city_stats_shards = [defaultdict(
            lambda: {
                "num_completed_rides": 0,
                "num_active_rides": 0,
                "total_distance": 0.,
            }
        ) for _ in range(num_locks)]

        self.total_city_stats = {
            "num_completed_rides": 0,
            "num_active_rides": 0,
            "total_distance": 0.,
        }
        self.top_k_drivers = heapdict()

        self.active_rides_locks = [Lock() for _ in range(num_locks)]
        self.driver_stats_locks = [Lock() for _ in range(num_locks)]
        self.city_stats_locks = [Lock() for _ in range(num_locks)]
        self.analytics_lock = Lock()
        self.top_k_drivers_lock = Lock()

    def _get_active_ride_shard_and_lock(self, ride_id: int):
        shard_index = hash(ride_id) % self.num_locks
        return (
            self.active_rides_shards[shard_index],
            self.active_rides_locks[shard_index],
        )
    
    def _get_driver_stats_shard_and_lock(self, driver_id: int):
        shard_index = hash(driver_id) % self.num_locks
        return (
            self.driver_stats_shards[shard_index],
            self.driver_stats_locks[shard_index],
        )

    def _get_city_stats_shard_and_lock(self, city: str):
        shard_index = hash(city) % self.num_locks
        return (
            self.city_stats_shards[shard_index],
            self.city_stats_locks[shard_index],
        )

    def _update_top_k_drivers(self, driver_id:int, total_distance: float):
        with self.top_k_drivers_lock:
            if (
                len(self.top_k_drivers) < self.top_k_default or
                driver_id in self.top_k_drivers
            ):
                self.top_k_drivers[driver_id] = total_distance
            else:
                _, lowest_distance = self.top_k_drivers.peekitem()
                if total_distance > lowest_distance:
                    self.top_k_drivers.popitem()
                    self.top_k_drivers[driver_id] = total_distance

    def start_ride(self, ride_id: int, driver_id: int, rider_id: int, city: str, start_time: int) -> None:
        """Records a new ride start event. Each ride_id is unique."""

        ride = Ride(
            ride_id, driver_id, rider_id, city, start_time
        )
        active_ride_shard, lock = self._get_active_ride_shard_and_lock(ride_id)
        with lock:
            if ride_id not in active_ride_shard:
                active_ride_shard[ride_id] = ride
            else:
                return
        
        city_stats_shard, lock = self._get_city_stats_shard_and_lock(city)
        with lock:
            city_stats = city_stats_shard[city]
            city_stats["num_active_rides"] += 1

        with self.analytics_lock:
            self.total_city_stats["num_active_rides"] += 1

    def end_ride(self, ride_id: int, end_time: int, distance_km: float) -> None:
        """Marks the ride as completed and records total distance."""
        active_ride_shard, lock = self._get_active_ride_shard_and_lock(ride_id)
        with lock:
            if ride_id not in active_ride_shard:
                return

            ride = active_ride_shard[ride_id]
            del active_ride_shard[ride_id]

        driver_stats_shard, lock = self._get_driver_stats_shard_and_lock(ride.driver_id)
        with lock:
            driver_stats = driver_stats_shard[ride.driver_id]
            driver_stats["num_completed_rides"] = driver_stats["num_completed_rides"] + 1
            driver_stats["total_distance"] += distance_km
            total_distance = driver_stats["total_distance"]

        city_stats_shard, lock = self._get_city_stats_shard_and_lock(ride.city)
        with lock:
            city_stats = city_stats_shard[ride.city]
            city_stats["num_completed_rides"] += 1
            city_stats["num_active_rides"] = max(0, city_stats["num_active_rides"] - 1)
            city_stats["total_distance"] += distance_km

        with self.analytics_lock:
            self.total_city_stats["num_completed_rides"] += 1
            self.total_city_stats["num_active_rides"] = max(0, self.total_city_stats["num_active_rides"] - 1)
            self.total_city_stats["total_distance"] += distance_km

        self._update_top_k_drivers(ride.driver_id, total_distance)            

    def cancel_ride(self, ride_id: int, cancel_time: int) -> None:
        """Cancels a ride before completion."""
        active_ride_shard, lock = self._get_active_ride_shard_and_lock(ride_id)
        with lock:
            if ride_id not in active_ride_shard:
                return
        
            ride = active_ride_shard[ride_id]
            del active_ride_shard[ride_id]

        driver_stats_shard, lock = self._get_driver_stats_shard_and_lock(ride.driver_id)
        with lock:
            driver_stats = driver_stats_shard[ride.driver_id]
            driver_stats["num_canceled_rides"] += 1

        city_stats_shard, lock = self._get_city_stats_shard_and_lock(ride.city)
        with lock:
            city_stats = city_stats_shard[ride.city]
            city_stats["num_active_rides"] = max(0, city_stats["num_active_rides"] - 1)

        with self.analytics_lock:
            self.total_city_stats["num_active_rides"] = max(0, self.total_city_stats["num_active_rides"] - 1)

    def get_active_rides(self, city: str | None) -> int:
        """Returns the number of currently active (ongoing) rides, optionally filtered by city."""
        if city is None:
            with self.analytics_lock:
                return int(self.total_city_stats["num_active_rides"])

        city_stats_shard, lock = self._get_city_stats_shard_and_lock(city)
        with lock:
            if city not in city_stats_shard:
                return 0
            city_stats = city_stats_shard[city]
            return int(city_stats["num_active_rides"])
            
    def get_total_distance(self, driver_id: int) -> float:
        """Returns the total distance driven by a given driver across all completed rides."""

        driver_stats_shard, lock = self._get_driver_stats_shard_and_lock(driver_id)
        with lock:
            if driver_id not in driver_stats_shard:
                return 0.

            driver_stats = driver_stats_shard[driver_id]
            return driver_stats["total_distance"]

    def get_top_drivers_by_distance_on_demand(self, k: int) -> list[tuple[int, float]]:
        """Returns top N drivers ranked by total distance driven."""

        top_k = []
        for i in range(self.num_locks):
            driver_stats_shard = self.driver_stats_shards[i]
            lock = self.driver_stats_locks[i]
            with lock:
                cur_shard = dict(driver_stats_shard)
            for driver_id, driver_stats in cur_shard.items():
                total_distance = driver_stats["total_distance"]
                if len(top_k) < k:
                    heapq.heappush(top_k, (total_distance, driver_id))
                elif total_distance > top_k[0][0]:
                    heapq.heapreplace(top_k, (total_distance, driver_id))

        return [(driver_id, distance) for distance, driver_id in sorted(top_k, reverse=True)]

    def get_top_drivers_by_distance_computed(self, k: int) -> list[tuple[int, float]]:
        """Returns top N drivers ranked by total distance driven."""
        with self.top_k_drivers_lock:
            top_k_drivers = dict(self.top_k_drivers)

        return sorted(
            top_k_drivers.items(),
            key=lambda driver_id_distance: driver_id_distance[1],
            reverse=True,
        )[:k]
                    
    def get_driver_stats(self, driver_id: int) -> tuple[int, int, float]:
        """
        Returns (num_completed_rides, num_canceled_rides, total_distance).
        """
        driver_stats_shard, lock = self._get_driver_stats_shard_and_lock(driver_id)
        with lock:
            if driver_id not in driver_stats_shard:
                return (0, 0, 0.)

            driver_stats = driver_stats_shard[driver_id]
            return (
                int(driver_stats["num_completed_rides"]),
                int(driver_stats["num_canceled_rides"]),
                float(driver_stats["total_distance"]),
            )

    def get_city_summary(self, city: str) -> tuple[int, int, float]:
        """
        Returns (num_completed_rides, num_active_rides, total_distance).
        """

        city_stats_shard, lock = self._get_city_stats_shard_and_lock(city)
        with lock:
            if city not in city_stats_shard:
                return (0, 0, 0.)

            city_stats = city_stats_shard[city]
            return (
                int(city_stats["num_completed_rides"]),
                int(city_stats["num_active_rides"]),
                float(city_stats["total_distance"]),
            )
