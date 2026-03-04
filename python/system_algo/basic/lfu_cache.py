
from heapdict import heapdict

class BasicLFUCache:
    def __init__(self, capacity: int) -> None:
        self.storage = {}
        self.cnts = heapdict()
        self.capacity = capacity


    def set(self, key: str, value):
        if key in self.storage:
            self.storage[key] = value
            return True

        if len(self.storage) >= self.capacity:
            lf_key, _ = self.cnts.popitem()
            del self.storage[lf_key]

        self.storage[key] = value
        self.cnts[key] = 1
        return True

    def get(self, key: str):
        if key in self.storage:
            self.cnts[key] = self.cnts[key] + 1
            return self.storage[key]

        raise KeyError("key does not exist")

    def delete(self, key: str):
        if key not in self.storage and key not in self.cnts:
            return False

        self.storage.pop(key, None)
        self.cnts.pop(key, None)
        return True

    def usage(self):
        return list(self.cnts.items())

            
def test_simple():

    cache = BasicLFUCache(3)
    cache.set("a", value=1)
    cache.set("b", value=2)
    cache.set("c", value="a")

    assert cache.get("c") == "a"
    assert cache.get("b") == 2
    assert sorted(cache.usage(), key=lambda kv: (kv[1], kv[0])) == [("a", 1), ("b", 2), ("c", 2)]

    cache.set("d", "b")
    assert sorted(cache.usage(), key=lambda kv: (kv[1], kv[0])) == [("d", 1), ("b", 2), ("c", 2)]

    cache.set("b", 3)
    assert sorted(cache.usage(), key=lambda kv: (kv[1], kv[0])) == [("d", 1), ("b", 2), ("c", 2)]
    assert cache.get("b") == 3


if __name__ == "__main__":
    test_simple()