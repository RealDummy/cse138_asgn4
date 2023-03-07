import hashlib
from bisect import bisect, bisect_left, bisect_right


# using sha256 rn
def hash_fn(key, max_hashes):
    return int(hashlib.sha256(key.encode()).hexdigest(), 16) % max_hashes


class HashRing:
    def __init__(self, max_hashes, num_shards):
        self.keys = []  # stores indexes on the HashRing where shards are
        self.shards = []  # stores the names of the shards
        # shards[i] is present on the HashRing at keys[i]
        self.max_hashes = max_hashes
        self.num_shards = num_shards

    def add_shard(self, shard):
        key = hash_fn(shard, self.max_hashes)
        index = bisect(self.keys, key)

        if index > 0 and self.keys[index - 1] == key:
            raise Exception("HashRing Collision")

        self.keys.insert(index, key)  # insert "real" shard
        self.shards.insert(index, shard)

        for i in range(1, self.num_shards):  # insert virtual shards
            virtual_key = (key + self.max_hashes // self.num_shards * i) % self.max_hashes
            index = bisect(self.keys, virtual_key)
            self.keys.insert(index, virtual_key)
            self.shards.insert(index, shard)

        return key

    def remove_shard(self, shard):
        if len(self.keys) == 0:
            raise Exception("HashRing Empty")

        key = hash_fn(shard, self.max_hashes)
        index = bisect_left(self.keys, key)

        if index >= len(self.keys) or self.keys[index] != key:
            raise Exception("Shard not in HashRing")

        self.keys.pop(index)  # remove "real" shard
        self.shards.pop(index)

        for i in reversed(range(1, self.num_shards)):  # remove virtual shards
            virtual_key = (key + self.max_hashes // self.num_shards * i) % self.max_hashes
            index = bisect_left(self.keys, virtual_key)
            self.keys.pop(index)
            self.shards.pop(index)

        return key

    def assign(self, val):  # returns which shard a thing should be assigned to
        key = hash_fn(val, self.max_hashes)
        index = bisect_right(self.keys, key) % len(self.keys)
        return self.shards[index]

    def print_ring(self):
        print(self.keys)
        print(self.shards)


if __name__ == '__main__':
    shards = ['shard1', 'shard5', 'shard3', 'shard4']
    shards2 = ['shard1', 'shard3']

    hr = HashRing(10000, 3)  # can set to arbitrarily large number, int('f' * 32, 16), and any number of virtual shards
    for shard in shards:
        hr.add_shard(shard)
    hr.print_ring()
    print(hr.assign("key1"))
    print(hr.assign("key2"))
    hr.remove_shard(hr.assign('key1'))
    hr.print_ring()
    hr.add_shard("shard2")
    hr.print_ring()
    print(hr.assign("key1"))
    print(hr.assign("key2"))
