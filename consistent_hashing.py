import hashlib
from bisect import bisect, bisect_left, bisect_right


# using sha256 rn
def hash_fn(key, max_hashes):
    return int(hashlib.sha256(key.encode()).hexdigest(), 16) % max_hashes


class HashRing:
    def __init__(self, max_hashes, virtual_shards):
        self.keys = []  # stores indexes on the HashRing where shards are
        self.shards = []  # stores the names of the shards
        # shards[i] is present on the HashRing at keys[i]
        self.max_hashes = max_hashes
        self.virtual_shards = virtual_shards  # number of shards in addition to the 'real' shard.

    def add_shard(self, shard):
        key = hash_fn(shard, self.max_hashes)
        index = bisect(self.keys, key)

        if index > 0 and self.keys[index - 1] == key:
            raise Exception("HashRing Collision")

        self.keys.insert(index, key)  # insert "real" shard
        self.shards.insert(index, shard)

        # since the hashing algorithm is uniformly random. Take the hash of the hash self.virtual_shards times and also
        # add those to the hashring under the shard name
        for i in range(self.virtual_shards):  # insert virtual shards
            virtual_key = hash_fn(str(key), self.max_hashes)
            index = bisect(self.keys, virtual_key)
            self.keys.insert(index, virtual_key)
            self.shards.insert(index, shard)
            key = str(virtual_key)

        return key

    def remove_shard(self, shard):
        if len(self.keys) == 0:
            raise Exception("HashRing Empty")

        key = hash_fn(shard, self.max_hashes)
        index = bisect_left(self.keys, key)

        if index >= len(self.keys) or self.keys[index] != key:
            raise Exception("Shard not in HashRing")

        for i in reversed(range(len(self.keys))):  # remove shards
            if self.shards[i] == shard:
                self.keys.pop(i)
                self.shards.pop(i)

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

    hr = HashRing(int('f' * 32, 16), 1000)  # can set max_hashes to arbitrarily large number, int('f' * 32, 16)
    for shard in shards:
        hr.add_shard(shard)

    def check_key_distribution(hashring):
        count = {}
        for i in range(10000):
            count[hashring.assign("key" + str(i))] = count.get(hashring.assign("key" + str(i)), 0) + 1
        return count
    print(check_key_distribution(hr))
    # hr.print_ring()
    print(hr.assign('key1'))
    hr.remove_shard(hr.assign('key1'))
    # hr.print_ring()
    print(check_key_distribution(hr))
    # hr.print_ring()
    # hr.add_shard("shard2")
    # hr.print_ring()
    # print(hr.assign("key1"))
    # print(hr.assign("key2"))
    # when you reshard you have to take all keys stored in a shard and check their assignment again. Then reassign them
    # to their appropriate shard.