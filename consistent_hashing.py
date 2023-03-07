import hashlib
from bisect import bisect, bisect_left, bisect_right


# using sha256 rn
def hash_fn(key, max_hashes):
    return int(hashlib.sha256(key.encode()).hexdigest(), 16) % max_hashes


class HashRing:
    def __init__(self, max_hashes, virtual_nodes):
        self.keys = []  # stores indexes on the HashRing where nodes are
        self.nodes = []  # stores the names of the nodes
        # nodes[i] is present on the HashRing at keys[i]
        self.max_hashes = max_hashes
        self.virtual_nodes = virtual_nodes

    def add_node(self, node):
        key = hash_fn(node, self.max_hashes)
        index = bisect(self.keys, key)

        if index > 0 and self.keys[index - 1] == key:
            raise Exception("HashRing Collision")

        self.keys.insert(index, key)  # insert "real" node
        self.nodes.insert(index, node)

        for i in range(1, self.virtual_nodes):  # insert virtual nodes
            virtual_key = (key + self.max_hashes // self.virtual_nodes * i) % self.max_hashes
            index = bisect(self.keys, virtual_key)
            self.keys.insert(index, virtual_key)
            self.nodes.insert(index, node)

        return key

    def remove_node(self, node):
        if len(self.keys) == 0:
            raise Exception("HashRing Empty")

        key = hash_fn(node, self.max_hashes)
        index = bisect_left(self.keys, key)

        if index >= len(self.keys) or self.keys[index] != key:
            raise Exception("Shard not in HashRing")

        self.keys.pop(index)  # remove "real" node
        self.nodes.pop(index)

        for i in reversed(range(1, self.virtual_nodes)):  # remove virtual nodes
            virtual_key = (key + self.max_hashes // self.virtual_nodes * i) % self.max_hashes
            index = bisect_left(self.keys, virtual_key)
            self.keys.pop(index)
            self.nodes.pop(index)

        return key

    def assign(self, val):  # returns which node a thing should be assigned to
        key = hash_fn(val, self.max_hashes)
        index = bisect_right(self.keys, key) % len(self.keys)
        return self.nodes[index]

    def print_ring(self):
        print(self.keys)
        print(self.nodes)


if __name__ == '__main__':
    nodes = ['node1', 'node5', 'node3', 'node4']
    nodes2 = ['node1', 'node3']

    hr = HashRing(10000, 3)  # can set to arbitrarily large number, int('f' * 32, 16), and any number of virtual nodes
    for node in nodes:
        hr.add_node(node)
    hr.print_ring()
    print(hr.assign("key1"))
    print(hr.assign("key2"))
    hr.remove_node(hr.assign('key1'))
    hr.print_ring()
    hr.add_node("node2")
    hr.print_ring()
    print(hr.assign("key1"))
    print(hr.assign("key2"))
