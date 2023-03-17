import subprocess
import json
from time import sleep
import sys
from threading import Thread, Semaphore


TESTS=[]
SUBNETS = {"kv_subnet", "kv_subnet2", "kv_subnet3"}

def run(cmd: list[str], *args, **kwargs):
    cmdStr = " ".join(cmd)
    return subprocess.run(["/bin/bash", "-c", cmdStr], *args, **kwargs)

# decarator! use on test scripts
def test(t):
    TESTS.append(t)
    return t

def runNodes(n: int):
    run(["tests/run_nodes.sh", str(n)])

def stopNodes(n: int):
    run(["tests/stop_nodes.sh", str(n)])

def get(client, key, who: int) -> dict:
    res = run(["tests/get.sh", client, key, str(who)], capture_output=True).stdout.decode()
    return json.loads(res)

def put(client, key, who: int, key_file: str) -> dict:
    with open(key_file) as out:
        res = run(["tests/put.sh", client, key, str(who)], stdin=out, capture_output=True).stdout.decode()
    return json.loads(res)

def getKeys(client, who:int) -> dict:
    res = run(["tests/get_keys.sh", client, str(who)], capture_output=True).stdout.decode()
    return json.loads(res)

def getView(who: int) -> dict:
    res = run(["tests/get_view.sh", str(who)], capture_output=True).stdout.decode()
    return json.loads(res)

def partition(groups: list[list[int]]):
    for i in range(len(groups) - 1):
        others = []
        for other in groups[i+1:]:
            others.extend(other)
        for node in groups[i]:
            args = ["tests/partition.sh", str(node)]
            args.extend(map(str, others))
            run(args)

def heal():
    run(["tests/heal.sh"])

def view(l: list[int], nShards: int):
    args = ["tests/view.sh"]
    args.append(str(nShards))
    args.extend(map(str,l))
    run(args)


def run_tests(tests: list[str]):
    res = {}
    for t in TESTS:
        if tests and t.__name__ not in tests:
            continue
        try:
            res[t.__name__] = t()
            print(".", end="", flush=True)
        except Exception as e:
            res[t.__name__] = str(e)
            stopNodes(9)
            print("!", end="", flush=True)
    print("")
    passCount = 0
    for k,v in res.items():
        if v == True or v is None:
            passCount += 1
            print(f"{k}: PASS!")
            continue
        print(f"{k}: {v}")
    print(f"{passCount} out of {len(res)} tests passing")

@test
def testUnderPartition():
    runNodes(6)
    view([1,2,3,4,5,6], 1)
    sleep(1)
    partition([[1,2,3],[4,5,6]])
    put("c1", "k1", 1, "tests/keys/small-key1")
    assert "val" in get("c1", "k1", 2), "val not in good half"

    assert "val" not in get("c2", "k1", 4), "val in bad half"

    put("c3", "k2", 5, "tests/keys/small-key2")
    assert "val" in get("c3", "k2", 6), "val2 not in good half"
    assert "val" not in get("c2", "k2", 1)
    heal()

    assert "val" in get("c1", "k1", 4), "partition not healed"
    assert "val" in get("c3", "k2", 1), "partition not healed 2"

    stopNodes(6)

@test
def testSplitKvsView():
    runNodes(6)
    view([1,2,3], 2)
    view([4,5,6], 2)


    put("c1", "k2", 1, "tests/keys/small-key1")
    put("c4", "k3", 4, "tests/keys/small-key1")

    assert "val" not in get("c1", "k3", 1), "view split not handled correctly 3"
    assert "val" not in get("c4", "k2", 4), "view split not handled correctly 4"

    assert "val" not in get("c1", "k3", 2), "view split metadata not handled correctly 1"
    assert "val" not in get("c4", "k2", 5), "view split metadata not handled correctly 2"
    sleep(2)
    view([1, 2, 3, 4, 5, 6], 3)
    sleep(2)
    for i in range(1,7):
        assert len(getView(i)['view']) == 3, "view is consistant"

    assert "val" in get("c4", "k3", 2), "split views healed 1"
    assert "val" in get("c1", "k2", 5), "split views healed 2"

    count = {}
    for node in range(1,7):
        res = getKeys(f"c{node}2", node)
        count[res["shard_id"]] = res["count"]
    
    

    assert sum([n for n in count.values()]) == 2, f"has {count} keys"

    stopNodes(6)

@test
def testCausalConsistnacyUnderPartition():
    runNodes(3)
    view( [1,2,3], 3 )
    partition( [[1],[2,3]] )
    heal()
    stopNodes(3)

@test 
def testViewUnderPartition():
    runNodes(3)
    partition( [[1],[2,3]] )
    sleep(1)
    view([1,2,3], 2) #view sent to 1
    assert len( getView(3)["view"] ) == 0, "partition didnt work" 
    sleep(1)
    heal()
    sleep(1)
    assert "error" not in put("c1", "k1", 2, "tests/keys/small-key1")
    assert "val" in get("c1", "k1", 3)
    assert len( getView(2)['view'] ) == 2 
    stopNodes(3)

@test
def testNodeDown():
    runNodes(3)
    view([1,2,3,4], 2)
    put("c1", "key", 1, "tests/keys/small-key1")
    sleep(1)
    stopNodes(1)
    assert "val" in get("c1", "key", 3), "works"
    assert len( getView(2)["view"] ) == 2, "works 2"
    view([2,3], 1)
    sleep(1)
    assert len( getView(2)["view"] ) == 1, "works 3"

    assert "val" in get("c1", "key", 3), "data not kept"

    stopNodes(3)

@test
def testAddShardToView():
    runNodes(5)
    view([1,2,3,4,5], 2)
    sleep(1)
    nodeView = getView(1)
    assert len(nodeView["view"]) == 2
    for i in range(2,6):
        assert nodeView == getView(i), "view consistency"
    view([1,2,3,4,5], 4)
    sleep(1)
    nodeView = getView(1)
    assert len(nodeView["view"]) == 4
    for i in range(2,6):
        assert nodeView == getView(i), "view consistency after change"
    stopNodes(5)
    
@test
def testRemoveShardFromView():
    runNodes(5)
    view([1,2,3,4,5], 3)
    sleep(1)
    nodeView = getView(1)
    assert len(nodeView["view"]) == 3
    for i in range(2,6):
        assert nodeView == getView(i), "view consistency"
    view([1,2,3,4,5], 2)
    sleep(1)
    nodeView = getView(1)
    assert len(nodeView["view"]) == 2
    for i in range(2,6):
        assert nodeView == getView(i), "view consistency after change"
    stopNodes(5)

def viewCorrect(expectedShardCount: int, expectedNodeCount: int) -> None:
    for n in range(1, expectedNodeCount):
        nodeView = getView(n)
        assert len(nodeView["view"]) == expectedShardCount, "wrong shard count"
        minShardSize = 1000000
        maxShardSize = 0
        for shard in nodeView["view"]:
            l = len(shard["nodes"])
            if l < minShardSize:
                minShardSize = l
            if l > maxShardSize:
                maxShardSize = l
        assert abs(maxShardSize - minShardSize) <= 1
        for i in range(1, expectedNodeCount):
            if i == n:
                continue
            node2view = getView(i)
            assert nodeView == node2view, f"{i} view wrong"
            nodeSet = set()
            for shard in node2view["view"]:
                for n in shard["nodes"]:
                    assert n not in nodeSet, "node in 2 shards"
                    nodeSet.add(n)
            assert len(nodeSet) == expectedNodeCount, "incorrect node count"
                        
            

@test
def testAddNodesToView():
    runNodes(6)
    view([1,2,3], 2)
    sleep(1)
    view([1,2,3,4,5,6], 2)
    sleep(1)
    viewCorrect(2,6)
    stopNodes(6)

@test
def testRemoveNodesFromView():
    runNodes(6)
    view([1,2,3,4,5,6], 2)
    sleep(1)
    view([1,2,3], 2)
    sleep(1)
    viewCorrect(2,3)
    stopNodes(6)

@test 
def testAddNodesAddShards():
    runNodes(6)
    view([1,2,3], 2)
    sleep(1)
    view([1,2,3,4,5,6], 3)
    sleep(1)
    viewCorrect(3,6)
    stopNodes(6)

@test 
def testAddNodesRemoveShards():
    runNodes(6)
    view([1,2,3], 3)
    sleep(1)
    view([1,2,3,4,5,6], 2)
    sleep(1)
    viewCorrect(2,6)
    stopNodes(6)

@test 
def testRemoveNodesRemoveShards():
    runNodes(6)
    view([1,2,3,4,5,6], 3)
    sleep(1)
    view([1,2,3], 2)
    sleep(1)
    viewCorrect(2,3)
    stopNodes(6)

@test 
def testRemoveNodesAddShards():
    runNodes(6)
    view([1,2,3,4,5,6], 2)
    sleep(1)
    view([1,2,3], 3)
    sleep(1)
    viewCorrect(3,3)
    stopNodes(6)

@test
def testAddKeyRemoveShards():
    runNodes(6)
    view([1,2,3,4,5,6], 6)
    sleep(1)

    put("c1", "testkey1", 3, "tests/keys/small-key1")
    put("c1", "testkey2", 4, "tests/keys/small-key1")
    put("c1", "testkey3", 5, "tests/keys/small-key1")

    view([1,2,3,4,5,6], 1)
    sleep(5)
    for node in range(1,7):
        for key in range(1,3):
            res = get("c2", f"testkey{key}", node)
            assert "error" not in res, "bad key sharing"
    stopNodes(6)

@test
def testAddKeyAddShards():
    runNodes(6)
    view([1,2,3,4,5,6], 1)
    sleep(1)

    put("c1", "testkey1", 3, "tests/keys/small-key1")
    put("c1", "testkey2", 4, "tests/keys/small-key1")
    put("c1", "testkey3", 5, "tests/keys/small-key1")

    view([1,2,3,4,5,6], 6)
    sleep(5)
    for node in range(1,7):
        for key in range(1,3):
            res = get("c2", f"testkey{key}", node)
            assert "error" not in res, "bad key sharing"
    stopNodes(6)

def addManyKeysWorker(node: int, count: int):
    for i in range(count):
        put(f"c{node}", f"{i}-{node}", node, "tests/keys/small-key2")
    return None
        

@test
def testAddManyKeys():
    runNodes(9)
    v = [1,2,3,4,5,6,7,8,9]
    view(v, 9)
    sleep(1)
    threads: list[Thread] = []
    nKeysPerNode = 100
    for node in v:
        threads.append(Thread(target=addManyKeysWorker, args=(node, nKeysPerNode), daemon=True))
    for t in threads:
        t.start()
    sleep(1)
    for t in threads:
        t.join()
    assert sum([getKeys(f"c{node}new", node)["count"] for node in v]) == 9 * nKeysPerNode, "1:1 view no work"

    view(v, 3)
    sleep(3)
    #3 nodes per shard, each shard should have +-300 keys, 300 * 9 = 2700
    viewCorrect(3,9)
    keys = [getKeys(f"c{node}new2", node) for node in v]
    shards: dict[str, set] = {}
    for k in keys:
        id = k["shard_id"]
        if id in shards:
            shards[id].update(k["keys"])
        else:
            shards[id] = set(k["keys"])
    summ = 0
    for shard in shards.values():
        summ += len(shard)
        for shard2 in shards.values():
            if shard is shard2:
                continue
            assert shard.isdisjoint(shard2), "shards share keys"
    assert summ == 9 * nKeysPerNode, "extra keys present"
    # print([getKeys(f"c{node}new2", node)["count"] for node in v])
    assert (s:=sum([getKeys(f"c{node}new2", node)["count"] for node in v])) == 27 * nKeysPerNode, f"3:1 view no work, {s}"
    stopNodes(9)

@test
def testKeyReshuffle():
    runNodes(9)
    view([1,2,3,4,5,6,7,8,9], 7)
    sleep(1)
    nKeys = 5
    for i in range(nKeys):
        put("c1", f"key{i}", 4, "tests/keys/small-key1")
    sleep(2)
    print(getView(3))
    view([1,2,3], 2)
    sleep(2)
    print(getView(1))
    print(getKeys("c2",1))
    print(getKeys("c3",2))
    print(getKeys("c4",3))

    for node in range(1,4):
        for i in range(nKeys):
            assert "error" not in get(f"c{node}{i}", f"key{i}", node), f"key not found {node} key{i}!"
    for node in range(1,4):
        for i in range(nKeys):
            assert "error" not in get(f"c1", f"key{i}", node), f"key not found {node} key{i}"  

    stopNodes(9)

@test
def testGossip():
    runNodes(4)
    view([1,2,3,4], 1)
    sleep(2)
    partition([[1],[2,3,4]])
    nKeys = 5
    for i in range(nKeys):
        put("c1", f"key-{i}", 1, "tests/keys/small-key1")
    for i in range(nKeys):
        put("c1", f"key2-{i}", 2, "tests/keys/small-key1")
    heal()
    sleep(5)
    for i in range(1,5):
        assert getKeys(f"c{i}new", i)["count"] == 10
    stopNodes(4)

if __name__ == "__main__":
    run_tests(sys.argv[1:])
    
