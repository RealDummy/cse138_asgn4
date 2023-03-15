import subprocess
import json
from time import sleep
import sys


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
        except Exception as e:
            res[t.__name__] = str(e)
            stopNodes(9)
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

    view([1, 2, 3, 4, 5, 6], 3)

    sleep(5)
    for i in range(1,7):
        assert len(getView(i)['view']) == 3

    assert "val" in get("c4", "k3", 2), "split views healed 1"
    assert "val" in get("c1", "k2", 5), "split views healed 2"

    assert getKeys("c5", 2)["count"] == 3
    assert getKeys("c5", 5)["count"] == 3

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
    nodeView = getView(1)
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
    for i in range(2, expectedNodeCount):
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
    print(getView(3))
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


if __name__ == "__main__":
    run_tests(sys.argv[1:])
    
