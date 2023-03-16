
from flask import Flask, request, abort
import os
import sys
import requests
import pickle
from time import time
from random import randrange
import asyncio
import json
import httpx

from kvs import Kvs, KvsNode, getLargerNode
from background import Executor, broadcastOne, broadcastAll
from operations import OperationGenerator, Operation
from causal import getData, putData, deleteData
from consistent_hashing import HashRing
from typing import Coroutine, Any
from key_reshuffle import remove_shards, add_shards, solveViewChange 


# need startup logic when creating a replica (broadcast?)
NAME = os.environ.get('ADDRESS')  # get IP and port
if not NAME:  # if no ADDRESS exit with return value '1'
    sys.exit('1')
PORT = 8080
DATA = Kvs()
BGE = Executor()
OPGEN = OperationGenerator(NAME)

nodes = [] # hold list of node in the cluster
operations = []
initialized = False
associated_nodes:dict[str, list[str]] = {} # hold shard id and nodes associated with it
current_shard_id = None
reshuffle = False

hashRing = HashRing(2**64, 2000)

app = Flask(__name__)

# kvs/admin/view - GET, PUT, DELETE
@app.route('/kvs/admin/view', methods= ['PUT'])
async def putview():
    global nodes, initialized
    myjson = request.get_json(silent=True)
    if (myjson == None):
        return {"error": "bad request"}, 400
    if (myjson.get("nodes") == None):
        return {"error": "bad request"}, 400
    if (myjson.get("num_shards") == None):
        return {"error": "bad request"}, 400
    initialized = True


    numshards = int(myjson["num_shards"]) #Number of Shards
    nodelist = myjson["nodes"] #(Temporary Variable) List of nodes

    if numshards > len(nodelist):
        return {"bruh": "too many nodes"}, 400

    global associated_nodes, current_shard_id, nodes

    # if len(associated_nodes):
    #     old_nodelist = []
    #     for key in associated_nodes:
    #         for n in associated_nodes[key]:
    #             old_nodelist.append(n)

        # num_old_shards = len(associated_nodes.keys())
        # # remove a shard -- move nodes in that shard to another shard
        # if numshards < num_old_shards:
        #     current_shard_id = remove_shards(num_old_shards, numshards, associated_nodes, hashRing, nodelist, NAME)

        # # FIXME: need to do
        # elif numshards > num_old_shards:
        #     add_shards(num_old_shards, numshards, associated_nodes, hashRing, nodelist, NAME)
        # else:
        #     # when list of new nodes is different than list of old nodes but num_shard stay the same
        #     print()
    associated_nodes, nodesToInform = solveViewChange(associated_nodes, nodelist, numshards)
    assert len(associated_nodes) == numshards
    for shardId, nodesInShard in associated_nodes.items():
        if NAME in nodesInShard:
            current_shard_id = shardId
            nodes = nodesInShard.copy()
    nodes.remove(NAME)


    await broadcastAll("PUT", list(nodesToInform), "/update_view", data=associated_nodes, timeout=5)

    return "OK", 200

@app.route('/kvs/admin/view', methods=['GET'])
def getview():
    l = []
    for shard in associated_nodes:
        l.append({'shard_id': str(shard), 'nodes': associated_nodes[shard]})
    return ({'view': l}), 200

@app.route('/kvs/admin/view', methods=['DELETE'])
def deleteFromViewEndpoint():
    return delete_node()


def delete_node():
    global initialized, DATA
    if not initialized:
        return {"error": "uninitialized"}, 418

    nodes.clear()
    associated_nodes.clear()
    hashRing.clear()
    DATA = Kvs()
    initialized = False
    return "", 200

def checkjson(myjson):
    try:
        json.loads(myjson)
    except ValueError as e:
        return False
    return True

@app.route('/update_view', methods= ['PUT'])
async def update_kvs_view():
    global DATA, nodes, initialized, associated_nodes, current_shard_id, hashRing, reshuffle
    reshuffle = True
    d = request.json
    associated_nodes = d
    hashRing.clear()
    current_shard_id = None
    for k,v in associated_nodes.items():
        hashRing.add_shard(k)
        if NAME in v:
            current_shard_id = k

    dataToSend: dict[str, dict[str, dict]] = {}
    for k in DATA.get_all_keys():
        shard_id, hash = hashRing.assign(k)
        if shard_id == current_shard_id:
            continue
        if shard_id in dataToSend:
            dataToSend[shard_id].update( {k: DATA.get(k).asDict()} )
        else:
            dataToSend[shard_id] = {k: DATA.get(k).asDict()}

    futures: list[Coroutine[Any, Any, tuple[str, int] | None]] = []
    for shardId, shardData in dataToSend.items():
        futures.append(asyncio.create_task( broadcastOne(
            "PUT",
            associated_nodes[shardId],
            "/reshuffle",
            shardData,
            20,
        ) ))
    if futures:
        done, pending = await asyncio.wait(futures)
        print(done, pending) #debug
    # when node is not in any shard -- send keys away before deleting
    if current_shard_id == None:
        # rehash_key_send_to_new_shard(DATA, hashRing, current_shard_id, associated_nodes)
        delete_node()
        return "OK", 200

    #rehash_key_send_to_new_shard(DATA, hashRing, current_shard_id, associated_nodes)

    nodes = associated_nodes[current_shard_id].copy()
    nodes.remove(NAME)

    initialized = True
    reshuffle = False
    return "OK", 200

@app.route('/reshuffle', methods=['PUT'])
def reshuffle_key():
    data = request.json
    for d in data.keys():
        kvs_node = KvsNode.fromDict(data[d])
        DATA.put(d, kvs_node)
    return "OK"

async def try_send_new_view(node, view):
    tries = 0
    while True:
        if tries == 100 or node not in nodes:
            break
        await asyncio.sleep(1)
        tries += 1
        url = f'http://{node}/update_kvs'
        pickled_data = pickle.dumps({"view": view, "data": DATA})
        try:
            requests.put(url, data=pickled_data, timeout=1)
        except Exception:
            continue
        else:
            break

# kvs/data/<KEY> - GET, PUT, DELETE

@app.route("/keys/<key>", methods=["GET"])
def getKey(key):
    key = DATA.get(key)
    return key.asDict()

@app.route("/keys/<key>", methods=["PUT"])
def putKey(key):
    reqDict = request.get_json(silent=True)
    assert reqDict
    node = KvsNode( reqDict["value"],
        operation=Operation.fromString(reqDict["operation"]),
        msSinceEpoch=int(reqDict["timestamp"]),
        dependencies=[*map(Operation.fromString, reqDict["dependencies"])]
    )
    DATA.put(key, node)
    return ":)"

@app.route('/keys/<key>', methods=["DELETE"])
def delete_key(key):
    DATA.delete(key)
    return ":)"



@app.route("/kvs/data/<key>", methods=["GET", "PUT", "DELETE"])
async def keyEndpoint(key: str):
    if not initialized:
        return {"error": "uninitialized"}, 418
    if request.get_json(silent=True) == None:
        return {"error": "bad request"}, 400

    #get url for every node in correct shard
    shardId, keyHashesTo = hashRing.assign(key)
    addresses = associated_nodes[shardId]
    proxyData = request.json
    proxyData["timestamp"] = time() * 1000
    print(addresses, flush=True)
    res = await broadcastOne(request.method, addresses, f"/proxy/data/{key}", proxyData, 20)
    if res is None:
        return {"error": "upstream down", "upstream": {"shard_id": shardId, "nodes": [addresses]}}, 503
    return res

@app.route("/proxy/data/<key>", methods=["GET", "PUT", "DELETE"])
async def dataRoute(key):
    if not initialized:
        return {"error": "uninitialized"}, 418
    if request.get_json(silent=True) == None:
        return {"error": "bad request"}, 400

    match request.method:
        case "GET":
            res = await getData(key, request.json, nodes=nodes, data=DATA)
            return res
        case "PUT":
            res = putData(key, request.json, data=DATA, nodes=nodes, executor=BGE, opgen=OPGEN)
            return res
        case "DELETE":
            return deleteData(key, request.json, data=DATA, nodes=nodes, executor=BGE, opgen=OPGEN)
        case _default:
            abort(405)


# kvs/data - GETs
@app.route("/kvs/data", methods=["GET"])
async def get_keys():
    if not initialized:
        return {"error": "uninitialized"}, 418

    new_keys = []
    metadata = set(request.json['causal-metadata'])
    count = 0
    for key in DATA.get_all_keys():
        if hashRing.assign(key)[0] != current_shard_id:
            continue
        res = await getData(key, request.json, nodes=nodes, data=DATA)
        if res[1] == 500:
            return res
        if res[1] == 200:
            count += 1
            new_keys.append(key)
            metadata.update(res[0].get('causal-metadata'))

    return {
        'shard_id': current_shard_id,
        "count" : count,
        "keys" : new_keys,
        "causal-metadata" : list(metadata)
    }, 200

async def gossip():
    while True:
        while reshuffle:
            await asyncio.sleep(0.5)

        await asyncio.sleep(1)
        # don't send anything if there is no available nodes or no data
        if len(nodes) == 0:
            continue
        if len(DATA) == 0:
            continue

        # pick a random node
        num = randrange(len(nodes))

        url = 'http://{}/gossip'.format(nodes[num])
        pickled_tree = pickle.dumps(DATA)
        try:
            requests.put(url, data=pickled_tree, timeout=1)
        except Exception:
            continue

@app.route('/gossip', methods=['PUT'])
def update_tree():
    global DATA

    # receive tree and compare it to the
    new_data = request.data

    if new_data == None:
        return {"error": "empty data"}, 400

    new_data: Kvs = pickle.loads(new_data)
    if len(DATA) == 0:
        DATA = new_data
        return "OK", 200
    for k in new_data.get_all_keys():
        if k in DATA.get_all_keys():
            n1 = DATA.get(k)
            n2 = new_data.get(k)
            node = getLargerNode(n1, n2)
            DATA.put(k, node)
        else:
            DATA.put(k, new_data.get(k))
    return "OK", 200


if __name__ == "__main__":
    BGE.run(gossip())
    app.run(host='0.0.0.0', port=PORT, debug=True)
