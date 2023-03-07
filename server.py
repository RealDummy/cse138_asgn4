
from flask import Flask, request, abort
import os
import sys
import requests
import pickle
from time import sleep
from random import randrange
import asyncio
import json
import re

from kvs import Kvs, KvsNode, getLargerNode
from background import Executor
from operations import OperationGenerator, Operation
from causal import getData, putData, deleteData
from consistent_hashing import HashRing


# need startup logic when creating a replica (broadcast?)
NAME = os.environ.get('ADDRESS')  # get IP and port
if not NAME:  # if no ADDRESS exit with return value '1'
    sys.exit('1')
PORT = 8080
DATA = Kvs()
BGE = Executor()
OPGEN = OperationGenerator(NAME)

nodes = []
operations = []
initialized = False

hashRing = HashRing(2543, 3)

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
    numnodes = len(nodelist) #Number of nodes
    """
    if numshards > numnodes:
        return {"bruh": "too many nodes"}, 400
    """
    
    shard_id = []
    global associated_nodes
    associated_nodes = {}

    for x in range(numshards): #Assign shard ID's (trivial)
        shard_id.append(x)
    
    for shard in shard_id:
        associated_nodes[shard] = []
    
    y = 0
    #Assign shards to nodes
    for x in range(numnodes):
        associated_nodes[shard_id[y]].append(nodelist[x])
        if y < numshards - 1:
            y += 1
            if y == numshards:
                y = 0
    
    return "OK", 200
    
    
@app.route('/kvs/admin/view', methods=['GET'])
def getview():
    l = []
    for shard in associated_nodes:
        l.append({' shard_id': shard, 'nodes': associated_nodes[shard]})
    return ({'view': l}), 200

@app.route('/kvs/admin/view', methods=['DELETE'])
def delete_node():
    global initialized, DATA
    if not initialized:
        return {"error": "uninitialized"}, 418

    nodes.clear()
    DATA = Kvs()
    initialized = False
    return "", 200

def checkjson(myjson):
    try:
        json.loads(myjson)
    except ValueError as e:
        return False
    return True

@app.route('/update_kvs', methods= ['PUT'])
def update_kvs_view():
    global DATA, nodes, initialized
    d = pickle.loads(request.data)
    
    # if d['data'] == None:
    #     DATA = Kvs()
    # else:
    #     DATA = d['data']
    
    nodes = d['view']
    nodes.remove(NAME)
    initialized = True
    return "OK", 200

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
        return {"error":"uninitialized"}, 418

    new_keys = []
    metadata = list(request.json['causal-metadata'])
    for key in DATA.get_all_keys():
        res = await getData(key, request.json, nodes=nodes, data=DATA)
        if res[1] == 500:
            return res
        if res[1] != 404:
            new_keys.append(key)
            metadata = list(set(metadata + list(res[0].get('causal-metadata'))))
            
    return {
        "count" : len(DATA),
        "keys" : new_keys,
        "causal-metadata" : metadata
    }, 200

async def gossip():
    while True:
        await asyncio.sleep(1)
        # don't send anything if there is no available nodes or no data
        if len(nodes) == 0:
            continue
        if DATA == None: 
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
        return {"error":"empty data"}, 400    
    
    new_data = pickle.loads(new_data)
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