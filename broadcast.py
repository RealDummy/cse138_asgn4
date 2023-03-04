from flask import Flask, request, jsonify, Response
import os
import json
import requests 
import pickle
from random import randrange
from threading import Thread
from time import sleep
from merkle import MerkleTree, Payload, MerkleTreeDifferenceFinder

PORT = 8080
socket = os.environ.get('ADDRESS')

# collection of all nodes in the view
nodes = ['10.10.0.2:8080', '10.10.0.3:8080', '10.10.0.4:8080']

# collection of kv
data = {}
merkleTree = MerkleTree()

app = Flask(__name__)

def background_broadcast():
    # do this every second 
    # send the request to 1 random node
    while True:
        sleep(2)
        # don't send anything if there is no available nodes or no data
        if len(nodes) == 0:
            continue
        if merkleTree.len() == 0: 
            continue

        # pick a random node
        num = randrange(len(nodes)) 
        while True:
            if nodes[num] == socket:
                num = randrange(len(nodes))
            else: 
                break

        # send root hash 
        url = 'http://{}/gossip'.format(nodes[num])
        pickled_tree = pickle.dumps(merkleTree)
        response = requests.put(url, data=pickled_tree, timeout=10)
        if response == None:
            # not sure what to do with the downed node
            print('address down: ', nodes[num], flush=True)

@app.route('/gossip', methods=['PUT'])
def update_tree():
    global merkleTree

    # receive tree and compare it to the 
    new_data = request.data
    
    if new_data == None:
        return jsonify(error="empty data"), 400    
    
    new_tree = pickle.loads(new_data)
    
    if merkleTree.len() == 0:
        merkleTree = new_tree
        return jsonify(replaced="tree"), 201

    mf1 = MerkleTreeDifferenceFinder(merkleTree)
    mf2 = MerkleTreeDifferenceFinder(new_tree)
    row = 1
    diff = None
    while True:
        incoming = mf2.dumpNextPyramidRow(row,diff)
        diff = mf1.compareForDifferences(incoming)
        row += 1
        if not diff:
            break
    
    new_kv = mf1.getResult()
    # print('new_kv ', new_kv, flush=True)
    for kv in new_kv:
        temp = Payload(kv['key'], kv['val'])
        merkleTree.insert(temp)

    return jsonify(good='good'), 200

@app.route('/kvs', methods=['PUT'])
def put():
    content_type = request.headers.get('Content-Type')
    if content_type == 'application/json':
        params = request.get_json()
        print(params)
        # check if body has key and val
        if not 'key' in params and not 'val' in params:
            return jsonify(error='bad PUT'), 400
                        
        new_key = params['key'].strip() 
        new_val = params['val'].strip() 
        # check if key or val is too long
        if len(new_key) > 200 or len(new_val) > 200:
            return jsonify(error='key or val too long'), 400
            
        # search for key 
        if new_key in data.keys():
            prev_val = data[new_key]
            data[new_key] = new_val
            
            temp = Payload(new_key,new_key)
            merkleTree.insert(temp)

            return jsonify(replaced=True, prev=prev_val), 200
        else:
            temp = Payload(new_key,new_key)
            merkleTree.insert(temp)
            
            data[new_key] = new_val
            return jsonify(replaced=False), 201
    # when body is not json with key and val
    else: 
        return jsonify(error='bad PUT'), 400 

@app.route('/kvs', methods=['GET'])
def do_GET():
    print(merkleTree, flush=True)
    return jsonify(val="a"), 200
    
if __name__ == '__main__':
    thread = Thread(target=background_broadcast)
    thread.daemon = True
    thread.start()
    
    app.run(host='0.0.0.0', port=PORT)