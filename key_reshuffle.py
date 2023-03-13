import requests
import json
import math

from consistent_hashing import HashRing

############################
# FIXME: currently assume that number of nodes hasn't changed
# find the amount of shards need to be remove
# get list of nodes from removed shard
# add them to the remaining shards
# send every nodes the new view
############################


def remove_shards(num_old_shards: int , numshards: int, associated_nodes: dict, hashRing: HashRing, nodelist: list, NAME: str):
    num_shard_need_to_rm = num_old_shards - numshards

    min_shard_id = [k for k in sorted(associated_nodes, key=lambda k:len(associated_nodes[k]))][:num_shard_need_to_rm]
            
    for id in min_shard_id:
        nodes_need_to_move += associated_nodes[id]
        associated_nodes.pop(id, None)
        hashRing.remove_shard(id)

    shard_id = associated_nodes.keys()
    y = 0
    num_node_in_shard = math.floor(len(nodelist) / numshards)
    for n in nodes_need_to_move:
        while (len(associated_nodes[shard_id[y]]) > num_node_in_shard):
            y += 1
            if y == numshards:
                y = 0
            
        associated_nodes[shard_id[y]].append(n)
        if n == NAME:
            current_shard_id = shard_id[y]
            y += 1
            if y == numshards:
                y = 0

    for k in associated_nodes.keys():
        for n in associated_nodes[k]:
            if n == NAME: 
                continue
            url = f'http://{n}/update_view'
            requests.put(url, json=json.dumps(associated_nodes), timeout=1)


def add_shards(num_old_shards: int, numshards: int, associated_nodes: dict, hashRing: HashRing, nodelist: list, NAME:str):
    num_shard_need_to_add = numshards - num_old_shards

    for i in range(num_shard_need_to_add):
        new_shard_id = 'shard' + str(num_old_shards + i)
        associated_nodes[new_shard_id] = associated_nodes.get(new_shard_id, [])
        hashRing.add_shard(new_shard_id)

    num_node_in_shard = math.floor(len(nodelist) / numshards)  # the min number of nodes per shard

    for i in range(num_shard_need_to_add):  # for each shard that we need to add
        for j in range(num_node_in_shard):  # for the min number of nodes per shard
            max_shard_id = [k for k in sorted(associated_nodes, key=lambda k: len(associated_nodes[k]), reverse=True)][0]  # get the shard with the most nodes
            min_shard_id = [k for k in sorted(associated_nodes, key=lambda k: len(associated_nodes[k]))][0]  # get the shard with the least nodes
            popped = associated_nodes[max_shard_id].pop()
            associated_nodes[min_shard_id].append(popped)

    for k in associated_nodes.keys():  # I think this asks a node in each shard to reshuffle?
        for n in associated_nodes[k]:
            if n == NAME:  # not 100% sure what this means
                continue
            url = f'http://{n}/update_view'
            requests.put(url, json=json.dumps(associated_nodes), timeout=1)

