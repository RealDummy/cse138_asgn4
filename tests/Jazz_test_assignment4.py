###################
# Course: CSE 138
# Quarter: Winter 2023
# Assignment: #3
# Author: Amin Karbas <mkarbasf@ucsc.edu>
###################

# Preferably, these should be run when nodes are partitioned, too.

import sys
import time
import unittest

import requests  # pip install requests
import subprocess


# Setup:


def usage():
    print(
        f'Usage: {sys.argv[0]} local_port1:ip1:port1 local_port2:ip2:port2 [local_port3:ip3:port3...]')
    sys.exit(1)


def check_arg_count():
    if len(sys.argv) < 3:
        usage()

nodes = ["8080:10.10.0.2:8080", "8081:10.10.0.3:8080", "8082:10.10.0.4:8080"]

def parse_args():
    check_arg_count()
    local_ports = []
    view = []
    for arg in nodes:
        try:
            col1_idx = arg.find(':')
            local_ports.append(int(arg[:col1_idx]))
            view.append(arg[col1_idx+1:])
        except:
            usage()
    return local_ports, view


ports, view_addresses = parse_args()
hosts = ['localhost'] * len(ports)
MAX_KEYS = 5000
keys = []
for i in range(1, MAX_KEYS+1):
    keys.append(f'key{i}')
vals = []
for i in range(1, MAX_KEYS+1):
    vals.append(f'val{i}')
causal_metadata_key = 'causal-metadata'


# Requests:


def get(url, body={}):
    return requests.get(url, json=body)


def put(url, body={}):
    return requests.put(url, json=body)


def delete(url, body={}):
    return requests.delete(url, json=body)


# URLs:


def make_base_url(port, host='localhost', protocol='http'):
    return f'{protocol}://{host}:{port}'


def kvs_view_admin_url(port, host='localhost'):
    return f'{make_base_url(port, host)}/kvs/admin/view'


def kvs_data_key_url(key, port, host='localhost'):
    return f'{make_base_url(port, host)}/kvs/data/{key}'


def kvs_data_url(port, host='localhost'):
    return f'{make_base_url(port, host)}/kvs/data'


# Bodies:


def nodes_list(ports, hosts=None):
    if hosts is None:
        hosts = ['localhost'] * len(ports)
    return [f'{h}:{p}' for h, p in zip(hosts, ports)]


def put_view_body(addresses, num_shards=1):
    return {'num_shards': num_shards, 'nodes': addresses}


def causal_metadata_body(cm={}):
    return {causal_metadata_key: cm}


def causal_metadata_from_body(body):
    return body[causal_metadata_key]


def put_val_body(val, cm={}):
    body = causal_metadata_body(cm)
    body['val'] = val
    return body

# Containers:

def get_container_ip(container_id):
    return subprocess.check_output(['docker', 'inspect', '-f', '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}', container_id]).decode('utf-8').strip()

def get_container_id(port):
    return subprocess.check_output(['docker', 'ps', '-q', '-f', f'publish={port}']).decode('utf-8').strip()

def pause_node(port):
    subprocess.run(['docker', 'pause', get_container_id(port)])
    time.sleep(0.25)

def unpause_node(port):
    subprocess.run(['docker', 'unpause', get_container_id(port)])
    time.sleep(0.25)

def partition(node_1_addr, node_2_addr):
    node_1_host = node_1_addr.split(':')[0]
    node_2_host = node_2_addr.split(':')[0]
    command = f'sudo iptables -A DOCKER -s {node_1_host} -d {node_2_host} -j DROP'
    subprocess.run(command.split())
    command = f'sudo iptables -A DOCKER -s {node_2_host} -d {node_1_host} -j DROP'
    subprocess.run(command.split())
    
def heal_partition(node_1_addr, node_2_addr):
    node_1_host = node_1_addr.split(':')[0]
    node_2_host = node_2_addr.split(':')[0]
    command = f'sudo iptables -D DOCKER -s {node_1_host} -d {node_2_host} -j DROP'
    subprocess.run(command.split())
    command = f'sudo iptables -D DOCKER -s {node_2_host} -d {node_1_host} -j DROP'
    subprocess.run(command.split())

class TestAssignment1(unittest.TestCase):
    def setUp(self):
        # Uninitialize all nodes:
        for h, p in zip(hosts, ports):
            unpause_node(p)
            delete(kvs_view_admin_url(p, h))
        heal_partition(view_addresses[0], view_addresses[1])
        heal_partition(view_addresses[0], view_addresses[2])
        heal_partition(view_addresses[1], view_addresses[2])

    def test_uninitialized_get_key(self):
        for h, p in zip(hosts, ports):
            with self.subTest(host=h, port=p):
                res = get(kvs_data_key_url('some_key', p, h))
                self.assertEqual(res.status_code, 418,
                                 msg='Bad status code (not a teapot!)')
                body = res.json()
                self.assertIn('error', body,
                              msg='Key not found in json response')
                self.assertEqual(body['error'], 'uninitialized',
                                 msg='Bad error message')

    def test_uninitialized_get_view(self):
        for h, p in zip(hosts, ports):
            with self.subTest(host=h, port=p):
                res = get(kvs_view_admin_url(p, h))
                self.assertEqual(res.status_code, 200, msg='Bad status code')
                body = res.json()
                self.assertIn('view', body,
                              msg='Key not found in json response')
                self.assertEqual(body['view'], [], msg='Bad view')

    def test_put_get_view(self):
        for h, p in zip(hosts, ports):
            with self.subTest(host=h, port=p, verb='put'):
                res = put(kvs_view_admin_url(p, h),
                          put_view_body(view_addresses))
                self.assertEqual(res.status_code, 200, msg='Bad status code')

        for h, p in zip(hosts, ports):
            with self.subTest(host=h, port=p, verb='get'):
                res = get(kvs_view_admin_url(p, h))
                self.assertEqual(res.status_code, 200, msg='Bad status code')
                body = res.json()
                self.assertIn('view', body,
                              msg='Key not found in json response')
                view = body['view']
                # The view should look like this:
                # [{'shard_id': '1', 'nodes': ["10.10.0.2:8080", "10.10.0.3:8080", "10.10.0.4:8080"]}]

                self.assertEqual(len(view), 1, msg='Bad view')
                self.assertIn('shard_id', view[0],
                              msg='shard_id not in the view')
                self.assertEqual(view[0]['shard_id'], 0,
                                    msg='Bad shard_id')
                self.assertIn('nodes', view[0],
                              msg='nodes not in the view')
                self.assertEqual(len(view[0]['nodes']), 3,
                                    msg='Bad number of nodes')
                self.assertEqual(set(view[0]['nodes']), set(view_addresses),
                                 msg='Bad nodes')

    def test_spec_ex2(self):
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        time.sleep(1)

        res = put(kvs_data_key_url(keys[0], ports[1], hosts[1]),
                  put_val_body(vals[0]))
        self.assertEqual(res.status_code, 201, msg='Bad status code')
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm1 = causal_metadata_from_body(body)

        res = put(kvs_data_key_url(keys[0], ports[0], hosts[0]),
                  put_val_body(vals[1], cm1))
        self.assertIn(res.status_code, {200, 201}, msg='Bad status code')
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm1 = causal_metadata_from_body(body)

        res = put(kvs_data_key_url(keys[1], ports[1], hosts[1]),
                  put_val_body(vals[0], cm1))
        self.assertEqual(res.status_code, 201, msg='Bad status code')
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm1 = causal_metadata_from_body(body)

        res = get(kvs_data_key_url(keys[1], ports[1], hosts[1]),
                  causal_metadata_body())
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm2 = causal_metadata_from_body(body)
        self.assertIn('val', body, msg='Key not found in json response')
        self.assertEqual(body['val'], vals[0], 'Bad value')

        res = get(kvs_data_key_url(keys[0], ports[1], hosts[1]),
                  causal_metadata_body(cm2))
        self.assertIn(res.status_code, {200, 500}, msg='Bad status code')
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm2 = causal_metadata_from_body(body)

        if res.status_code == 200:
            self.assertIn('val', body, msg='Key not found in json response')
            self.assertEqual(body['val'], vals[1], 'Bad value')
            return

        # 500
        self.assertIn('error', body, msg='Key not found in json response')
        self.assertEqual(body['error'], 'timed out while waiting for depended updates',
                         msg='Bad error message')

    def test_tie_breaking(self):
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        res = put(kvs_data_key_url(keys[0], ports[0], hosts[0]),
                  put_val_body(vals[0]))
        self.assertEqual(res.status_code, 201, msg='Bad status code')

        res = put(kvs_data_key_url(keys[0], ports[1], hosts[1]),
                  put_val_body(vals[1]))
        self.assertIn(res.status_code, {200, 201}, msg='Bad status code')

        time.sleep(10)

        res0 = get(kvs_data_key_url(keys[0], ports[0], hosts[0]),
                   causal_metadata_body())
        self.assertEqual(res0.status_code, 200, msg='Bad status code')
        body = res0.json()
        self.assertIn('val', body, msg='Key not found in json response')
        val0 = body['val']
        self.assertIn(val0, {vals[0], vals[1]}, 'Bad value')

        res1 = get(kvs_data_key_url(keys[0], ports[0], hosts[0]),
                   causal_metadata_body())
        self.assertEqual(res0.status_code, 200, msg='Bad status code')
        body = res1.json()
        self.assertIn('val', body, msg='Key not found in json response')
        val1 = body['val']
        self.assertIn(val1, {vals[0], vals[1]}, 'Bad value')

        self.assertEqual(val0, val1, 'Bad tie-breaking')

    def test_key_list(self):
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        res = put(kvs_data_key_url(keys[0], ports[0], hosts[0]),
                  put_val_body(vals[0]))
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm = causal_metadata_from_body(body)
        self.assertEqual(res.status_code, 201, msg='Bad status code')

        res = put(kvs_data_key_url(keys[1], ports[1], hosts[1]),
                  put_val_body(vals[1], cm))
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm = causal_metadata_from_body(body)
        self.assertIn(res.status_code, {200, 201}, msg='Bad status code')

        time.sleep(3)

        res = get(kvs_data_url(ports[0], hosts[0]), causal_metadata_body(cm))
        self.assertEqual(res.status_code, 200, 'Bad status code')
        body = res.json()
        # At this point, the body should look like:
        # {"shard_id": "1", "count": 2, "keys": keys[:2], "causal-metadata": {}}

        self.assertIn('shard_id', body, msg='shard_id not found in json body')
        self.assertEqual(body['shard_id'], 0, 'Bad shard_id')
        self.assertIn('count', body, msg='count not found in json body')
        self.assertEqual(body['count'], 2, 'Bad count')
        self.assertIn('keys', body, msg='keys not found in json body')
        self.assertEqual(body['keys'], keys[:2], 'Bad keys')
    
    def test_basic_put_get(self):
        # Set view to all three nodes
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        # Put data, "key1" = "Value 1", to first node
        res = put(kvs_data_key_url(keys[0], ports[0], hosts[0]), put_val_body(vals[0], {}))
        self.assertEqual(res.status_code, 201, msg='Bad status code')
        cm = causal_metadata_from_body(res.json())

        res = get(kvs_data_key_url(keys[0], ports[0], hosts[0]), causal_metadata_body(cm))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertIn('val', body, msg='Key not found in json response')
        self.assertEqual(body['val'], vals[0], msg='Bad value')
        self.assertIn(causal_metadata_key, body, msg='Key not found in json response')
        cm = causal_metadata_from_body(body)
        
    def test_basic_put_get_partition(self):
        # Set view to all three nodes
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        # Seperate node 1 from the rest of the cluster
        partition(view_addresses[0], view_addresses[1])
        partition(view_addresses[0], view_addresses[2])
        
        # Put data, "key1" = "Value 1", to first node
        res = put(kvs_data_key_url(keys[0], ports[0], hosts[0]), put_val_body(vals[0], {}))
        self.assertEqual(res.status_code, 201, msg='Bad status code')
        cm = causal_metadata_from_body(res.json())
        
        # Heal partitions
        
        # Let it fester a bit
        time.sleep(2)
        
        heal_partition(view_addresses[0], view_addresses[1])
        heal_partition(view_addresses[0], view_addresses[2])

        # Wait for agreement
        time.sleep(1)
        
        # Seperate node 3 from the rest of the cluster
        partition(view_addresses[2], view_addresses[0])
        partition(view_addresses[2], view_addresses[1])
        
        # Get key1 from node 3
        res = get(kvs_data_key_url(keys[0], ports[2], hosts[2]), causal_metadata_body(cm))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertIn('val', body, msg='Key not found in json response')
        self.assertEqual(body['val'], vals[0], msg='Bad value')
        self.assertIn(causal_metadata_key, body, msg='Key not found in json response')
        cm = causal_metadata_from_body(body)
        # self.assertIn('dependency-list', cm, msg='Dependency list not found in json response')
        # self.assertEqual(cm['dependency-list'], ['V1'], msg='Bad causal metadata')

    def test_basic_majority(self):
        # Set view to all three nodes
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        # Pause node 3
        pause_node(ports[2])

        # Put data, "key1" = "Value 1", to first node
        res = put(kvs_data_key_url(keys[0], ports[0], hosts[0]), put_val_body(vals[0], {"dependency-list": []}))
        self.assertEqual(res.status_code, 201, msg='Bad status code')
        cm = causal_metadata_from_body(res.json())

        # Unpause node 3
        unpause_node(ports[2])

        # Wait 1s for agreement
        time.sleep(1)

        # Pause Node2 so Node1 will be forced to check majority
        # with Node3

        # Get data, "key1" = "Value 1", from node1
        res = get(kvs_data_key_url(keys[0], ports[0], hosts[0]), causal_metadata_body(cm))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertIn('val', body, msg='Key not found in json response')
        val = body['val']
        self.assertEqual(val, vals[0], msg='Bad value')
        cm = causal_metadata_from_body(body)
        # self.assertIn('dependency-list', cm, msg='Dependency list not found in json response')
        # self.assertEqual(cm['dependency-list'], ['V1'], msg='Bad causal metadata')

    def test_persistence(self):
        # Set view to all three nodes
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        # Pause node 3
        pause_node(ports[2])

        # Put data, "key1" = "Value 1", to first node
        res = put(kvs_data_key_url(keys[0], ports[0], hosts[0]), put_val_body(vals[0], {"dependency-list": []}))
        self.assertEqual(res.status_code, 201, msg='Bad status code')
        cm = causal_metadata_from_body(res.json())

        # Wait for 7 seconds (longer than the timeout)
        time.sleep(10)

        # Unpause node 3
        unpause_node(ports[2])

        # Wait 1s for agreement
        time.sleep(1)

        # Pause Node2 so Node1 will be forced to check majority
        # with Node3

        # Get data, "key1" = "Value 1", from node1
        res = get(kvs_data_key_url(keys[0], ports[0], hosts[0]), causal_metadata_body(cm))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertIn('val', body, msg='Key not found in json response')
        val = body['val']
        self.assertEqual(val, vals[0], msg='Bad value')
        cm = causal_metadata_from_body(body)
        # self.assertIn('dependency-list', cm, msg='Dependency list not found in json response')
        # self.assertEqual(cm['dependency-list'], ['V1'], msg='Bad causal metadata')


    def test_basic_majority_blasting(self):
        # Set view to all three nodes
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        # Pause node 3
        pause_node(ports[2])

        # Put data, "key1" = "Value 1", to first node
        res = put(kvs_data_key_url(keys[0], ports[0], hosts[0]), put_val_body(vals[0], {"dependency-list": []}))
        self.assertEqual(res.status_code, 201, msg='Bad status code')
        cm = causal_metadata_from_body(res.json())
        # self.assertIn('dependency-list', cm, msg='Dependency list not found in json response')
        # self.assertEqual(cm['dependency-list'], ['V1'], msg='Bad causal metadata')

        # Blast put 10 times, "key1" = "Value 1", to first node
        for i in range(10):
            res = put(kvs_data_key_url(keys[0], ports[0], hosts[0]), put_val_body(vals[0], cm))
            self.assertEqual(res.status_code, 200, msg='Bad status code')
            cm = causal_metadata_from_body(res.json())

        # Unpause node 3
        unpause_node(ports[2])

        # Wait 1s for agreement
        time.sleep(1)

        # Pause Node2 so Node1 will be forced to check majority
        # with Node3
        pause_node(ports[1])

        # Get data, "key1" = "Value 1", from node1
        res = get(kvs_data_key_url(keys[0], ports[0], hosts[0]), causal_metadata_body(cm))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertIn('val', body, msg='Key not found in json response')
        val = body['val']
        self.assertEqual(val, vals[0], msg='Bad value')
        cm_3 = causal_metadata_from_body(body)
        # self.assertIn('dependency-list', cm_3, msg='Dependency list not found in json response')
        # self.assertIn('dependency-list', cm, msg='Dependency list not found in json response')
        # self.assertEqual(cm_3['dependency-list'], cm['dependency-list'], msg='Bad causal metadata')

        # Unpause Node2
        unpause_node(ports[1])
        
    
        
    def test_malformed_requests(self):
        # Set view to all three nodes
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        
        # Send put view without "view" field
        res = put(kvs_view_admin_url(ports[0], hosts[0]), {"heckin": "gotem"})
        self.assertEqual(res.status_code, 400, msg="Bad status code")
        
        # Send put view with empty "view" field
        res = put(kvs_view_admin_url(ports[0], hosts[0]), {"view": ""})
        self.assertEqual(res.status_code, 400, msg="Bad status code")
        
        # Send put view with bad "view" field type
        res = put(kvs_view_admin_url(ports[0], hosts[0]), {"view": "hello good sir"})
        self.assertEqual(res.status_code, 400, msg="Bad status code")
        
        # Send put view with bad "view" field type inside array
        res = put(kvs_view_admin_url(ports[0], hosts[0]), {"view": ["bruh", 5, "deez"]})
        self.assertEqual(res.status_code, 400, msg="Bad status code")
        
        # Send view with weird method
        res = requests.head(kvs_view_admin_url(ports[0], hosts[0]), json=put_view_body(view_addresses))
        self.assertEqual(res.status_code, 400, msg="Bad status code")
        
        ### Data Tests ###
        
        # Send delete with bad path
        
        res = delete(kvs_data_key_url(keys[0], ports[0], hosts[0]) + "/hello")
        self.assertEqual(res.status_code, 400, msg="Bad status code")
        
        # Send put with bad path
        
        res = put(kvs_data_key_url(keys[0], ports[0], hosts[0]) + "/hello", put_val_body(vals[0], {}))
        self.assertEqual(res.status_code, 400, msg="Bad status code")
        
        # Send get with bad path
        
        res = get(kvs_data_key_url(keys[0], ports[0], hosts[0]) + "/hello")
        self.assertEqual(res.status_code, 400, msg="Bad status code")
        
        # Send delete with long ass key
        
        res = delete(kvs_data_key_url("a" * 1000, ports[0], hosts[0]))
        self.assertEqual(res.status_code, 400, msg="Bad status code")
        
        # Send put with long ass key
        
        res = put(kvs_data_key_url("a" * 1000, ports[0], hosts[0]), put_val_body(vals[0], {}))
        self.assertEqual(res.status_code, 400, msg="Bad status code")
        
        # Send get with long ass key
        
        res = get(kvs_data_key_url("a" * 1000, ports[0], hosts[0]))
        self.assertEqual(res.status_code, 400, msg="Bad status code")
        
        # Send get with 200 sized key
        res = get(kvs_data_key_url("a" * 200, ports[0], hosts[0]))
        self.assertEqual(res.status_code, 404, msg="Bad status code")
        
        # Send put with 200 sized key
        
        res = put(kvs_data_key_url("a" * 200, ports[0], hosts[0]), put_val_body(vals[0], {}))
        self.assertEqual(res.status_code, 201, msg="Bad status code")
        
        # Send delete with 200 sized key
        
        res = delete(kvs_data_key_url("a" * 200, ports[0], hosts[0]))
        self.assertEqual(res.status_code, 200, msg="Bad status code")
        
        # Send get with 200 sized key
        res = get(kvs_data_key_url("a" * 201, ports[0], hosts[0]))
        self.assertEqual(res.status_code, 400, msg="Bad status code")
        
        # Send put with 200 sized key
        
        res = put(kvs_data_key_url("a" * 201, ports[0], hosts[0]), put_val_body(vals[0], {}))
        self.assertEqual(res.status_code, 400, msg="Bad status code")
        
        # Send delete with 200 sized key
        
        res = delete(kvs_data_key_url("a" * 201, ports[0], hosts[0]))
        self.assertEqual(res.status_code, 400, msg="Bad status code")
        
        # Send put with weird body
        
        res = put(kvs_data_key_url(keys[0], ports[0], hosts[0]), {"heckin": "gotem"})
        self.assertEqual(res.status_code, 400, msg="Bad status code")
        
        # Send put data without body
        
        res = put(kvs_data_key_url(keys[0], ports[0], hosts[0]))
        self.assertEqual(res.status_code, 400, msg="Bad status code")
        

    def test_if_green_submit(self):
        # Put all nodes in view
        res = put(kvs_view_admin_url(ports[0], hosts[0]), put_view_body(view_addresses))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        # Partition node 1 and node 2
        partition(view_addresses[0], view_addresses[1])

        ### Client 2, GREEN request ###
        # Put data, "key2" = val2, to second node
        res = put(kvs_data_key_url(keys[1], ports[1], hosts[1]), put_val_body(vals[1], {}))
        self.assertEqual(res.status_code, 201, msg='Bad status code')
        cm_2 = causal_metadata_from_body(res.json())
        # self.assertIn('dependency-list', cm_2, msg='Dependency list not found in json response')
        # self.assertEqual(cm_2['dependency-list'], ['V1'], msg='Bad causal metadata')

        ### Client 1, BLUE request ###
        # Put data key1=val1 to first node
        res = put(kvs_data_key_url(keys[0], ports[0], hosts[0]), put_val_body(vals[0], {}))
        self.assertEqual(res.status_code, 201, msg='Bad status code')
        cm_1 = causal_metadata_from_body(res.json())
        # self.assertIn('dependency-list', cm_1, msg='Dependency list not found in json response')
        # self.assertEqual(cm_1['dependency-list'], ['V1'], msg='Bad causal metadata')

        ### Client 2, BLACK request ###
        # Put data, "key2" = val3, to second node
        res = put(kvs_data_key_url(keys[1], ports[1], hosts[1]), put_val_body(vals[2], {}))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        cm_2 = causal_metadata_from_body(res.json())
        # self.assertIn('dependency-list', cm_2, msg='Dependency list not found in json response')
        # self.assertEqual(cm_2['dependency-list'], ['V1', 'V2'], msg='Bad causal metadata')

        ### Client 2, YELLOW request ###
        # Delete data, "key2", to second node
        res = delete(kvs_data_key_url(keys[1], ports[1], hosts[1]), causal_metadata_body(cm_2))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        cm_2 = causal_metadata_from_body(res.json())
        # self.assertIn('dependency-list', cm_2, msg='Dependency list not found in json response')
        # self.assertEqual(cm_2['dependency-list'], ['V1', 'V2', 'V3'], msg='Bad causal metadata')

        ### Client 1, RED request ###
        # Put data, key2 = val4 to first node
        res = put(kvs_data_key_url(keys[1], ports[0], hosts[0]), put_val_body(vals[3], {}))
        self.assertEqual(res.status_code, 201, msg='Bad status code')
        cm_1 = causal_metadata_from_body(res.json())
        # self.assertIn('dependency-list', cm_1, msg='Dependency list not found in json response')
        # self.assertEqual(cm_1['dependency-list'], ['V1', 'V2'], msg='Bad causal metadata')

        ### Client 1, GET1.1 request ###
        # Get key1 from first node, should be val1
        res = get(kvs_data_key_url(keys[0], ports[0], hosts[0]), causal_metadata_body(cm_1))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertIn('val', body, msg='Key not found in json response')
        self.assertEqual(body['val'], vals[0], msg='Bad value')
        cm_1 = causal_metadata_from_body(body)
        # self.assertIn('dependency-list', cm_1, msg='Dependency list not found in json response')
        # self.assertEqual(cm_1['dependency-list'], ['V1', 'V2'], msg='Bad causal metadata')

        ### Client 1, GET1.2 request ###
        # Get key2 from first node, should be val4
        res = get(kvs_data_key_url(keys[1], ports[0], hosts[0]), causal_metadata_body(cm_1))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertIn('val', body, msg='Key not found in json response')
        self.assertEqual(body['val'], vals[3], msg='Bad value')
        cm_1 = causal_metadata_from_body(body)
        # self.assertIn('dependency-list', cm_1, msg='Dependency list not found in json response')
        # self.assertEqual(cm_1['dependency-list'], ['V1', 'V2'], msg='Bad causal metadata')

        ### Client 2, GET2.1 request ###
        # Get key2 from second node, should be status 404
        res = get(kvs_data_key_url(keys[1], ports[1], hosts[1]), causal_metadata_body(cm_2))
        self.assertEqual(res.status_code, 404, msg='Bad status code')
        cm_2 = causal_metadata_from_body(res.json())
        # self.assertIn('dependency-list', cm_2, msg='Dependency list not found in json response')
        # self.assertEqual(cm_2['dependency-list'], ['V1', 'V2', 'V3'], msg='Bad causal metadata')

        ### Client 2, GET 2.2 request ###
        # Get key1 from second node, should be status 404
        res = get(kvs_data_key_url(keys[0], ports[1], hosts[1]), causal_metadata_body(cm_2))
        self.assertEqual(res.status_code, 404, msg='Bad status code')
        cm_2 = causal_metadata_from_body(res.json())
        # self.assertIn('dependency-list', cm_2, msg='Dependency list not found in json response')
        # self.assertEqual(cm_2['dependency-list'], ['V1', 'V2', 'V3'], msg='Bad causal metadata')

        ### Client 3, GET 3.1 request ###
        # Get key1 from third node, should be val1
        res = get(kvs_data_key_url(keys[0], ports[2], hosts[2]), causal_metadata_body({}))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertIn('val', body, msg='Key not found in json response')
        Node3_x = body['val']
        self.assertEqual(Node3_x, vals[0], msg='Bad value')
        cm_3 = causal_metadata_from_body(body)
        # self.assertIn('dependency-list', cm_3, msg='Dependency list not found in json response')
        # self.assertEqual(cm_3['dependency-list'], ['V1', 'V2', 'V3', 'V4', 'V5'], msg='Bad causal metadata') # JUSTIN, should it be V1-5?

        ### Client 3, GET 3.2 request ###
        # Get key2 from third node, should be val4
        res = get(kvs_data_key_url(keys[1], ports[2], hosts[2]), causal_metadata_body(cm_3))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertIn('val', body, msg='Key not found in json response')
        Node3_y = body['val']
        self.assertEqual(Node3_y, vals[3], msg='Bad value')
        cm_3 = causal_metadata_from_body(body)
        # self.assertIn('dependency-list', cm_3, msg='Dependency list not found in json response')
        # self.assertEqual(cm_3['dependency-list'], ['V1', 'V2', 'V3', 'V4', 'V5'], msg='Bad causal metadata')

        # Heal the partition between the first and second node
        heal_partition(view_addresses[0], view_addresses[1])

        # Wait for the partition to heal
        time.sleep(5)

        ### Client 1, GET 4.1 request ###
        # Get key1 from first node, should be val1
        res = get(kvs_data_key_url(keys[0], ports[0], hosts[0]), causal_metadata_body(cm_1))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertIn('val', body, msg='Key not found in json response')
        Node1_x = body['val']
        self.assertEqual(Node1_x, vals[0], msg='Bad value')
        cm_1 = causal_metadata_from_body(body)
        # self.assertIn('dependency-list', cm_1, msg='Dependency list not found in json response')
        # self.assertEqual(cm_1['dependency-list'], ['V1', 'V2', 'V3', 'V4', 'V5'], msg='Bad causal metadata')

        ### Client 1, GET 4.2 request ###
        # Get key2 from first node, should be val4
        res = get(kvs_data_key_url(keys[1], ports[0], hosts[0]), causal_metadata_body(cm_1))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertIn('val', body, msg='Key not found in json response')
        Node1_y = body['val']
        self.assertEqual(Node1_y, vals[3], msg='Bad value')
        cm_1 = causal_metadata_from_body(body)
        # self.assertIn('dependency-list', cm_1, msg='Dependency list not found in json response')
        # self.assertEqual(cm_1['dependency-list'], ['V1', 'V2', 'V3', 'V4', 'V5'], msg='Bad causal metadata')

        ### Client 2, GET 5.1 request ###
        # Get key1 from second node, should be val1
        res = get(kvs_data_key_url(keys[0], ports[1], hosts[1]), causal_metadata_body(cm_2))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertIn('val', body, msg='Key not found in json response')
        Node2_x = body['val']
        self.assertEqual(Node2_x, vals[0], msg='Bad value')
        cm_2 = causal_metadata_from_body(body)
        # self.assertIn('dependency-list', cm_2, msg='Dependency list not found in json response')
        # self.assertEqual(cm_2['dependency-list'], ['V1', 'V2', 'V3', 'V4', 'V5'], msg='Bad causal metadata')

        ### Client 2, GET 5.2 request ###
        # Get key2 from second node, should be val4
        res = get(kvs_data_key_url(keys[1], ports[1], hosts[1]), causal_metadata_body(cm_2))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertIn('val', body, msg='Key not found in json response')
        Node2_y = body['val']
        self.assertEqual(Node2_y, vals[3], msg='Bad value')
        cm_2 = causal_metadata_from_body(body)
        # self.assertIn('dependency-list', cm_2, msg='Dependency list not found in json response')
        # self.assertEqual(cm_2['dependency-list'], ['V1', 'V2', 'V3', 'V4', 'V5'], msg='Bad causal metadata')

        ### EVERYTHING AGREES ###
        # Check to make sure all nodes have the same data and dependency lists
        # self.assertEqual(cm_1['dependency-list'], cm_2['dependency-list'], msg='Dependency lists do not match')
        # self.assertEqual(cm_1['dependency-list'], cm_3['dependency-list'], msg='Dependency lists do not match')
        # self.assertEqual(cm_2['dependency-list'], cm_3['dependency-list'], msg='Dependency lists do not match')
        self.assertEqual(Node1_x, Node2_x, msg='Values do not match')
        self.assertEqual(Node1_x, Node3_x, msg='Values do not match')
        self.assertEqual(Node2_x, Node3_x, msg='Values do not match')
        self.assertEqual(Node1_y, Node2_y, msg='Values do not match')
        self.assertEqual(Node1_y, Node3_y, msg='Values do not match')
        self.assertEqual(Node2_y, Node3_y, msg='Values do not match')
        
    def test_timeout(self):
        # Put all nodes in view
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        
        # Partition off the first node from the rest
        partition(view_addresses[0], view_addresses[1])
        partition(view_addresses[0], view_addresses[2])
        
        # Send put to node 3
        res = put(kvs_data_key_url(keys[0], ports[2], hosts[2]),
                  put_val_body(vals[0]))
        self.assertEqual(res.status_code, 201, msg='Bad status code')
        cm = causal_metadata_from_body(res.json())
        
        # Send get request to node 1 and hope for timeout
        
        start = time.time()
        res = get(kvs_data_key_url(keys[0], ports[0], hosts[0]), causal_metadata_body(cm))
        end = time.time()
        self.assertEqual(res.status_code, 500, msg='Bad status code')
        error_msg = res.json()["error"]
        self.assertEqual(error_msg, "timed out while waiting for depended updates", msg="Bad error message")
        self.assertGreaterEqual(end - start, 20, msg="Timeout too short")
        
    def test_example_causal(self):
        # Put nodes 1 and 2 in the view
        res = put(kvs_view_admin_url(ports[0], hosts[0]), put_view_body(view_addresses[0:2]))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        
        # Parition nodes 1 and 2
        partition(view_addresses[0], view_addresses[1])
        
        ### CLIENT 1 ###
        
        # Event A1: Put(y, 10) to R2
        
        res = put(kvs_data_key_url("y", ports[1], hosts[1]), put_val_body(10))
        self.assertEqual(res.status_code, 201, msg='Bad status code')
        cm1 = causal_metadata_from_body(res.json())
        self.assertIn('y', cm1, msg='Key not found in causal metadata')
        y_timestamp = float(cm1['y'])
        
        # Event B1: Put(y, 20) to R1
        
        res = put(kvs_data_key_url("y", ports[0], hosts[0]), put_val_body(20, cm1))
        self.assertEqual(res.status_code, 201, msg='Bad status code')
        cm1 = causal_metadata_from_body(res.json())
        self.assertIn('y', cm1, msg='Key not found in causal metadata')
        new_timestamp = float(cm1['y'])
        self.assertGreater(new_timestamp, y_timestamp, msg='Timestamp not updated')
        
        # Event C1: Get(y) to R1
        
        res = get(kvs_data_key_url("y", ports[0], hosts[0]), causal_metadata_body(cm1))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertIn('val', body, msg='Key not found in json response')
        self.assertEqual(body['val'], 20, msg='Bad value')
        cm1 = causal_metadata_from_body(res.json())
        self.assertIn('y', cm1, msg='Key not found in causal metadata')
        
        # Event D1: Put(x, 5) to R2
        
        res = put(kvs_data_key_url("x", ports[1], hosts[1]), put_val_body(5, cm1))
        self.assertEqual(res.status_code, 201, msg='Bad status code')
        cm1 = causal_metadata_from_body(res.json())
        self.assertIn('x', cm1, msg='Key not found in causal metadata')
        self.assertIn('y', cm1, msg='Key not found in causal metadata')
    
        ### CLIENT 2 ###
        
        # Event E1: Get(x) to R2
        
        res = get(kvs_data_key_url("x", ports[1], hosts[1]))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertEqual(body['val'], 5, msg='Bad value')
        cm2 = causal_metadata_from_body(res.json())
        self.assertIn('x', cm2, msg='Key not found in causal metadata')
        self.assertIn('y', cm2, msg='Key not found in causal metadata')
        
        # Event F1: Get(y) to R2, should timeout
        
        res = get(kvs_data_key_url("y", ports[1], hosts[1]), causal_metadata_body(cm2))
        self.assertEqual(res.status_code, 500, msg='Bad status code')

    def test_lots_of_keys(self):
        # Put all nodes in view
        res = put(kvs_view_admin_url(ports[0], hosts[0]), put_view_body(view_addresses))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        
        # Partition Node 1 from the rest
        partition(view_addresses[0], view_addresses[1])
        partition(view_addresses[0], view_addresses[2])
        
        # Send 10 thousand keys to node 1
        cm = {}
        num_keys = 10000
        for i in range(num_keys + 1):
            res = put(kvs_data_key_url(str(i // 200), ports[0], hosts[0]), put_val_body(i, cm))
            cm = causal_metadata_from_body(res.json())
        
        # Heal partitions
        
        heal_partition(view_addresses[0], view_addresses[1])
        heal_partition(view_addresses[0], view_addresses[2])
        
        time.sleep(5) # Time to agree
        
        # Send get to node 1 as a sanity check
        res = get(kvs_data_key_url(str(num_keys // 200), ports[0], hosts[0]), causal_metadata_body(cm))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        self.assertEqual(res.json()['val'], num_keys, msg='Bad value')
        
        # Send get to node 2
        
        res = get(kvs_data_key_url(str(num_keys // 200), ports[1], hosts[1]), causal_metadata_body(cm))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        self.assertEqual(res.json()['val'], num_keys, msg='Bad value')

    def test_shard_put2_get2_simple(self):
        # Create the view with 2 nodes and 2 shards
        res = put(kvs_view_admin_url(ports[0], hosts[0]), put_view_body(view_addresses[:2], 2))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        # Put key1=val1 to node1
        res = put(kvs_data_key_url(keys[0], ports[0], hosts[0]), put_val_body(vals[0]))
        self.assertEqual(res.status_code, 201, msg='Bad status code')

        # Put key2=val2 to node1
        res = put(kvs_data_key_url(keys[1], ports[0], hosts[0]), put_val_body(vals[1]))
        self.assertEqual(res.status_code, 201, msg='Bad status code')

        # Get key1 from node2
        res = get(kvs_data_key_url(keys[0], ports[1], hosts[1]))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body_1 = res.json()
        self.assertIn('val', body_1, msg='Key not found in json response')
        self.assertEqual(body_1['val'], vals[0], msg='Bad value')

        # Get key2 from node2
        res = get(kvs_data_key_url(keys[1], ports[0], hosts[0]))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body_2 = res.json()
        self.assertIn('val', body_2, msg='Key not found in json response')
        self.assertEqual(body_2['val'], vals[1], msg='Bad value')

    def test_shard_put2_get2_simple_causal(self):
        # Create the view with all nodes and 2 shards
        res = put(kvs_view_admin_url(ports[0], hosts[0]), put_view_body(view_addresses, 2))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        # Put key1 to node1
        res = put(kvs_data_key_url(keys[0], ports[0], hosts[0]), put_val_body(vals[0]))
        self.assertEqual(res.status_code, 201, msg='Bad status code')
        cm1 = causal_metadata_from_body(res.json())
        self.assertIn(keys[0], cm1, msg='Key not found in causal metadata')

        # Put key2 to node2
        res = put(kvs_data_key_url(keys[1], ports[1], hosts[1]), put_val_body(vals[1], cm1))
        self.assertEqual(res.status_code, 201, msg='Bad status code')
        cm1 = causal_metadata_from_body(res.json())
        self.assertIn(keys[0], cm1, msg='Key not found in causal metadata') # Touched node1, should have key1
        self.assertIn(keys[1], cm1, msg='Key not found in causal metadata')

        # Get key1 from node2
        res = get(kvs_data_key_url(keys[0], ports[1], hosts[1]), causal_metadata_body(cm1))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertIn('val', body, msg='Key not found in json response')
        self.assertEqual(body['val'], vals[0], msg='Bad value')
        cm1 = causal_metadata_from_body(res.json())
        self.assertIn(keys[0], cm1, msg='Key not found in causal metadata')

        # Get key2 from node1
        res = get(kvs_data_key_url(keys[1], ports[0], hosts[0]), causal_metadata_body(cm1))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertIn('val', body, msg='Key not found in json response')
        self.assertEqual(body['val'], vals[1], msg='Bad value')
        cm1 = causal_metadata_from_body(res.json())
        self.assertIn(keys[1], cm1, msg='Key not found in causal metadata')

    def test_shard_put2_get2_harder(self):
        # Create the view with 2 nodes and 2 shards
        res = put(kvs_view_admin_url(ports[0], hosts[0]), put_view_body(view_addresses[:2], 2))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        # Put key1 to node1
        res = put(kvs_data_key_url(keys[0], ports[0], hosts[0]), put_val_body(vals[0]))
        self.assertEqual(res.status_code, 201, msg='Bad status code')
        
        # Put key1 to node2
        res = put(kvs_data_key_url(keys[0], ports[1], hosts[1]), put_val_body(vals[1]))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        # Now that the nodes have the save key, because of sharding only one of the nodes should have the key
        # Using GET(kvs/data) we can see that only one of the nodes has the key
        res = get(kvs_data_url(ports[0], hosts[0]))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body_1 = res.json()
        self.assertIn('keys', body_1, msg='Keys not found in json response')
        body_1_keys = body_1['keys']

        res = get(kvs_data_url(ports[1], hosts[1]))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body_2 = res.json()
        self.assertIn('keys', body_2, msg='Keys not found in json response')
        body_2_keys = body_2['keys']

        # We dont know which body has the key, but we know that only one should have it
        self.assertNotEqual(body_1_keys, body_2_keys, msg='Both nodes have the key')

        # Get key1 from node2
        res = get(kvs_data_key_url(keys[0], ports[1], hosts[1]))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body_1 = res.json()
        self.assertIn('val', body_1, msg='Key not found in json response')
        self.assertEqual(body_1['val'], vals[1], msg='Bad value')


    def TODO_test_shard_causal_breaks(self): # THIS TEST WORKS DEPENDING ON HOW YOU ASSIGN SHARDS. N1/N2 -> s1 and N3 -> s2
        # Create the view with all nodes and 2 shards
        res = put(kvs_view_admin_url(ports[0], hosts[0]), put_view_body(view_addresses, 2))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        # Create a partition between node1 and node2
        partition(view_addresses[0], view_addresses[1])

        # As Client1, PUT key1=val1 to node1
        res = put(kvs_data_key_url(keys[0], ports[0], hosts[0]), put_val_body(vals[0]))
        self.assertEqual(res.status_code, 201, msg='Bad status code')
        cm1 = causal_metadata_from_body(res.json())
        self.assertIn(keys[0], cm1, msg='Key not found in causal metadata')

        # As Client2, PUT key2=val1 to node2
        res = put(kvs_data_key_url(keys[1], ports[1], hosts[1]), put_val_body(vals[0]))
        self.assertEqual(res.status_code, 201, msg='Bad status code')
        cm2 = causal_metadata_from_body(res.json())
        self.assertIn(keys[1], cm2, msg='Key not found in causal metadata')

        # As Client3, GET key1 from node3
        res = get(kvs_data_key_url(keys[0], ports[2], hosts[2]), causal_metadata_body())
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        cm3 = causal_metadata_from_body(res.json())
        body = res.json()
        self.assertIn('val', body, msg='Key not found in json response')
        self.assertEqual(body['val'], vals[0], msg='Bad value')
        self.assertIn(keys[0], cm3, msg='Key0 not found in causal metadata')
        self.assertIn(keys[1], cm3, msg='Key1 not found in causal metadata')

        # Create another partition between node2 and node3
        partition(view_addresses[1], view_addresses[2])

        # As Client3, GET key2 from node3
        res = get(kvs_data_key_url(keys[1], ports[2], hosts[2]), causal_metadata_body(cm3))
        # We are expecting an error of 500
        self.assertEqual(res.status_code, 500, msg='Bad status code')

        # Heal the partition between node2 and node3
        heal_partition(view_addresses[1], view_addresses[2])

        # Now we can get key2 from node3 as client3
        res = get(kvs_data_key_url(keys[1], ports[2], hosts[2]), causal_metadata_body(cm3))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertIn('val', body, msg='Key not found in json response')
        self.assertEqual(body['val'], vals[0], msg='Bad value')

    def test_shard_basic_put_get(self):
        # 2 shards 3 nodes
        res = put(kvs_view_admin_url(ports[0], hosts[0]), put_view_body(view_addresses[:3], 2))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        
        # Put key1 to node1
        res = put(kvs_data_key_url(keys[0], ports[0], hosts[0]), put_val_body(vals[0]))
        self.assertEqual(res.status_code, 201, msg='Bad status code')
        
        # Get key1 from node2
        res = get(kvs_data_key_url(keys[0], ports[1], hosts[1]))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertIn('val', body, msg='Key not found in json response')
        self.assertEqual(body['val'], vals[0], msg='Bad value')

    def TODO_test_shard_node_balance(self):
        # This test is intended to test if there are balanced nodes per shard
        # Create a view for 3 nodes and 3 shards
        res = put(kvs_view_admin_url(ports[0], hosts[0]), put_view_body(view_addresses[:3], 3))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        # Get kvs/admin/view
        res = get(kvs_view_admin_url(ports[0], hosts[0]))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertIn('view', body, msg='View not found in json response')
        view = body['view']
        self.assertEqual(len(view), 3, msg='Bad view length')
        for shard in view:
            self.assertIn('shard_id', shard, msg='Shard_id not found in json response')
            self.assertIn('nodes', shard, msg='Nodes not found in json response')
            self.assertEqual(len(shard['nodes']), 1, msg='Bad nodes length')

        # Create a view for 4 nodes and 2 shards
        res = put(kvs_view_admin_url(ports[0], hosts[0]), put_view_body(view_addresses[:4], 2))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        # Get kvs/admin/view
        res = get(kvs_view_admin_url(ports[0], hosts[0]))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertIn('view', body, msg='View not found in json response')
        view = body['view']
        self.assertEqual(len(view), 2, msg='Bad view length')
        for shard in view:
            self.assertIn('shard_id', shard, msg='Shard_id not found in json response')
            self.assertIn('nodes', shard, msg='Nodes not found in json response')
            self.assertEqual(len(shard['nodes']), 2, msg='Bad nodes length')

        # Create a view for 5 nodes and 2 shards
        res = put(kvs_view_admin_url(ports[0], hosts[0]), put_view_body(view_addresses[:5], 2))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        # Get kvs/admin/view
        res = get(kvs_view_admin_url(ports[0], hosts[0]))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertIn('view', body, msg='View not found in json response')
        view = body['view']
        self.assertEqual(len(view), 2, msg='Bad view length')
        for shard in view:
            self.assertIn('shard_id', shard, msg='Shard_id not found in json response')
            self.assertIn('nodes', shard, msg='Nodes not found in json response')
            self.assertTrue(len(shard['nodes']) == 2 or len(shard['nodes']) == 3, msg='Bad nodes length')

    def test_shard_down(self):
        # Create a view for 3 nodes and 3 shards
        res = put(kvs_view_admin_url(ports[0], hosts[0]), put_view_body(view_addresses[:3], 3))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        # Insert MAX_KEYS to node1
        for i in range(MAX_KEYS):
            res = put(kvs_data_key_url(keys[i], ports[0], hosts[0]), put_val_body(vals[i]))
            self.assertEqual(res.status_code, 201, msg='Bad status code')
        
        # Wait 5 seconds
        time.sleep(5)

        pre_down = {}
        # Stores the 3 shard ids no matter how they are named.
        # Stores as: {0: 'shard1_name', 1: 'shard2_name', 2: 'shard3_name'}
        # This allows us to index the shards without knowing the naming convention
        pre_shard_ids = {}

        # Get kvs/data from every node
        for i in range(3):
            res = get(kvs_data_url(ports[i], hosts[i]))
            self.assertEqual(res.status_code, 200, msg='Bad status code')
            body = res.json()
            self.assertIn('keys', body, msg=f'keys not found in node{i}')
            self.assertIn('shard_id', body, msg=f'shard_id not found in node{i}')
            pre_shard_ids[i] = body['shard_id']
            pre_down[pre_shard_ids[i]] = body['keys']
        
        # I am expecting each shard to have either 5 or 6 keys
        # I want the length of the shards to be with 10% of the average
        average_shard_length = MAX_KEYS // 3
        # Check if len(pre_down[shard_ids[i]]) is within 10% of average_shard_length
        for i in range(3):
            self.assertTrue(abs(len(pre_down[pre_shard_ids[i]]) - average_shard_length) <= average_shard_length * 0.1, msg=f'Bad shard length in shard {pre_shard_ids[i]}')

        # Create a view for 3 nodes and 2 shards
        res = put(kvs_view_admin_url(ports[0], hosts[0]), put_view_body(view_addresses[:3], 2))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        # Wait 5 seconds
        time.sleep(5)

        post_down = {}
        post_shard_ids = {}
        
        # Get kvs/data from every node
        for i in range(3):
            res = get(kvs_data_url(ports[i], hosts[i]))
            self.assertEqual(res.status_code, 200, msg='Bad status code')
            body = res.json()
            self.assertIn('keys', body, msg=f'keys not found in node{i}')
            self.assertIn('shard_id', body, msg=f'shard_id not found in node{i}')
            post_shard_ids[i] = (body['shard_id'])
            post_down[post_shard_ids[i]] = body['keys']
        
        # I want the length of the shards to be with 10% of the average
        average_shard_length = MAX_KEYS // 2
        # Check if len(post_down[shard_ids[i]]) is within 10% of average_shard_length
        for i in range(2):
            self.assertTrue(abs(len(post_down[post_shard_ids[i]]) - average_shard_length) <= average_shard_length * 0.1, msg=f'Bad shard length in shard {post_shard_ids[i]}')

        keys_added = []
        for key in post_down[post_shard_ids[0]]:
            if key not in pre_down[pre_shard_ids[0]]:
                keys_added.append(key)
        for key in post_down[post_shard_ids[1]]:
            if key not in pre_down[pre_shard_ids[1]]:
                keys_added.append(key)
        
        # I now need to check that the keys in keys_added are exactly the ones in shard3
        self.assertTrue(len(keys_added) == len(pre_down[pre_shard_ids[2]]), msg='Incorrect number of keys added')

    def test_shard_up(self):
        # Create a view for 3 nodes and 2 shards
        res = put(kvs_view_admin_url(ports[0], hosts[0]), put_view_body(view_addresses[:3], 2))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        # Insert 16 keys to node1
        for i in range(16):
            res = put(kvs_data_key_url(keys[i], ports[0], hosts[0]), put_val_body(vals[i]))
            self.assertEqual(res.status_code, 201, msg='Bad status code')
        
        # Wait 2 seconds for the keys to be replicated
        time.sleep(2)

        pre_up = {}
        post_up = {}

        # Get kvs/data from node1
        res = get(kvs_data_url(ports[0], hosts[0]))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body_1 = res.json()
        self.assertIn('keys', body_1, msg='Keys not found in json response')
        self.assertIn('shard_id', body_1, msg='Shard_id not found in json response')
        pre_up[body_1['shard_id']] = body_1['keys']

        # Get kvs/data from node2
        res = get(kvs_data_url(ports[1], hosts[1]))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body_2 = res.json()
        self.assertIn('keys', body_2, msg='Keys not found in json response')
        self.assertIn('shard_id', body_2, msg='Shard_id not found in json response')
        pre_up[body_2['shard_id']] = body_2['keys']

        # Get kvs/data from node3
        res = get(kvs_data_url(ports[2], hosts[2]))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body_3 = res.json()
        self.assertIn('keys', body_3, msg='Keys not found in json response')
        self.assertIn('shard_id', body_3, msg='Shard_id not found in json response')
        pre_up[body_3['shard_id']] = body_3['keys']

        # Now I have the keys that are in each shard in the store dictionary
        # I am going to check if the keys are balanced
        # I am expecting the store to have 2 shards, each with 8 keys
        self.assertEqual(len(pre_up), 2, msg='Bad number of shards')
        self.assertEqual(len(pre_up[0]), 8, msg='Bad number of keys in shard 0')
        self.assertEqual(len(pre_up[1]), 8, msg='Bad number of keys in shard 1')

        # Now I am going to view change to 3 shards
        res = put(kvs_view_admin_url(ports[0], hosts[0]), put_view_body(view_addresses[:3], 3))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        # Wait 2 seconds for the keys to be replicated
        time.sleep(2)

        # Get kvs/data from node1
        res = get(kvs_data_url(ports[0], hosts[0]))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body_1 = res.json()
        self.assertIn('keys', body_1, msg='Keys not found in json response')
        self.assertIn('shard_id', body_1, msg='Shard_id not found in json response')
        post_up[body_1['shard_id']] = body_1['keys']

        # Get kvs/data from node2
        res = get(kvs_data_url(ports[1], hosts[1]))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body_2 = res.json()
        self.assertIn('keys', body_2, msg='Keys not found in json response')
        self.assertIn('shard_id', body_2, msg='Shard_id not found in json response')
        post_up[body_2['shard_id']] = body_2['keys']

        # Get kvs/data from node3
        res = get(kvs_data_url(ports[2], hosts[2]))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body_3 = res.json()
        self.assertIn('keys', body_3, msg='Keys not found in json response')
        self.assertIn('shard_id', body_3, msg='Shard_id not found in json response')
        post_up[body_3['shard_id']] = body_3['keys']

        # Now I have the keys that are in each shard in the post_up dictionary
        # I am going to check if the keys are balanced
        # I am expecting the store to have 3 shards, each with either 5 or 6 keys
        # We do not know which shard has which amount of keys
        self.assertEqual(len(post_up), 3, msg='Bad number of shards')
        self.assertTrue(len(post_up[0]) == 5 or len(post_up[0]) == 6, msg='Bad number of keys in shard 0')
        self.assertTrue(len(post_up[1]) == 5 or len(post_up[1]) == 6, msg='Bad number of keys in shard 1')
        self.assertTrue(len(post_up[2]) == 5 or len(post_up[2]) == 6, msg='Bad number of keys in shard 2')
        
        # We now need to check what keys were lost from each shard
        # We can do this by checking the keys that were in each shard before the view change
        # and comparing them to the keys that are in each shard after the view change
        # We can then check if the keys that were lost are in the third shard
        lost_keys = {}

        # Check shard 1
        lost_keys['shard_1'] = []
        for key in pre_up[1]:
            if key not in post_up[1]:
                lost_keys['shard_1'].append(key)
        
        # Check shard 2
        lost_keys['shard_2'] = []
        for key in pre_up[2]:
            if key not in post_up[2]:
                lost_keys['shard_2'].append(key)
        
        # Now we need to check if the keys that were lost are in shard 3
        # We can do this by checking if the keys that were lost are in the keys that are in shard 3
        # If they are, then the keys were not lost
        # If they are not, then the keys were lost
        for key in lost_keys['shard_1']:
            self.assertIn(key, post_up[3], msg='Key was lost from shard 1')

        for key in lost_keys['shard_2']:
            self.assertIn(key, post_up[3], msg='Key was lost from shard 2')
        
        
    def test_shard_many_get_put(self):
        # Create the view for 3 nodes and 3 shards
        res = put(kvs_view_admin_url(ports[0], hosts[0]), put_view_body(view_addresses[:3], 3))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        keys_to_insert = 100
        # Insert keys_to_insert keys to node1
        for i in range(keys_to_insert):
            res = put(kvs_data_key_url(keys[i], ports[0], hosts[0]), put_val_body(vals[i]))
            self.assertEqual(res.status_code, 201, msg='Bad status code')
        
        # Create the view for 3 nodes and 2 shards
        res = put(kvs_view_admin_url(ports[0], hosts[0]), put_view_body(view_addresses[:3], 2))
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        
        time.sleep(5)
        
        

        # Get keys_to_insert keys from node2
        for i in range(keys_to_insert):
            res = get(kvs_data_key_url(keys[i], ports[1], hosts[1]))
            self.assertEqual(res.status_code, 200, msg='Bad status code')
if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
