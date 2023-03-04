#! /bin/bash

for n in $(seq $1); do
    docker start "kvs-replica$(($n))" 1>/dev/null
done

sleep 1