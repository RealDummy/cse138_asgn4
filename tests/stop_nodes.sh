#! /bin/bash

for n in $(seq $1); do
    docker stop "kvs-replica$(($n))" 1> /dev/null
done

rm -f tests/metadata/*.json