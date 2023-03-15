#! /bin/bash


nth () {
    echo $1 | cut -f $2 -d " " 
}

ports=$( cat tests/metadata/ports.txt )

client=$1
key=$2
port=$( nth "$ports" $3 )

if [ "$client" = "" ] || [ "$key" = "" ] || [ "$port" = "" ]; then
    echo "usage $0 <client> <key> <node number>"
    exit 1
fi

metadata=$(printf "" | python tests/request_data.py "tests/metadata/metadata-$client.json")

curl --request  GET --header "Content-Type: application/json" --data "$metadata" http://localhost:$port/kvs/data/$key 2>/dev/null 1> tests/metadata/"metadata-$client.json"

cat tests/metadata/"metadata-$client.json"