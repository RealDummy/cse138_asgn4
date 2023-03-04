#! /bin/bash


nth () {
    echo $1 | cut -d " " -f $2
}

ports=$( cat tests/metadata/ports.txt )
metadata=$(printf "" | python tests/request_data.py "tests/metadata/metadata-$1.json")

curl \
--request GET \
--header "Content-Type: application/json" \
--data "$metadata" \
"http://localhost:$(nth "$ports" $2)/kvs/data"