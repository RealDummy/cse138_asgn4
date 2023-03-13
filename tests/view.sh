#! /bin/bash

nth () {
    echo $1 | cut -d " " -f $2
}

ports=$( cat tests/metadata/ports.txt )
ips=$( cat tests/metadata/ips.txt )

view=""
port=$( nth "$ports" $2 )

for arg in ${@:2}; do
    if [ "$view" != "" ]; then
        view+=", "
    fi
    view+="\"$(nth "$ips" $arg)\""
done


curl \
--request PUT \
--header "Content-Type: application/json" \
--write-out "%{http_code}\n" \
--data "{\"nodes\": [$view], \"num_shards\": $1}" \
http://localhost:$port/kvs/admin/view