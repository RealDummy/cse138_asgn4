#! /bin/bash

nth () {
    echo $1 | cut -d " " -f $2
}

ports=$( cat tests/metadata/ports.txt )
ips=$( cat tests/metadata/ips.txt )

view=""
port=$( nth "$ports" $1 )

for arg in ${@:1}; do
    if [ "$view" != "" ]; then
        view+=", "
    fi
    view+="\"$(nth "$ips" $arg)\""
done


curl \
--request PUT \
--header "Content-Type: application/json" \
--write-out "%{http_code}\n" \
--data "{\"view\": [$view]}" \
http://localhost:$port/kvs/admin/view