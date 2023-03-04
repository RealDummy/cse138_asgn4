#! /bin/bash

nth () {
    echo $1 | cut -d " " -f $2
}

ports=$( cat tests/metadata/ports.txt )

curl \
--request GET \
--header "Content-Type: application/json" \
"http://localhost:$(nth "$ports" $1)/kvs/admin/view"