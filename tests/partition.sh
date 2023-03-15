#! /bin/bash

nth () {
    echo $1 | cut -f $2 -d " "  | grep -o -E "[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+"
}

ips=$( cat tests/metadata/ips.txt )
cmds=""
for i in ${@:2}; do
    cmds+="iptables -A INPUT -s $( nth "$ips" $i ) -j DROP\n iptables -A OUTPUT -s $( nth "$ips" $i ) -j DROP\n "
done

printf "$cmds" | docker exec -i --privileged kvs-replica$1 bash
