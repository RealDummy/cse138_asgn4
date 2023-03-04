#! /bin/bash

ips=$( cat tests/metadata/ips.txt )


count=1
for ip in $ips; do
    cmds=""
    for ip2 in $ips; do
        ip3=$(echo $ip2 | grep -o -E "[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+")
        cmds+="iptables -D INPUT -s $ip3 -j DROP\n iptables -D OUTPUT -s $ip3 -j DROP\n "
    done
    printf "$cmds" | docker exec -i --privileged "kvs-replica$count" bash 2>/dev/null
    count=$(($count+1))
done

