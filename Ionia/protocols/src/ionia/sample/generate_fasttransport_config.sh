#!/bin/bash

SERVER_IP=$(hostname -I | awk '{print $1}')
printf "f 0\nreplica $SERVER_IP:31851" > config_fasttransport
printf "f 0\nreplica $SERVER_IP:31850" > config_fasttransport.client

echo "Copy [config_fasttransport] and [config_fasttransport.client] to client"
echo "Change [config_fasttransport]'s on client to use client ip instead - Run [hostname -I] and replace ip address in the file of the client"

echo "On the client in fasttransport.cc, comment out [GetSession(receiver, replicaIdx, groupIdx)]"

echo "Run command on server [sudo ../bench/replica -i 0 -b 64 -c ./config_fasttransport -m vr]"
echo "Run command on client [sudo ../bench/client -s ./config_fasttransport -c ./config_fasttransport.client -e 60 -m vr -k ./test -n 10 -l latencies.1]"

