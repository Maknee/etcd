#!/bin/bash

# i specifies the replica number

sudo killall -s 9 replica
sudo rm -rf /tmp/vrlog*
sudo ../bench/replica -i 0 -b 64 -c ./config_fasttransport -m vr > /tmp/vrlog.0 2>&1 &
sleep 2
