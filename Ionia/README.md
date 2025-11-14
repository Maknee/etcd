# Ionia

IONIA: High-Performance Replication for key-value stores

This is artifact of `[TDDO: add link to ionia]`

## Requirements

* Multiple (15+) XL170 machines on [cloudlab](https://www.cloudlab.us) (Mellanox MT27710 Family ConnectX-4 Lx/Ubuntu 20.04 LTS)

## Setup

```
Go to setup
Copy the manifest in cloudlab into config.xml
then run 

skyros_upload.py --step=0
...
skyros_upload.py --step=5
```

## Running experiments

Basic experiments for runs are using ycsb
```
./run_everything.sh
```





