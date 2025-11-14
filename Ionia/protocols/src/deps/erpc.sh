#!/bin/bash

set -e

BASE_DIR=$PWD

# install eRPC
git clone https://github.com/erpc-io/eRPC

cd eRPC

git apply $BASE_DIR/eRPC.patch

cmake . -DTRANSPORT=dpdk
sudo make -j`nproc` install

