#!/bin/bash

set -e

BASE_DIR=$PWD

sudo bash -c "echo 16192 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages"

cd splinterdb
./compile.sh
cd ..

rm -rf eRPC

# install eRPC
git clone https://github.com/erpc-io/eRPC

cd eRPC
git checkout 1ef5d5f3b095776bba4ed21a31cc804211136f16

git apply $BASE_DIR/eRPC.patch

cmake . -DTRANSPORT=dpdk -DPERF=true -DCMAKE_BUILD_TYPE=Release
# cmake . -DTRANSPORT=infiniband -DPERF=true -DCMAKE_BUILD_TYPE=Release -DERPC_INFINIBAND=true
sudo make -j`nproc` install

cd ..

rm -rf parallel-hashmap
git clone https://github.com/greg7mdp/parallel-hashmap

rm -rf concurrentqueue
git clone https://github.com/cameron314/concurrentqueue

rm -rf readerwriterqueue
git clone https://github.com/cameron314/readerwriterqueue

rm -rf unordered_dense
git clone https://github.com/martinus/unordered_dense

rm -rf function2
git clone https://github.com/Naios/function2

rm -f protobuf-cpp-3.21.12.tar.gz
wget https://github.com/protocolbuffers/protobuf/releases/download/v21.12/protobuf-cpp-3.21.12.tar.gz
tar xvf protobuf-cpp-3.21.12.tar.gz
rm -f protobuf-cpp-3.21.12.tar.gz
cd protobuf-3.21.12 && ./autogen.sh && ./configure && make -j && sudo make install && sudo ldconfig
cd ..

git clone https://github.com/jemalloc/jemalloc.git && cd jemalloc && autoconf && ./configure && make dist && sudo make install
cd ..
