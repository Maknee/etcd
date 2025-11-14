#!/bin/bash
sudo apt-get update
sudo apt-get install -y build-essential cmake gcc libudev-dev libnl-3-dev libnl-route-3-dev ninja-build pkg-config valgrind python3-dev cython3 python3-docutils pandoc
sudo apt update
sudo apt install -y libtbb-dev
sudo apt install -y libaio-dev libconfig-dev libxxhash-dev gcc
sudo apt install -y make cmake g++ gcc libnuma-dev libgflags-dev numactl

sudo apt install wget -y
sudo apt install rdma-core -y
sudo apt install cmake g++ gcc clang libnuma-dev libgflags-dev numactl -y
sudo apt install libibverbs-dev -y
sudo modprobe ib_uverbs
sudo modprobe mlx4_ib

sudo apt install libboost-all-dev libasio-dev libtbb-dev -y
sudo apt install protobuf-compiler -y

sudo apt install software-properties-common python-setuptools screen curl ant expect-dev python-dev python3-pip protobuf-compiler pkg-config libunwind-dev libssl-dev libprotobuf-dev libevent-dev libgtest-dev g++ cmake libboost-all-dev libevent-dev libdouble-conversion-dev libgoogle-glog-dev libgflags-dev libiberty-dev liblz4-dev liblzma-dev libsnappy-dev make zlib1g-dev binutils-dev libjemalloc-dev libssl-dev pkg-config libunwind-dev libunwind8-dev libelf-dev libdwarf-dev libdouble-conversion-dev libfarmhash-dev libre2-dev libgif-dev libpng-dev libsqlite3-dev libsnappy-dev liblmdb-dev libiberty-dev -y

exit 0
