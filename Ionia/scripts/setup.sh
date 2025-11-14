#!/bin/bash

# Get rdma-core source
if [ ! -d "rdma-core" ]; then
{
    git clone https://github.com/linux-rdma/rdma-core.git
    if [[ $? -ne 0 ]]; then
        echo 'Unable to download rdma-core'
        exit 1
    fi
    cd rdma-core
    cmake .
    sudo make install -j18
    cd ..
} 1>build.log 2>&1
fi


# Get dpdk source
if [ ! -d "dpdk" ]; then
{
    wget "https://fast.dpdk.org/rel/dpdk-19.11.5.tar.xz"
    if [[ $? -ne 0 ]]; then
        echo 'Unable to download DPDK!'
        exit 1
    fi
    tar xf dpdk-19.11.5.tar.xz
    if [[ $? -ne 0 ]]; then
        echo 'Unable to extract DPDK archive!'
        exit 1
    fi
    rm -f dpdk-19.11.5.tar.xz
    mv dpdk-stable-19.11.5 dpdk
    cd dpdk
    sed -i 's/CONFIG_RTE_LIBRTE_MLX5_PMD=n/CONFIG_RTE_LIBRTE_MLX5_PMD=y/g' config/common_base

    sudo make install T=x86_64-native-linuxapp-gcc DESTDIR=/usr -j18

    cd x86_64-native-linuxapp-gcc
    modprobe -a ib_uverbs mlx5_core mlx5_ib

    cd ../..
} 1>build.log 2>&1
fi


exit 0
