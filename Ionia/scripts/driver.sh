wget "https://content.mellanox.com/ofed/MLNX_OFED-5.6-2.0.9.0/MLNX_OFED_LINUX-5.6-2.0.9.0-ubuntu20.04-x86_64.iso"
sudo mount -o ro,loop MLNX_OFED_LINUX-5.6-2.0.9.0-ubuntu20.04-x86_64.iso /mnt
sudo /mnt/mlnxofedinstall --upstream-libs --dpdk --force
sudo /etc/init.d/openibd restart
