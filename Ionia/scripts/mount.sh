sudo mkfs.ext4 /dev/sda4
sudo mkdir /mnt/sda4
sudo mount /dev/sda4 /mnt/sda4
sudo chown -R y4xu /mnt/sda4/
sudo chmod -R g+rw /mnt/sda4

sudo bash -c "echo 2048 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages"
sudo mkdir /mnt/huge
sudo mount -t hugetlbfs nodev /mnt/huge
