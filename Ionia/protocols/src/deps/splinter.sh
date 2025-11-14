sudo rm -rf splinterdb
git clone https://github.com/vmware/splinterdb.git
cd splinterdb
git checkout 4b6e0b1c4f74ad43d46bb05445d7aa8272a7e8f6
sudo find ./ -type f -exec sed -i -e 's/timestamp/splinterdb_timestamp/g' {} \;
sudo find ./ -type f -exec sed -i -e 's/cfg->io_flags = O_RDWR | O_CREAT/cfg->io_flags = O_RDWR | O_CREAT | O_DIRECT/g' {} \;
export COMPILER=gcc 
export CC=$COMPILER 
export LD=$COMPILER 
sudo make -j18
sudo make run-tests 
sudo make install

sudo cp build/release/lib/libsplinterdb.so /lib/x86_64-linux-gnu/
