cd ../
sudo git clone https://github.com/vmware/splinterdb.git
cd splinterdb
sudo find ./ -type f -exec sed -i -e 's/timestamp/splinterdb_timestamp/g' {} \;
export COMPILER=gcc
export CC=$COMPILER
export LD=$COMPILER
sudo make -j18
sudo make run-tests
sudo make install

sudo cp build/release/lib/libsplinterdb.so /lib/x86_64-linux-gnu/
