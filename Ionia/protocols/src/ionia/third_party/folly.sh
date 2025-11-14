cd /tmp
git clone https://github.com/facebook/folly.git
cd folly
git checkout 49926b98f5afb5667d0c06807da79d606a6d43c3
git clone https://github.com/fmtlib/fmt.git
cd fmt
mkdir \_build
cd \_build
cmake ..
make -j
sudo make install
cd /tmp/folly
mkdir \_build
cd \_build
cmake ..
make -j
sudo make install
