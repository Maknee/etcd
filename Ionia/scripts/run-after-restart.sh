cd dpdk
sudo modprobe uio_pci_generic
sudo ./usertools/dpdk-devbind.py --bind=uio_pci_generic 03:00.0
sudo ./usertools/dpdk-devbind.py --bind=uio_pci_generic 07:00.1
