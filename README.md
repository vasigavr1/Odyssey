# ABD Algorithm(Attiya, Bar-Noy, Dolev )
ABD is an algorithm that implements consistency for DSM systems.
The projects implements 2 variants of ABD: Lin-ABD which offers Linearizability(as described in  https://ieeexplore.ieee.org/abstract/document/614100/) and
SC-ABD which offers Sequential Consistency (as described in https://link.springer.com/chapter/10.1007/978-3-319-46140-3_14)
The protocols are implemented over RDMA.

## Optimizations
The protocol is implemented over UD Sends and Receives.
All messages are batched, the stats will print out the batching of all messages.
The Read will only perfrom a second round, if it detects that the value it is reading is not available to a quorum of machines.
The read replies will not include a key or a timestamp, if the timestamp that is proposed to be read is smaller or equal to the local timestamp


## Repository Contains
1. A modified version of MICA that serves as the store for the ABD
2. A layer that implements the protocol that runs over 1

## Requirments

### Dependencies
1. numactl
1. libgsl0-dev
1. libnuma-dev
1. libatmomic_ops
1. libmemcached-dev
1. MLNX_OFED_LINUX-4.1-1.0.2.0

### Settings
1. Run subnet-manager in one of the nodes: '/etc/init.d/opensmd start'
1. On every node apply the following:
 1. echo 8192 | tee /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages /sys/devices/system/node/node1/hugepages/hugepages-2048kB/nr_hugepages > /dev/null
 1. echo 10000000001 | tee /proc/sys/kernel/shmmax /proc/sys/kernel/shmall > /dev/null
 * Make sure that the changes have been applied using cat on the above files
 * The following changes are temporary (i.e. need to be performed after a reboot)

## Tested on
* Infiniband cluster of 9 inter-connected nodes, via a Mellanox MSX6012F-BS switch, each one equiped with a single-port 56Gb Infiniband NIC (Mellanox MCX455A-FCAT PCIe-gen3 x16).
* OS: Ubuntu 14.04 (Kernel: 3.13.0-32-generic) 
