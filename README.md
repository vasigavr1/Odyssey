# Odyssey

Odyssey is a framework that allows developers to easily design, 
measure and deploy 
multi-threaded replication protocols over RDMA.
The odlib submodule is responsible for this.

The repo contains the implementation of 10 protocols using odlib:
1. Zab (inside zookeeper)
2. Multi-Paxos (inside zookeeper)
3. CHT (inside cht) 
4. CHT multi-leader (inside cht)
5. CRAQ (inside craq)
6. Derecho (inside derecho)
7. Classic Paxos (inside kite)
8. All-aboard Paxos (inside kite)
9. ABD (inside kite)
10. Hermes (inside hermes) 

## Odyssey API
Odyssey API contains two flavours (a blocking and a nonblocking) of the following commands
1. read()
2. write()
3. release()
4. acquire()
5. CAS_strong()
6. CAS_weak()
7. FAA()


The Odyssey API can be used by the client threads.
./odlib/src/client.c already contains implementations of
* The Treiber Stack
* Michael & Scott Queues
* Harris and Michael Lists
* A circular producer consumer pattern

Also a User Interface to issue requests from the Command Line is available.


### Dependencies
1. numactl
2. libgsl0-dev
3. libnuma-dev
4. MLNX_OFED_LINUX-4.1-1.0.2.0

### Settings
1. Run subnet-manager in one of the nodes: '/etc/init.d/opensmd start'
2. On every node apply the following:
3. echo 8192 | tee /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages /sys/devices/system/node/node1/hugepages/hugepages-2048kB/nr_hugepages > /dev/null
4. echo 10000000001 | tee /proc/sys/kernel/shmmax /proc/sys/kernel/shmall > /dev/null
 * Make sure that the changes have been applied using cat on the above files
 * The above changes are temporary (i.e. need to be performed after a reboot)


### How to run Odyssey
To run Odyssey:
1. Modify the script ./bin/copy-run.sh to contain the ip-addresses of the machines that will run Odyssey
2. Run the script in all machines passing as a parameter the name of the executable.

The script bin/copy-executables.sh  can be used to compile in one 
machine and then copy the executable in the rest of the machines. 
(The machines must be specified within the script).


## Tested on
* Infiniband cluster of 5 inter-connected nodes, via a Mellanox MSX6012F-BS switch, each one equiped with a single-port 56Gb Infiniband NIC (Mellanox MCX455A-FCAT PCIe-gen3 x16).
* OS: Ubuntu 18.04.1 LTS (Kernel: 4.15.0-55-generic)


### Example

To run Hermes while in the Odyssey directory:

```sh
cmake -B build
./bin/copy-run.sh hermes
```

The script ./bin/copy-run.sh will
* make hermes in build
* copy the hermes executable to all nodes specified in bin/copy_executables.sh
* copy bin/run-exe.sh to the same set of machines. This is the script used to run a protocol.
* Then it will execute the following
```sh
./bin/run-exe.sh hermes
```
* This same command must be executed in the rest of the machines of the deployment

### Using git
To push changes to github use the script
```sh
./bin/git-scripts/git-all-push.sh "meaningfull message"
```

--------------------------------------------------------------
The title of the project is inspired by [this](
https://www.youtube.com/watch?v=NQBFCaQPtEs)
