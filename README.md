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

## Example

To run Hermes while in the Odyssey directory:

```sh
cmake -B build
./bin/copy-run.sh hermes
```

The script ./bin/copy-run.sh will
* make hermes in build
* copy the hermes executable to all nodes specified in bin/copy_executables.sh
* copy bin/run-exe.sh to the same set of machines. This is the script used to run a protocol.
* Then it will execute  the following
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
The title of the project is inspired by this
https://www.youtube.com/watch?v=NQBFCaQPtEs
