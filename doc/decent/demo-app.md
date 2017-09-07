# Sample App: IPFS-based Decentralized Chat

Demo app code is in the [ipfs-chat](https://github.com/attic-labs/noms/tree/master/samples/go/ipfs-chat/) directory. To get it up and running take the following steps:
* Use git to clone the noms repository onto your computer:
```
git clone git@github.com:attic-labs/noms.git or git clone https://github.com/attic-labs/noms.git
```
* From the noms/samples/go/ipfs-chat directory, build the program with the following command:
```
go build
```
* Run the ipfs-chat client with the following command:
```
./ipfs-chat client --username <aname1> --node-idx=1 ipfs:/tmp/ifps1::chat
```
* Run a second ipfs-chat client with the following command:
```
./ipfs-chat client --username <aname2> --node-idx=2 ipfs:/tmp/ifps2::chat
```
  
If desired, ipfs-chat can be run as a daemon which will replicate all chat content in a local store which will enable clients to go offline without causing data to become unavailable to other clients:
```
./ipfs-chat daemon --node-idx=3 ipfs:/tmp/ifps3::chat
```

Note: the 'node-idx' argument ensures that each IPFS-based program uses a distinct set of ports. This is useful when running multiple IPFS-based programs on the same machine.
