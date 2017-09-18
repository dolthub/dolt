[About Noms](about-noms.md)&nbsp; | &nbsp;[How to Use Noms](how-to-use-noms.md)&nbsp; | &nbsp;[Demo App](demo-app.md)&nbsp; | &nbsp;[Vision](vision.md)&nbsp; | &nbsp;[What's Next](whats-next.md)
<br><br>
[![Build Status](http://jenkins3.noms.io/buildStatus/icon?job=NomsMasterBuilder)](http://jenkins3.noms.io/job/NomsMasterBuilder/)
[![codecov](https://codecov.io/gh/attic-labs/noms/branch/master/graph/badge.svg)](https://codecov.io/gh/attic-labs/noms)
[![GoDoc](https://godoc.org/github.com/attic-labs/noms?status.svg)](https://godoc.org/github.com/attic-labs/noms)
[![Slack](http://slack.noms.io/badge.svg)](http://slack.noms.io)

# Demo App: IPFS-based Decentralized Chat

Demo app code is in the
[ipfs-chat](https://github.com/attic-labs/noms/tree/master/samples/go/ipfs-chat/)
directory. To get it up and running take the following steps:
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
  
If desired, ipfs-chat can be run as a daemon which will replicate all
chat content in a local store which will enable clients to go offline
without causing data to become unavailable to other clients:

```
./ipfs-chat daemon --node-idx=3 ipfs:/tmp/ifps3::chat
```

Note: the 'node-idx' argument ensures that each IPFS-based program
uses a distinct set of ports. This is useful when running multiple
IPFS-based programs on the same machine.
