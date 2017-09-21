This demo application is the simplest p2p chat app you could build using Noms.

Basic idea:

- Every node runs a Noms HTTP server (port controlled by --port) flag
- Every node broadcasts its current commit and IP/port continuously
- Every node continuously sync/merges with every other node
  (note that due to content addressing, most of these syncs will immediately exit)


