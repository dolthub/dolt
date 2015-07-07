# Server

This is an http server for noms. Currently it is read-only.

## Build

```
cd <noms>/clients/server
go build
go test
```

## Run
```
# See flags for more options
./server -file-store="/tmp/foo"
```
