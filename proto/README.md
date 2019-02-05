The top-level directory for protobuf message and service definitions.

`Makefile` generates golang message and service stub implementations into
//go/gen/proto/. The generated code is checked into the repository.

Typescript and javascript artifact generation has been disabled for the time being.

Dependencies
------------

You need `protoc-gen-go` and `ts-protoc-gen`. To build `ts-protoc-gen`, you
need to install bazel.

```
$ go get -u github.com/golang/protobuf/protoc-gen-go

$ mkdir -p $HOME/src/3p
$ pushd $HOME/src/3p
$ [ -d ts-protoc-gen ] || git clone git@github.com:improbable-eng/ts-protoc-gen.git
$ cd ts-protoc-gen
$ git checkout 0.7.6
$ bazel build //...
$ popd
```

Generating code
---------------

```
$ make
```

or potentially

```
$ make clean all
```
