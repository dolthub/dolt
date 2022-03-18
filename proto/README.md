Protobuf message and service definitions.

`Makefile` generates golang message and service stub implementations into
//go/gen/proto/. The generated code is checked into the repository.

Dependencies
------------

Dependencies are git submodules in //proto/third_party.

* You need to build protoc in //proto/third_party/protobuf by running `bazel
  build //:protoc` from that directory. Currently tested with bazel version 3.1.0.

* You need to run `go build -o ._protoc-gen-go ./cmd/protoc-gen-go` in
  `third_party/protobuf-go`.

* You need to run `go build -o ._protoc-gen-go-grpc .` in
  `third_party/grpc-go/cmd/protoc-gen-go-grpc`.

Dependency tracking and hermeticity here are poor.

Generating code
---------------

```
$ make
```

or potentially

```
$ make clean all
```
