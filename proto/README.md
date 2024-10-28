Protobuf message and service definitions.

`Makefile` generates golang message and service stub implementations into
//go/gen/proto/. The generated code is checked into the repository.

Dependencies
------------

Dependencies are git submodules in //proto/third_party. Make sure you have all these
submodules synced. If not, you can sync them initially with:
  git submodule update --init 

* Get the 6.3.0 version of bazel from https://github.com/bazelbuild/bazel/releases/tag/6.3.0
  Later versions don't currently work. Verify the version and ensure it's first in your path.

* You need to build protoc in //proto/third_party/protobuf by running:
  `bazel build //:protoc` from that directory.
  WARNING: you may need to simplify your env with a prefix of `PATH=/usr/local/bin:/usr/bin:/bin`

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
