Protobuf message and service definitions.

`Makefile` generates golang message and service stub implementations into
//go/gen/proto/. The generated code is checked into the repository.

Dependencies
------------

Dependencies are git submodules in //proto/third_party. You need to build
protoc in //proto/third_party/protobuf by running `bazel build //:protoc` from
that directory. Currently tested with bazel version 0.28.0.

Dependency tracking and hermeticity here poor.

Generating code
---------------

```
$ make
```

or potentially

```
$ make clean all
```
