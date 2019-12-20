workspace(name = "com_github_liquidata_inc_dolt")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Go support

http_archive(
    name = "io_bazel_rules_go",
    urls = [
        "https://storage.googleapis.com/bazel-mirror/github.com/bazelbuild/rules_go/releases/download/v0.20.2/rules_go-v0.20.2.tar.gz",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.20.2/rules_go-v0.20.2.tar.gz",
    ],
    sha256 = "b9aa86ec08a292b97ec4591cf578e020b35f98e12173bbd4a921f84f583aebd9",
)

load("@io_bazel_rules_go//go:deps.bzl", "go_rules_dependencies", "go_register_toolchains")

go_rules_dependencies()

go_register_toolchains()

# Gazelle support

http_archive(
    name = "bazel_gazelle",
    urls = [
        "https://storage.googleapis.com/bazel-mirror/github.com/bazelbuild/bazel-gazelle/releases/download/v0.19.1/bazel-gazelle-v0.19.1.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.19.1/bazel-gazelle-v0.19.1.tar.gz",
    ],
    sha256 = "86c6d481b3f7aedc1d60c1c211c6f76da282ae197c3b3160f54bd3a8f847896f",
)

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")

gazelle_dependencies()

# Our Gazelle / go.mod dependencies.

local_repository(
    name = "com_github_liquidata_inc_dolt_go_gen_proto_dolt_services_eventsapi",
    path = "go/gen/proto/dolt/services/eventsapi",
)

# gazelle:repository_macro bazel/go_repositories.bzl%go_repositories

load("@//bazel:go_repositories.bzl", "go_repositories")

go_repositories()

# 418c51d37de8db2b66d84061ec7c959b  bazel/go_repositories.bzl
