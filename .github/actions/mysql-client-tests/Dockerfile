# syntax=docker/dockerfile:1
FROM golang:1.25-alpine AS golang_cgo125
ENV CGO_ENABLED=1
ENV GO_LDFLAGS="-linkmode external -extldflags '-static'"
RUN apk add --no-cache build-base


FROM golang_cgo125 AS dolt_build
COPY dolt/go/go.mod /build/dolt/go/
COPY go-mysql-server*/ /build/go-mysql-server/
COPY vitess*/ /build/vitess/
WORKDIR /build/dolt/go/
RUN go mod download
RUN apk add --no-cache icu-dev icu-static
COPY dolt/go/ /build/dolt/go/
RUN go build -tags icu_static -o /build/bin/dolt ./cmd/dolt


FROM golang_cgo125 AS go_clients_build
COPY dolt/integration-tests/mysql-client-tests/go /build/go/
WORKDIR /build/go/
RUN go build -o /build/bin/go-mysql-client-test

COPY dolt/integration-tests/mysql-client-tests/go-mysql/ /build/go-mysql/
WORKDIR /build/go-mysql/
RUN go build -o /build/bin/go-sql-driver-test


FROM rust:1.90-alpine3.22 AS rust_clients_build
COPY dolt/integration-tests/mysql-client-tests/rust/ /build/rust/
WORKDIR /build/rust/
RUN apk add --no-cache musl-dev
RUN cargo build --release --target-dir /build/bin/ # exe is in release/


FROM mcr.microsoft.com/dotnet/sdk:9.0-bookworm-slim AS dotnet_clients_build
COPY dolt/integration-tests/mysql-client-tests/dotnet/MySqlClient/ /build/dotnet/MySqlClient/
WORKDIR /build/dotnet/MySqlClient/
RUN dotnet publish -c Release -o /build/bin

COPY dolt/integration-tests/mysql-client-tests/dotnet/MySqlConnector/ /build/dotnet/MySqlConnector/
WORKDIR /build/dotnet/MySqlConnector/
RUN dotnet publish -c Release -o /build/bin


FROM mcr.microsoft.com/devcontainers/cpp:1-bookworm AS  c_clients_build
COPY dolt/integration-tests/mysql-client-tests/c/vcpkg.json /build/c/vcpkg.json
WORKDIR /build/c/
RUN vcpkg install --triplet=x64-linux-release
COPY dolt/integration-tests/mysql-client-tests/c/ /build/c/
ENV CMAKE_TOOLCHAIN_FILE=/usr/local/vcpkg/scripts/buildsystems/vcpkg.cmake
RUN cmake -B build -S .
RUN cmake --build build







