# syntax=docker/dockerfile:1
ARG MAVEN_VERSION=3.9.11
ARG NODE_MAJOR=22

FROM golang:1.25-alpine AS golang_cgo125
ENV CGO_ENABLED=1
ENV GO_LDFLAGS="-linkmode external -extldflags '-static'"
RUN apk add --no-cache build-base

FROM golang_cgo125 AS dolt_build
RUN apk add --no-cache icu-dev icu-static
#COPY go-mysql-server /build/go-mysql-server
COPY dolt/go/go.mod /build/dolt/go/
WORKDIR /build/dolt/go/
RUN --mount=type=cache,target=/root/go/pkg/mod go mod download
COPY dolt/go/ /build/dolt/go/
RUN --mount=type=cache,target=/root/go/pkg/mod \
    go build -tags icu_static -ldflags "$GO_LDFLAGS" -o /build/bin/dolt ./cmd/dolt

FROM --platform=${BUILDPLATFORM} ubuntu:20.04 AS runtime
ARG MAVEN_VERSION
ARG NODE_MAJOR

ENV DEBIAN_FRONTEND=noninteractive \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/ \
    PATH=/opt/apache-maven-${MAVEN_VERSION}/bin:$PATH

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update -y && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gnupg && \
    curl -fsSL https://deb.nodesource.com/setup_${NODE_MAJOR}.x | bash - && \
    apt-get install -y --no-install-recommends \
        nodejs \
        python3.9 \
        python3-pip \
        git \
        mysql-client \
        libmysqlclient-dev \
        netcat-openbsd \
        # weird issue: installing openjdk-17-jdk errors if `maven` or possibly any other package is not installed after it
        openjdk-17-jdk \
        # apt installs maven v3.6.0 which does not work with openjdk-17; overridden below
        maven && \
    update-ca-certificates -f && \
    rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 --branch v1.13.0 https://github.com/bats-core/bats-core.git /tmp/bats-core && \
    /tmp/bats-core/install.sh /usr/local && \
    rm -rf /tmp/bats-core

RUN pip3 install --no-cache-dir mysql-connector-python PyMySQL sqlalchemy

RUN curl -fsSL https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
        | tar -xzC /opt && \
    ln -s /opt/apache-maven-${MAVEN_VERSION} /opt/apache-maven

COPY --from=dolt_build /build/bin/dolt /usr/local/bin/dolt
COPY dolt/integration-tests/orm-tests /orm-tests
COPY dolt/integration-tests/bats/helper /orm-tests/helper
COPY --chmod=755 dolt/integration-tests/orm-tests/orm-tests-entrypoint.sh /orm-tests/entrypoint.sh

WORKDIR /orm-tests
ENTRYPOINT ["/orm-tests/entrypoint.sh"]
