# syntax=docker/dockerfile:1
FROM golang:1.25-alpine AS golang_cgo125
ENV CGO_ENABLED=1
ENV GO_LDFLAGS="-linkmode external -extldflags '-static'"
RUN apk add --no-cache build-base

FROM golang_cgo125 AS dolt_build
RUN apk add --no-cache icu-dev icu-static
#COPY go-mysql-server /build/go-mysql-server
COPY dolt/go/go.mod /build/dolt/go/
WORKDIR /build/dolt/go/
RUN go mod download
COPY dolt/go/ /build/dolt/go/
RUN go build -tags icu_static -ldflags "$GO_LDFLAGS" -o /build/bin/dolt ./cmd/dolt

FROM --platform=${BUILDPLATFORM} ubuntu:20.04 AS runtime

# install ORM tools and dependencies
ENV DEBIAN_FRONTEND=noninteractive
RUN apt update -y && \
    apt install -y \
    ca-certificates \
    curl \
    gnupg \
    software-properties-common && \
    curl -sL https://deb.nodesource.com/setup_22.x | bash -
RUN apt update -y && \
    apt install -y \
    nodejs \
    python3.9 \
    python3-pip \
    git \
    mysql-client \
    libmysqlclient-dev \
    netcat-openbsd \
    # weird issue: installing openjdk-17-jdk errors if `maven` or possibly any other package is not installed after it
    openjdk-17-jdk \
    # currently, `apt install maven` installs v3.6.0 which does not work with openjdk-17-jdk
    maven && \
    update-ca-certificates -f

RUN git clone --depth 1 --branch v1.13.0 https://github.com/bats-core/bats-core.git /tmp/bats-core && \
    /tmp/bats-core/install.sh /usr/local && \
    rm -rf /tmp/bats-core

# install mysql connector and pymsql
RUN pip3 install mysql-connector-python PyMySQL sqlalchemy

# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/

# install the current latest maven version, `v3.9.11`, because apt installed one does not work with jdk 17
ADD https://archive.apache.org/dist/maven/maven-3/3.9.11/binaries/apache-maven-3.9.11-bin.tar.gz apache-maven-3.9.11-bin.tar.gz
RUN tar zxvf apache-maven-3.9.11-bin.tar.gz && \
    cp -r apache-maven-3.9.11 /opt && \
    rm -rf apache-maven-3.9.11 apache-maven-3.9.11-bin.tar.gz

# add maven binary
ENV PATH=/opt/apache-maven-3.9.11/bin:$PATH

COPY --from=dolt_build /build/bin/dolt /usr/local/bin/dolt
COPY dolt/integration-tests/orm-tests /orm-tests
COPY dolt/integration-tests/bats/helper /orm-tests/helper
COPY dolt/integration-tests/orm-tests/orm-tests-entrypoint.sh /orm-tests/entrypoint.sh
RUN chmod +x /orm-tests/entrypoint.sh

WORKDIR /orm-tests
ENTRYPOINT ["/orm-tests/entrypoint.sh"]
