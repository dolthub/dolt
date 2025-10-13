# syntax=docker/dockerfile:1
FROM golang:1.25-alpine AS golang_cgo125
ENV CGO_ENABLED=1
ENV GO_LDFLAGS="-linkmode external -extldflags '-static'"
RUN apk add --no-cache build-base

FROM golang_cgo125 AS dolt_build
RUN apk add --no-cache icu-dev icu-static
COPY dolt/go/go.mod /build/dolt/go/
WORKDIR /build/dolt/go/
RUN go mod download
COPY dolt/go/ /build/dolt/go/
RUN go build -tags icu_static -ldflags "$GO_LDFLAGS" -o /build/bin/dolt ./cmd/dolt

FROM golang_cgo125 AS go_clients_build
COPY dolt/integration-tests/mysql-client-tests/go /build/go/
WORKDIR /build/go/
RUN go build -ldflags "$GO_LDFLAGS" -o /build/bin/mysql-client-test

COPY dolt/integration-tests/mysql-client-tests/go-mysql/ /build/go-mysql/
WORKDIR /build/go-mysql/
RUN go build -ldflags "$GO_LDFLAGS" -o /build/bin/sql-driver-mysql-test

FROM rust:1.90-alpine3.22 AS rust_clients_build
RUN apk add --no-cache musl-dev
COPY dolt/integration-tests/mysql-client-tests/rust/ /build/rust/
WORKDIR /build/rust/
RUN cargo build --release --target-dir /build/bin/ # exe is in release/

FROM mcr.microsoft.com/dotnet/sdk:9.0-bookworm-slim AS dotnet_clients_build
COPY dolt/integration-tests/mysql-client-tests/dotnet/MySqlClient/*.csproj /build/dotnet/MySqlClient/
WORKDIR /build/dotnet/MySqlClient/
RUN dotnet restore
COPY dolt/integration-tests/mysql-client-tests/dotnet/MySqlClient/ /build/dotnet/MySqlClient/
RUN dotnet publish -c Release -o /build/bin --no-restore

COPY dolt/integration-tests/mysql-client-tests/dotnet/MySqlConnector/*.csproj /build/dotnet/MySqlConnector/
WORKDIR /build/dotnet/MySqlConnector/
RUN dotnet restore
COPY dolt/integration-tests/mysql-client-tests/dotnet/MySqlConnector/ /build/dotnet/MySqlConnector/
RUN dotnet publish -c Release -o /build/bin --no-restore
# devart dotconnect reqs a license so we've skipped it here

FROM gcc:12.5-bookworm AS c_clients_build
# default-libmysqlclient-dev uses libmariadb under the hood but here we test both header interfaces
RUN apt-get update && apt-get install -y default-libmysqlclient-dev libmariadb-dev unixodbc-dev odbcinst wget && rm -rf /var/lib/apt/lists/*
RUN wget --progress=dot:giga https://dlm.mariadb.com/4465891/Connectors/odbc/connector-odbc-3.2.7/mariadb-connector-odbc_3.2.7-1+maria~bookworm_amd64.deb \
    && dpkg -i mariadb-connector-odbc_3.2.7-1+maria~bookworm_amd64.deb || apt-get install -y -f \
    && rm mariadb-connector-odbc_3.2.7-1+maria~bookworm_amd64.deb \
    && odbcinst -i -d -f /dev/stdin <<EOF
[MariaDB ODBC 3.2 Driver]
Description=MariaDB Connector/ODBC v.3.2
Driver=/usr/lib/x86_64-linux-gnu/libmaodbc.so
EOF
COPY dolt/integration-tests/mysql-client-tests/c/ /build/c/
WORKDIR /build/c/
RUN make

FROM gcc:12.5-bookworm AS cpp_clients_build
RUN apt-get update && apt-get install -y libmysqlcppconn-dev wget && rm -rf /var/lib/apt/lists/*
RUN wget --progress=dot:giga https://dlm.mariadb.com/4464866/Connectors/cpp/connector-cpp-1.1.7/mariadb-connector-cpp_1.1.7-1+maria~bookworm_amd64.deb \
  && dpkg -i mariadb-connector-cpp_1.1.7-1+maria~bookworm_amd64.deb || apt-get install -y -f \
  && rm mariadb-connector-cpp_1.1.7-1+maria~bookworm_amd64.deb
COPY dolt/integration-tests/mysql-client-tests/cpp/ /build/cpp/
WORKDIR /build/cpp/
RUN make

FROM python:3.14-slim-bookworm AS python_clients_build
RUN apt-get update && apt-get install -y binutils libmariadb-dev gcc && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir mysql-connector-python==8.0.33 PyMySQL==1.0.2 sqlalchemy==1.4.46 mariadb pyinstaller
COPY dolt/integration-tests/mysql-client-tests/python/ /build/python/
WORKDIR /build/python/
RUN pyinstaller --onefile pymysql-test.py
RUN pyinstaller --onefile --collect-all mysql.connector sqlalchemy-test.py
RUN pyinstaller --onefile --collect-all mysql.connector mysql-connector-test.py
RUN pyinstaller --onefile mariadb-connector-test.py

FROM elixir:1.18-slim AS elixir_clients_build
RUN apt-get update && apt-get install -y ca-certificates xz-utils curl && rm -rf /var/lib/apt/lists/*
RUN curl -sSL https://ziglang.org/download/0.14.1/zig-x86_64-linux-0.14.1.tar.xz | tar -xJ
ENV PATH="/zig-x86_64-linux-0.14.1:${PATH}"
COPY dolt/integration-tests/mysql-client-tests/elixir/myxql/mix.exs /build/elixir/myxql/
WORKDIR /build/elixir/myxql/
RUN mix local.hex --force && mix local.rebar --force
RUN mix deps.get
COPY dolt/integration-tests/mysql-client-tests/elixir/myxql/ /build/elixir/myxql/
RUN MIX_ENV=prod mix release simple
COPY dolt/integration-tests/mysql-client-tests/elixir/mysql/mix.exs /build/elixir/mysql/
WORKDIR /build/elixir/mysql/
RUN mix deps.get
COPY dolt/integration-tests/mysql-client-tests/elixir/mysql/ /build/elixir/mysql/
RUN MIX_ENV=prod mix release mysql_otp
RUN mkdir -p /build/bin && \
    cp /build/elixir/myxql/burrito_out/simple_linux /build/bin/myxql-driver-test && \
    cp /build/elixir/mysql/burrito_out/mysql_otp_linux /build/bin/mysql-otp-test

FROM maven:3.9.11-amazoncorretto-21-debian-bookworm AS java_clients_build
RUN apt-get update && apt-get install -y binutils && rm -rf /var/lib/apt/lists/*
COPY dolt/integration-tests/mysql-client-tests/java/ /build/java/
WORKDIR /build/java/
RUN mvn package
RUN jlink \
    --add-modules java.base,java.sql,java.naming,java.xml,java.logging,java.management \
    --strip-debug \
    --no-man-pages \
    --no-header-files \
    --output /build/jre

FROM node:22-bookworm-slim AS node_clients_build
COPY dolt/integration-tests/mysql-client-tests/node/package.json /build/bin/
WORKDIR /build/bin/
RUN npm install --omit=dev
COPY dolt/integration-tests/mysql-client-tests/node/ /build/bin/

FROM ruby:3.4-bookworm AS ruby_clients_build
RUN apt-get update && apt-get install -y default-libmysqlclient-dev && rm -rf /var/lib/apt/lists/*
COPY dolt/integration-tests/mysql-client-tests/ruby/Gemfile /build/ruby/
WORKDIR /build/ruby/
RUN bundle install
COPY dolt/integration-tests/mysql-client-tests/ruby/ /build/bin/

FROM php:8.3-bookworm AS php_deps
RUN docker-php-ext-install mysqli pdo_mysql

FROM swift:5.10-bookworm AS swift_clients_build
RUN apt-get update && apt-get install -y libmariadb-dev pkg-config libstdc++-12-dev && rm -rf /var/lib/apt/lists/*
COPY dolt/integration-tests/mysql-client-tests/swift/ /build/swift/
WORKDIR /build/swift/
RUN swift build -c release --static-swift-stdlib
RUN mkdir -p /build/bin && cp .build/release/MariaDBTest /build/bin/mariadb-swift-test

FROM debian:bookworm-slim AS r_deps
RUN apt-get update && apt-get install -y r-base-core libmariadb-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /build/r/
RUN R -e "install.packages('DBI', INSTALL_opts='--build', repos='https://cloud.r-project.org/')" && \
  R -e "install.packages('RMySQL', INSTALL_opts='--build', repos='https://cloud.r-project.org/')" && \
  R -e "install.packages('RMariaDB', INSTALL_opts='--build', repos='https://cloud.r-project.org/')"

FROM php:8.3-bookworm AS runtime
RUN apt-get update && apt-get install -y \
  libmysqlcppconn-dev \
  libdbd-mysql-perl \
  libdbd-mariadb-perl \
  default-mysql-client \
  postgresql-15-mysql-fdw \
  r-base-core \
  lsof \
  bats \
  && rm -rf /var/lib/apt/lists/*

# Runtime Libraries
COPY --from=cpp_clients_build /usr/lib/x86_64-linux-gnu/libmariadb* /usr/lib/x86_64-linux-gnu/
COPY --from=c_clients_build /usr/lib/x86_64-linux-gnu/ /usr/lib/x86_64-linux-gnu/
COPY --from=c_clients_build /etc/odbcinst.ini /etc/odbcinst.ini
COPY --from=java_clients_build /build/jre/ /opt/jre/
COPY --from=node_clients_build /usr/local/bin/node /usr/local/bin/
COPY --from=php_deps /usr/local/lib/php/extensions/ /usr/local/lib/php/extensions/
COPY --from=php_deps /usr/local/etc/php/conf.d/ /usr/local/etc/php/conf.d/
COPY --from=ruby_clients_build /usr/local/bin/ruby /usr/local/bin/
COPY --from=ruby_clients_build /usr/local/lib/ /usr/local/lib/
COPY --from=ruby_clients_build /usr/local/bundle/ /usr/local/bundle/

COPY --from=r_deps /build/r/*.tar.gz /tmp/r-packages/
RUN R CMD INSTALL /tmp/r-packages/*.tar.gz && rm -rf /tmp/r-packages/

ENV PATH="/opt/jre/bin:${PATH}" GEM_HOME="/usr/local/bundle"
RUN ldconfig

COPY --from=go_clients_build /build/bin/ /build/bin/go/
COPY --from=rust_clients_build /build/bin/release/ /build/bin/rust/
COPY --from=dotnet_clients_build /build/bin/ /build/bin/dotnet/
COPY --from=c_clients_build /build/bin/ /build/bin/c/
COPY --from=cpp_clients_build /build/bin/ /build/bin/cpp/
COPY --from=python_clients_build /build/python/dist/ /build/bin/python/
COPY --from=elixir_clients_build /build/bin/ /build/bin/elixir/
COPY --from=java_clients_build /build/java/target/ /build/bin/java/
COPY --from=node_clients_build /build/bin/ /build/bin/node/
COPY --from=swift_clients_build /build/bin/ /build/bin/swift/
COPY dolt/integration-tests/mysql-client-tests/php/ /build/bin/php/
COPY --from=ruby_clients_build /build/bin/ /build/bin/ruby/
COPY dolt/integration-tests/mysql-client-tests/r/ /build/bin/r/
COPY dolt/integration-tests/mysql-client-tests/perl/ /build/bin/perl/

COPY dolt/integration-tests/mysql-client-tests/mysql-client-tests.bats /build/bin/
COPY dolt/integration-tests/mysql-client-tests/helpers.bash /build/bin/
COPY dolt/integration-tests/mysql-client-tests/mysql-client-tests-entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

COPY --from=dolt_build /build/bin/ /usr/local/bin/

ENTRYPOINT ["/entrypoint.sh"]
