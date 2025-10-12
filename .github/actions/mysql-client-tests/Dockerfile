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
RUN go build -tags icu_static -ldflags "$GO_LDFLAGS" -o /build/bin/dolt ./cmd/dolt

FROM golang_cgo125 AS go_clients_build
COPY dolt/integration-tests/mysql-client-tests/go /build/go/
WORKDIR /build/go/
RUN go build -ldflags "$GO_LDFLAGS" -o /build/bin/go-mysql-client-test

COPY dolt/integration-tests/mysql-client-tests/go-mysql/ /build/go-mysql/
WORKDIR /build/go-mysql/
RUN go build -ldflags "$GO_LDFLAGS" -o /build/bin/go-sql-driver-test

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

FROM gcc:12.5-bookworm AS c_clients_build
RUN apt-get update && apt-get install -y default-libmysqlclient-dev libmariadb-dev && rm -rf /var/lib/apt/lists/*
COPY dolt/integration-tests/mysql-client-tests/c/ /build/c/
WORKDIR /build/c/
RUN make

FROM gcc:12.5-bookworm AS cpp_clients_build
RUN apt-get update && apt-get install -y libmysqlcppconn-dev wget && rm -rf /var/lib/apt/lists/*
RUN wget https://dlm.mariadb.com/4464866/Connectors/cpp/connector-cpp-1.1.7/mariadb-connector-cpp_1.1.7-1+maria~bookworm_amd64.deb \
  && dpkg -i mariadb-connector-cpp_1.1.7-1+maria~bookworm_amd64.deb || apt-get install -y -f \
  && rm mariadb-connector-cpp_1.1.7-1+maria~bookworm_amd64.deb
COPY dolt/integration-tests/mysql-client-tests/cpp/ /build/cpp/
WORKDIR /build/cpp/
RUN make

FROM python:3.14-slim-bookworm AS python_clients_build
RUN apt-get update && apt-get install -y binutils && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir mysql-connector-python==8.0.33 PyMySQL==1.0.2 sqlalchemy==1.4.46 pyinstaller
COPY dolt/integration-tests/mysql-client-tests/python/ /build/python/
WORKDIR /build/python/
RUN pyinstaller --onefile pymysql-test.py
RUN pyinstaller --onefile --collect-all mysql.connector py-sqlalchemy-test.py
RUN pyinstaller --onefile --collect-all mysql.connector py-mysql-connector-test.py

FROM elixir:1.18-slim AS elixir_clients_build
RUN apt-get update && apt-get install -y ca-certificates xz-utils curl && rm -rf /var/lib/apt/lists/*
RUN curl -sSL https://ziglang.org/download/0.14.1/zig-x86_64-linux-0.14.1.tar.xz | tar -xJ
ENV PATH="/zig-x86_64-linux-0.14.1:${PATH}"
COPY dolt/integration-tests/mysql-client-tests/elixir/mix.exs /build/elixir/
WORKDIR /build/elixir/
RUN mix local.hex --force && mix local.rebar --force
RUN mix deps.get
COPY dolt/integration-tests/mysql-client-tests/elixir/ /build/elixir/
RUN MIX_ENV=prod mix release
RUN mkdir -p /build/bin && cp burrito_out/simple_linux /build/bin/elixir-mysql-client-test

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

FROM php:8.3-bookworm AS php_clients_build
RUN docker-php-ext-install mysqli pdo_mysql
COPY dolt/integration-tests/mysql-client-tests/php/ /build/bin/

FROM ruby:3.4-bookworm AS ruby_clients_build
RUN apt-get update && apt-get install -y default-libmysqlclient-dev && rm -rf /var/lib/apt/lists/*
COPY dolt/integration-tests/mysql-client-tests/ruby/Gemfile /build/ruby/
WORKDIR /build/ruby/
RUN bundle install
COPY dolt/integration-tests/mysql-client-tests/ruby/ /build/bin/

FROM php:8.3-bookworm AS runtime
RUN apt-get update && apt-get install -y \
  default-libmysqlclient-dev \
  libmysqlcppconn-dev \
  libmariadb3 \
  libdbd-mysql-perl \
  default-mysql-client \
  postgresql \
  postgresql-contrib \
  postgresql-15-mysql-fdw \
  r-base-core \
  perl-base \
  lsof \
  bats \
  && rm -rf /var/lib/apt/lists/*
# compiled, must come after above
RUN R -e "install.packages(c('RMariaDB', 'RMySQL', 'DBI'), repos='https://cloud.r-project.org/')"

COPY --from=dolt_build /build/bin/ /usr/local/bin/
COPY --from=go_clients_build /build/bin/ /build/bin/go/
COPY --from=rust_clients_build /build/bin/release/ /build/bin/rust/
COPY --from=dotnet_clients_build /build/bin/ /build/bin/dotnet/
COPY --from=c_clients_build /build/bin/ /build/bin/c/
COPY --from=cpp_clients_build /build/bin/ /build/bin/cpp/
COPY --from=python_clients_build /build/python/dist/ /build/bin/python/
COPY --from=elixir_clients_build /build/bin/ /build/bin/elixir/
COPY --from=java_clients_build /build/java/target/ /build/bin/java/
COPY --from=node_clients_build /build/bin/ /build/bin/node/
COPY --from=php_clients_build /build/bin/ /build/bin/php/
COPY --from=ruby_clients_build /build/bin/ /build/bin/ruby/
COPY dolt/integration-tests/mysql-client-tests/r/ /build/bin/r/
COPY dolt/integration-tests/mysql-client-tests/perl/ /build/bin/perl/

COPY --from=cpp_clients_build /usr/lib/x86_64-linux-gnu/libmariadb* /usr/lib/x86_64-linux-gnu/
COPY --from=cpp_clients_build /usr/include/mariadb/ /usr/include/mariadb/
COPY --from=java_clients_build /build/jre/ /opt/jre/
COPY --from=node_clients_build /usr/local/bin/node /usr/local/bin/node
COPY --from=php_clients_build /usr/local/lib/php/extensions/ /usr/local/lib/php/extensions/
COPY --from=php_clients_build /usr/local/etc/php/conf.d/ /usr/local/etc/php/conf.d/
COPY --from=ruby_clients_build /usr/local/bin/ruby /usr/local/bin/ruby
COPY --from=ruby_clients_build /usr/local/lib/ /usr/local/lib/
COPY --from=ruby_clients_build /usr/local/bundle/ /usr/local/bundle/
ENV PATH="/opt/jre/bin:/usr/local/bundle/bin:${PATH}"
ENV GEM_HOME="/usr/local/bundle"
ENV BUNDLE_APP_CONFIG="/usr/local/bundle"

COPY dolt/integration-tests/mysql-client-tests/mysql-client-tests.bats /build/bin/
COPY dolt/integration-tests/mysql-client-tests/helpers.bash /build/bin/
COPY dolt/integration-tests/mysql-client-tests/mysql-client-tests-entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
