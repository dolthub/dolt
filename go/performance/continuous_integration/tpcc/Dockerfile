FROM golang:1.17.1-buster

ENV DEBIAN_FRONTEND=noninteractive

# Get TPCC installed
RUN apt update
RUN apt install -y curl
RUN apt install -y git
RUN apt install -y lua5.3
RUN curl -s https://packagecloud.io/install/repositories/akopytov/sysbench/script.deb.sh | bash
RUN apt -y install sysbench

# Install sqlite3 from source
RUN \
  apt-get install -y \
  build-essential \
  tcl \
  lsb-release \
  && wget \
    -O sqlite.tar.gz \
    https://www.sqlite.org/src/tarball/sqlite.tar.gz?r=release \
  && tar xvfz sqlite.tar.gz \
  # Configure and make SQLite3 binary
  && ./sqlite/configure --prefix=/usr \
  && make \
  && make install \
  # Smoke test
  && sqlite3 --version

WORKDIR /
COPY ./go /dolt/go
COPY ./tpcc-config.json /tpcc-config.json
COPY ./tpcc-runner-tests-entrypoint.sh /entrypoint.sh
RUN git clone https://github.com/Percona-Lab/sysbench-tpcc

WORKDIR /mysql
RUN curl -L -O https://dev.mysql.com/get/mysql-apt-config_0.8.22-1_all.deb
RUN dpkg -i mysql-apt-config_0.8.22-1_all.deb
RUN apt-get update && apt-get install -y mysql-server
RUN mysql --version

# Install dolt
WORKDIR /dolt/go/cmd/dolt
RUN go build -o /usr/local/bin/dolt .

WORKDIR /dolt/go/performance/utils/tpcc_runner/cmd
ENTRYPOINT ["/entrypoint.sh"]
