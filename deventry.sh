#!/bin/bash

# A quick entrypoint for working in a pristine, ephemeral docker container with Ubuntu.
#
# Suggested usage:
#
# docker run --rm -ti -v `pwd`:/src ubuntu /src/deventry.sh

set -e

BATS_VERSION=1.9.0
GO_VERSION=1.20.4

export DEBIAN_FRONTEND=noninteractive

apt-get update

apt-get install -y curl 
curl -o /go"$GO_VERSION".linux-amd64.tar.gz -L https://go.dev/dl/go"$GO_VERSION".linux-amd64.tar.gz
rm -rf /usr/local/go && tar -C /usr/local -xzf /go"$GO_VERSION".linux-amd64.tar.gz
rm -f /go"$GO_VERSION".linux-amd64.tar.gz

apt-get install -y git python3 python3.10-venv

curl -o /bats-v"$BATS_VERSION".tar.gz -L https://github.com/bats-core/bats-core/archive/refs/tags/v"$BATS_VERSION".tar.gz
cd /
tar zxf /bats-v"$BATS_VERSION".tar.gz
cd /bats-core-"$BATS_VERSION"
./install.sh /usr/local
cd /

python3 -m venv /bats_venv
echo "source /bats_venv/bin/activate" >> /etc/bash.bashrc
source /bats_venv/bin/activate
pip3 install mysql-connector-python pandas pyarrow

unset DEBIAN_FRONTEND
cd /src
export PATH=/root/go/bin:/usr/local/go/bin:"$PATH"
exec /bin/bash -l
