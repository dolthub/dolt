#!/bin/bash

# A quick entrypoint for working in a pristine, ephemeral docker container with Ubuntu.
#
# Suggested utilization:
#
# docker run --rm -ti -v `pwd`:/src ubuntu /src/deventry.sh

set -e

export DEBIAN_FRONTEND=noninteractive

apt-get update

apt-get install -y curl 
curl -o /go1.20.4.linux-amd64.tar.gz -L https://go.dev/dl/go1.20.4.linux-amd64.tar.gz
rm -rf /usr/local/go && tar -C /usr/local -xzf /go1.20.4.linux-amd64.tar.gz
rm -f /go1.20.4.linux-amd64.tar.gz

apt-get install -y git python3 python3.10-venv

curl -o /bats-v1.9.0.tar.gz -L https://github.com/bats-core/bats-core/archive/refs/tags/v1.9.0.tar.gz
cd /
tar zxf /bats-v1.9.0.tar.gz
cd /bats-core-1.9.0
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
