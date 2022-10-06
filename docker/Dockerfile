# syntax=docker/dockerfile:1.3-labs
FROM ubuntu:22.04

ADD https://github.com/dolthub/dolt/releases/download/v${DOLT_VERSION}/dolt-linux-amd64.tar.gz dolt-linux-amd64.tar.gz
RUN tar zxvf dolt-linux-amd64.tar.gz && \
    cp dolt-linux-amd64/bin/dolt /usr/local/bin && \
    rm -rf dolt-linux-amd64 dolt-linux-amd64.tar.gz

CMD ["/usr/local/bin/dolt"]