# syntax=docker/dockerfile:1.3-labs
FROM --platform=linux/amd64 ubuntu:22.04

ARG DOLT_VERSION=0.50.6

ADD https://github.com/dolthub/dolt/releases/download/v${DOLT_VERSION}/dolt-linux-amd64.tar.gz dolt-linux-amd64.tar.gz
RUN tar zxvf dolt-linux-amd64.tar.gz && \
    cp dolt-linux-amd64/bin/dolt /usr/local/bin && \
    rm -rf dolt-linux-amd64 dolt-linux-amd64.tar.gz

ENTRYPOINT ["/usr/local/bin/dolt"]
