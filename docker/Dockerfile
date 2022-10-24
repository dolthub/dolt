# syntax=docker/dockerfile:1.3-labs
FROM --platform=$BUILDPLATFORM ubuntu:22.04

ARG DOLT_VERSION

ADD https://github.com/dolthub/dolt/releases/download/v${DOLT_VERSION}/dolt-linux-amd64.tar.gz dolt-linux-$BUILDARCH.tar.gz
RUN tar zxvf dolt-linux-$BUILDARCH.tar.gz && \
    cp dolt-linux-$BUILDARCH/bin/dolt /usr/local/bin && \
    rm -rf dolt-linux-$BUILDARCH dolt-linux-$BUILDARCH.tar.gz

ENTRYPOINT ["/usr/local/bin/dolt"]
