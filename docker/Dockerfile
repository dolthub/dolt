# syntax=docker/dockerfile:1.3-labs
FROM --platform=$BUILDPLATFORM ubuntu:22.04

ARG DOLT_VERSION
ARG ARCH

ADD https://github.com/dolthub/dolt/releases/download/v${DOLT_VERSION}/dolt-linux-${ARCH}.tar.gz dolt-linux-${ARCH}.tar.gz
RUN tar zxvf dolt-linux-${ARCH}.tar.gz && \
    cp dolt-linux-${ARCH}/bin/dolt /usr/local/bin && \
    rm -rf dolt-linux-${ARCH} dolt-linux-${ARCH}.tar.gz

ENTRYPOINT ["/usr/local/bin/dolt"]
