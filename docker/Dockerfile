# syntax=docker/dockerfile:1.3-labs
FROM --platform=$BUILDPLATFORM ubuntu:22.04

ARG DOLT_VERSION
ARG BUILDARCH

RUN apt update -y && \
    apt install -y \
        tini

ADD https://github.com/dolthub/dolt/releases/download/v${DOLT_VERSION}/dolt-linux-${BUILDARCH}.tar.gz dolt-linux-${BUILDARCH}.tar.gz
RUN tar zxvf dolt-linux-${BUILDARCH}.tar.gz && \
    cp dolt-linux-${BUILDARCH}/bin/dolt /usr/local/bin && \
    rm -rf dolt-linux-${BUILDARCH} dolt-linux-${BUILDARCH}.tar.gz

ENTRYPOINT ["tini", "--", "/usr/local/bin/dolt"]
