# syntax=docker/dockerfile:1.3-labs
FROM ubuntu:22.04

ARG DOLT_VERSION
ARG BUILDARCH

RUN echo "DUSTIN: BUILDARCH: $BUILDARCH"

RUN apt-get update -y && apt-get install file -y

RUN apt update -y && \
    apt install -y \
        tini \
        ca-certificates && \
    apt clean && \
    rm -rf /var/lib/apt/lists/*

ADD https://github.com/dolthub/dolt/releases/download/v${DOLT_VERSION}/dolt-linux-${BUILDARCH}.tar.gz dolt-linux-${BUILDARCH}.tar.gz
RUN tar zxvf dolt-linux-${BUILDARCH}.tar.gz

RUN file dolt-linux-${BUILDARCH}/bin/dolt

RUN cp dolt-linux-${BUILDARCH}/bin/dolt /usr/local/bin && \
    rm -rf dolt-linux-${BUILDARCH} dolt-linux-${BUILDARCH}.tar.gz

WORKDIR /var/lib/dolt
ENTRYPOINT ["tini", "--", "/usr/local/bin/dolt"]
