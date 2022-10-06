# syntax=docker/dockerfile:1.3-labs
FROM ubuntu:20.04

WORKDIR /
ARG DOLT_VERSION
ADD https://github.com/dolthub/dolt/releases/download/v${DOLT_VERSION}/dolt-linux-amd64.tar.gz dolt-linux-amd64.tar.gz
RUN tar zxvf dolt-linux-amd64.tar.gz && \
    cp dolt-linux-amd64/bin/dolt /usr/local/bin && \
    rm -rf dolt-linux-amd64 dolt-linux-amd64.tar.gz \

ENV HOST="0.0.0.0"
ENV PORT=3306
ENV DOLT_ROOT_PATH="/.dolt"
ENV DATABASE_NAME=$DATABASE_NAME
ENV DOLTHUB_USER=$DOLTHUB_USER
ENV DOLTHUB_EMAIL=$DOLTHUB_EMAIL
ENV DATA_DIR="/doltdbs/$DOLTHUB_USER/$DATABASE_NAME"

EXPOSE 3306
VOLUME [ "/doltdbs" ]

COPY docker/docker-entrypoint.sh /usr/local/bin/docker/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker/docker-entrypoint.sh
ENTRYPOINT [ "/usr/local/bin/docker/docker-entrypoint.sh" ]

WORKDIR "/doltdbs/$DOLTHUB_USER"
CMD [ "dolt", "sql-server", "-l=trace", "--host=0.0.0.0", "--port=3306"]
