# syntax=docker/dockerfile:1.3-labs
FROM ubuntu:20.04
WORKDIR /
RUN apt update &&     apt upgrade -y &&     apt install -y curl     && curl -L https://github.com/dolthub/dolt/releases/latest/download/install.sh | bash

ENV HOST="0.0.0.0"
ENV PORT=3306
ENV DOLT_ROOT_PATH="/.dolt"
ENV DATABASE_NAME="mydb"
ENV DOLTHUB_USER="Dolt Server Account"
ENV DOLTHUB_EMAIL="doltServer@company.com"
ENV DATA_DIR="/doltdbs/$DOLT_USER/$DATABASE_NAME"

EXPOSE 3306
VOLUME [ "/doltdbs" ]

COPY docker/docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker/docker-entrypoint.sh
ENTRYPOINT [ "/docker/docker-entrypoint.sh" ]

WORKDIR "/doltdbs/$DOLTHUB_USER"
CMD [ "dolt", "sql-server", "-l=trace", "--host=0.0.0.0", "--port=3306"]
