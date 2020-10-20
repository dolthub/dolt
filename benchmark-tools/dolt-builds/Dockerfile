FROM ubuntu:18.04

COPY ./dolt /usr/local/bin/dolt
COPY ./start_dolt_sql_server.sh /start_dolt_sql_server.sh

ENTRYPOINT [ "/start_dolt_sql_server.sh"]
