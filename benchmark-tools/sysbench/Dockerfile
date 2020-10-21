FROM python:3.8.6-slim-buster

# Get sysbench installed
RUN apt-get update -y && apt-get install -y curl
RUN curl -s https://packagecloud.io/install/repositories/akopytov/sysbench/script.deb.sh | bash
RUN apt update
RUN apt -y install sysbench

COPY ./benchmark.sh /benchmark.sh

ENTRYPOINT ["/benchmark.sh"]