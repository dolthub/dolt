FROM python:3.8.6-slim-buster

# Get sysbench installed
RUN apt update
RUN apt install -y curl
RUN curl -s https://packagecloud.io/install/repositories/akopytov/sysbench/script.deb.sh | bash
RUN apt -y install sysbench
RUN pip install SQLAlchemy mysql-connector-python

COPY ./benchmark.sh /benchmark.sh

ENTRYPOINT ["/benchmark.sh"]