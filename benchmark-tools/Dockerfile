FROM python:3.7-slim-stretch

ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

RUN apt-get update
RUN apt-get install -yqq --no-install-recommends software-properties-common curl
RUN curl -s "https://packagecloud.io/install/repositories/akopytov/sysbench/script.deb.sh" | bash
RUN set -ex \
    && buildDeps=' \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        git \
    ' \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        apt-utils \
        awscli \
        build-essential \
        ca-certificates \
        cpanminus \
        curl \
        dumb-init \
        fonts-liberation \
        freetds-bin \
        gconf-service \
        libappindicator1 \
        libasound2 \
        libatk-bridge2.0-0 \
        libatk1.0-0 \
        libc6 \
        libcairo2 \
        libcups2 \
        libdbus-1-3 \
        libexpat1 \
        libfontconfig1 \
        libgcc1 \
        libgconf-2-4 \
        libgdk-pixbuf2.0-0 \
        libglib2.0-0 \
        libgtk-3-0 \
        libnspr4 \
        libnss3 \
        libpango-1.0-0 \
        libpangocairo-1.0-0 \
        libpq-dev \
        libstdc++6 \
        libx11-6 \
        libx11-xcb1 \
        libxcb1 \
        libxcomposite1 \
        libxcursor1 \
        libxdamage1 \
        libxext6 \
        libxfixes3 \
        libxi6 \
        libxrandr2 \
        libxrender1 \
        libxss1 \
        libxtst6 \
        locales \
        lsb-release \
        netcat \
        perl \
        rsync \
        unzip \
        wget \
        xdg-utils
RUN pip install doltpy
