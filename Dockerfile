FROM ubuntu:latest
MAINTAINER Portworx Inc. <support@portworx.com>

RUN apt-get update \
    && apt-get dist-upgrade -y \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
    && apt-get clean -y \
    && apt-get autoremove -y \
    && rm -rf /tmp/* /var/tmp/* \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /

COPY ./bin/kdmp /

ENTRYPOINT ["/kdmp"]
