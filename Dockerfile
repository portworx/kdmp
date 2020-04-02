FROM ubuntu:latest
MAINTAINER Portworx Inc. <support@portworx.com>

WORKDIR /

COPY ./bin/kdmp /

ENTRYPOINT ["/kdmp"]
