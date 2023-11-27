FROM apache/spark:latest

USER root

RUN apt-get update -yqq && \
    apt-get upgrade -yqq && \
    apt-get install -yqq wget git unzip

WORKDIR /app

RUN git clone "https://github.com/mukimmanan/segwise_assignment.git"

WORKDIR /app/segwise_assignment

COPY archive.zip .

RUN unzip "archive.zip"
RUN mv google-play-dataset-by-tapivedotcom.csv playstore.csv
