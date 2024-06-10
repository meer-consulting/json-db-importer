FROM python:3.12.4-slim-bookworm

RUN apt-get update \
  && apt-get install -y wget unzip libaio1 libaio-dev \
  && rm -rf /var/lib/apt/lists/*

RUN wget https://download.oracle.com/otn_software/linux/instantclient/2340000/instantclient-basic-linux.x64-23.4.0.24.05.zip \
  && unzip instantclient-basic-linux.x64-23.4.0.24.05.zip \
  && rm instantclient-basic-linux.x64-23.4.0.24.05.zip

ENV LD_LIBRARY_PATH /instantclient_23_4:$LD_LIBRARY_PATH

COPY ./requirements.txt /requirements.txt

RUN pip3 install --upgrade pip \
  && pip3 install -r /requirements.txt
