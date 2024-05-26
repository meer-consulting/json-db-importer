FROM python:3.10-slim

RUN mkdir /wikiextractor
COPY ./requirements.txt /requirements.txt
RUN pip3 install --upgrade pip \
  && pip3 install -r /requirements.txt

WORKDIR /wikiextractor
