FROM locustio/locust:2.20.0

COPY ./requirements.txt /requirements.txt

RUN pip3 install --upgrade pip \
  && pip3 install -r /requirements.txt
