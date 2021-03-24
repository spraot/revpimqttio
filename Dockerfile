FROM python:3.8-alpine

ENV LANG C.UTF-8  
ENV LC_ALL C.UTF-8  

RUN apk add --no-cache build-base gcc make linux-headers

WORKDIR /app

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY mqtt_io.py ./

CMD [ "python", "mqtt_io.py", "/config.yml" ]
