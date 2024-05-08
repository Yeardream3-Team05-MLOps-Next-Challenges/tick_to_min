FROM python:3.10.13-slim

WORKDIR /usr/src/app

COPY . .

RUN python -m pip install --upgrade pip\
    && pip install --no-cache-dir -r requirements.txt 

WORKDIR ./myapp

ENV LOG_LEVEL=INFO
ENV SPARK_URL=spark_url
ENV KAFKA_URL=kafka_url
#ENV TICK_TOPIC=tick_topic
#ENV MIN_TOPIC=min_topic

CMD ["python3", "main.py"]