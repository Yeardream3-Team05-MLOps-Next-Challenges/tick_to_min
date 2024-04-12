FROM python:3.10.13-slim

WORKDIR /usr/src/app

COPY . .

RUN python -m pip install --upgrade pip openjdk-11-jdk procps\
    && pip install --no-cache-dir -r requirements.txt

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64

WORKDIR ./myapp

ENV KAFA_URL=kafka_url
ENV TICK_TOPIC=tick_topic
ENV MIN_TOPIC=min_topic

CMD ["python3", "main.py"]