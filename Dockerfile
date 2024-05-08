FROM python:3.10.13-slim

WORKDIR /usr/src/app

COPY . .

RUN apt-get update && apt-get install -y openjdk-17-jdk procps \
    && python -m pip install --upgrade pip\
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64

WORKDIR ./myapp

ENV LOG_LEVEL=INFO
ENV SPARK_URL=spark_url
ENV KAFA_URL=kafka_url
#ENV TICK_TOPIC=tick_topic
#ENV MIN_TOPIC=min_topic

CMD ["python3", "main.py"]