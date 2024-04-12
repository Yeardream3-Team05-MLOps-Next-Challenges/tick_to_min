FROM python:3.10.13-slim

WORKDIR /usr/src/app

COPY . .

RUN ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64 \
    && python -m pip install --upgrade pip\
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64

WORKDIR ./myapp

ENV KAFA_URL=kafka_url
ENV TICK_TOPIC=tick_topic
ENV MIN_TOPIC=min_topic

CMD ["python3", "main.py"]