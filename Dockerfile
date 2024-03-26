FROM python:3.10.13-slim

WORKDIR /usr/src/app

COPY . .

RUN python -m pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

WORKDIR ./myapp

ENV KAFA_URL=kafka_url
ENV TICK_TOPIC=tick_topic
ENV MIN_TOPIC=MIN_TOPIC

CMD ["python3", "main.py"]