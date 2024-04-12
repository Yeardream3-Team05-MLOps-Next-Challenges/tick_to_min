FROM spark:python3

WORKDIR /usr/src/app

COPY . .

RUN python -m pip install --upgrade pip\
    && pip install --no-cache-dir -r requirements.txt 
    
WORKDIR ./myapp

ENV KAFA_URL=kafka_url
ENV TICK_TOPIC=tick_topic
ENV MIN_TOPIC=min_topic

CMD ["python3", "main.py"]