FROM python:3.11-slim

# librdkafka needs gcc; snappy compression needs libsnappy-dev
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        libsnappy-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY 02_transactional_mirror_topic1_to_topic2.py .

# INSTANCE_ID / BOOTSTRAP_SERVERS / SCHEMA_REGISTRY_URL are all injected
# per-container by docker-compose; these are just safe defaults.
ENV INSTANCE_ID=0 \
    BOOTSTRAP_SERVERS=kafka-1:29092,kafka-2:29093,kafka-3:29094 \
    SCHEMA_REGISTRY_URL=http://schema-registry-1:8081 \
    PYTHONUNBUFFERED=1

CMD ["python", "-u", "02_transactional_mirror_topic1_to_topic2.py"]
