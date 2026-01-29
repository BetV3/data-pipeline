FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONNUNBUFFERED=1
WORKDIR /app

# Install deps first for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Source
COPY src ./src

# Defaults (override via env / args)
ENV KAFKA_BROKERS="kafka:29092" \
    KAFKA_TOPIC="events" \
    EVENTS_PER_SEC="25" \
    RUN_SECONDS="10" \
    EVENT_TYPE="event_received" \
    EVENT_SOURCE="producer-python" \
    PRODUCER_METRICS_PORT="9101"

EXPOSE 9101

CMD ["python", "src/producer.py"]