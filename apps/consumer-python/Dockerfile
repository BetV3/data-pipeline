FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install deps first for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY src ./src

# Defaults (override via env / args)
ENV KAFKA_BROKERS="kafka:29092" \
    KAFKA_TOPIC="events" \
    KAFKA_GROUP_ID="raw-events-writer" \
    POSTGRES_DSN="postgresql://platform:platform@postgres:5432/platform" \
    POLL_MS="1000" \
    CONSUMER_METRICS_PORT="9102"

EXPOSE 9102

CMD ["python", "src/consumer.py"]
