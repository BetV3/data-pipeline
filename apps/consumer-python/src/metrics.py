import os
from prometheus_client import Counter, Histogram, start_http_server

CONSUMER_MESSAGES = Counter(
    "dpp_consumer_messages_total",
    "Total Kafka messages polled.",
    ["topic"],
)

CONSUMER_WRITTEN = Counter(
    "dpp_consumer_events_written_total",
    "Total events written to Postgres.",
    ["topic"],
)

CONSUMER_ERRORS = Counter(
    "dpp_consumer_processing_errors_total",
    "Total processing errors.",
    ["topic"],
)

DB_WRITE_SECONDS = Histogram(
    "dpp_consumer_db_write_seconds",
    "Time spent writing one event to Postgres (including commit).",
)

def start_metrics_server() -> int:
    port = int(os.getenv("CONSUMER_METRICS_PORT", "9102"))
    start_http_server(port)
    return port