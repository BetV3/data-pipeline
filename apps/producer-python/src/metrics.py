import os
from prometheus_client import Counter, Histogram, start_http_server

PRODUCER_SENT = Counter(
    "dpp_producer_events_sent_total",
    "Total events produced (produce calls issued).",
    ["topic"]
)

PRODUCER_DELIVERED = Counter(
    "dpp_producer_events_delivered_total",
    "Total events delivered successfully",
    ["topic"]
)

PRODUCER_DELIVERY_ERRORS = Counter(
    "dpp_producer_delivery_errors_total",
    "Total delivery errors",
    ["topic"]
)

PRODUCER_BUILD_SECONDS = Histogram(
    "dpp_producer_event_build_seconds",
    "Time to build an event payload"
)

def start_metrics_server() -> int:
    port = int(os.getenv("PRODUCER_METRICS_PORT", 9101))
    start_http_server(port)
    return port