"""Prometheus metrics. Exposed at /metrics on API nodes and on a side port by workers."""

from prometheus_client import Counter, Gauge, Histogram

_LATENCY_BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)

HTTP_REQUESTS = Counter(
    "modstream_http_requests_total", "HTTP requests", ["method", "route", "status"]
)
HTTP_LATENCY = Histogram(
    "modstream_http_request_duration_seconds", "HTTP request latency", ["method", "route"],
    buckets=_LATENCY_BUCKETS,
)

ANALYSES = Counter("modstream_analyses_total", "Texts analyzed", ["source", "verdict"])

BATCH_SIZE = Histogram(
    "modstream_inference_batch_size", "Texts per model forward pass", ["batcher"],
    buckets=(1, 2, 4, 8, 16, 32, 64, 128, 256),
)
INFERENCE_SECONDS = Histogram(
    "modstream_inference_duration_seconds", "Model forward pass latency", ["batcher"],
    buckets=_LATENCY_BUCKETS,
)
BATCHER_DEPTH = Gauge("modstream_batcher_queue_depth", "Requests waiting for a batch", ["batcher"])
BATCHER_REJECTED = Counter("modstream_batcher_rejected_total", "Requests shed because the queue was full", ["batcher"])

STREAM_PROCESSED = Counter("modstream_stream_messages_processed_total", "Stream messages moderated by workers")
STREAM_DEAD_LETTERED = Counter("modstream_stream_dead_lettered_total", "Messages moved to the dead-letter queue")
STREAM_RECLAIMED = Counter("modstream_stream_reclaimed_total", "Messages reclaimed from crashed or stalled workers")
STREAM_E2E_SECONDS = Histogram(
    "modstream_stream_end_to_end_seconds", "Enqueue to verdict published", buckets=_LATENCY_BUCKETS,
)
STREAM_LAG = Gauge("modstream_stream_lag", "Messages waiting to be read by the consumer group")
STREAM_PENDING = Gauge("modstream_stream_pending", "Messages delivered to a worker but not yet acknowledged")

SUBSCRIBERS = Gauge("modstream_subscribers", "Connected live subscribers", ["transport"])
HUB_DROPPED = Counter("modstream_hub_dropped_events_total", "Events dropped for slow subscribers")
