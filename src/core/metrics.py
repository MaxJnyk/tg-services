"""Prometheus metrics для tad-worker.

Метрики:
- tad_posts_total — счётчик постов (label: status)
- tad_rotation_duration_seconds — гистограмма времени публикации
- tad_dlq_size — gauge размера DLQ
- tad_circuit_breaker_state — gauge состояния circuit breaker
- tad_rate_limiter_hits — счётчик rate limit срабатываний
- tad_telegram_errors_total — счётчик ошибок Telegram (label: error_type)
- tad_message_stats_collected — счётчик собранных статистик сообщений
- tad_billing_charges_total — счётчик billing операций

Использование:
    from src.core.metrics import posts_total, rotation_duration
    
    posts_total.labels(status="published").inc()
    with rotation_duration.time():
        await publish_post(...)
"""
from prometheus_client import Counter, Histogram, Gauge, Info, generate_latest


# === Posting метрики ===

posts_total = Counter(
    "tad_posts_total",
    "Total number of posts processed",
    ["status"]  # published, failed, already_processed, already_processing
)

rotation_duration = Histogram(
    "tad_rotation_duration_seconds",
    "Time spent publishing a post",
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]
)

# === DLQ метрики ===

dlq_size = Gauge(
    "tad_dlq_size",
    "Current number of messages in DLQ"
)

retry_exhausted_total = Counter(
    "tad_retry_exhausted_total",
    "Total number of tasks that exhausted all retries"
)

# === Circuit Breaker метрики ===

circuit_breaker_failures = Counter(
    "tad_circuit_breaker_failures_total",
    "Total number of circuit breaker failures recorded",
    ["token"]  # hashed token prefix
)

circuit_breaker_opens = Counter(
    "tad_circuit_breaker_opens_total",
    "Total number of times circuit opened",
    ["token"]
)

# === Rate Limiter метрики ===

rate_limiter_hits = Counter(
    "tad_rate_limiter_hits_total",
    "Total number of rate limit hits",
    ["type"]  # global, per_chat, retry_after
)

rate_limiter_waits = Histogram(
    "tad_rate_limiter_wait_seconds",
    "Time spent waiting due to rate limiting"
)

# === Telegram ошибки ===

telegram_errors_total = Counter(
    "tad_telegram_errors_total",
    "Total number of Telegram API errors",
    ["error_type"]  # retry_after, network, unauthorized, not_found, flood_wait, other
)

# === Stats метрики ===

message_stats_collected = Counter(
    "tad_message_stats_collected_total",
    "Total number of message stats collected"
)

channel_stats_collected = Counter(
    "tad_channel_stats_collected_total",
    "Total number of channel stats collected"
)

# === Billing метрики ===

billing_charges_total = Counter(
    "tad_billing_charges_total",
    "Total number of billing charge attempts",
    ["status"]  # success, already_charged, failed
)

billing_backend_latency = Histogram(
    "tad_billing_backend_latency_seconds",
    "Latency of billing backend API calls",
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0]
)

# === Worker информация ===

worker_info = Info(
    "tad_worker",
    "Worker metadata"
)


def update_worker_info(worker_id: str, concurrency: int, labels: list) -> None:
    """Обновить информацию о worker'е."""
    worker_info.info({
        "worker_id": worker_id,
        "concurrency": str(concurrency),
        "labels": ",".join(labels),
    })


def get_metrics() -> bytes:
    """Получить все метрики в формате Prometheus text.
    
    Returns:
        bytes с метриками для HTTP ответа.
    """
    return generate_latest()
