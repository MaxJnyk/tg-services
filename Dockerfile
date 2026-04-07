# Dockerfile
FROM python:3.12-slim

WORKDIR /app

RUN pip install uv

COPY pyproject.toml ./
RUN uv pip install --system -r pyproject.toml

COPY src/ ./src/
COPY migrations/ ./migrations/
COPY scripts/ ./scripts/

# Проверяем что критические импорты работают
RUN python -c "from src.core.config import settings; print(f'✅ Config: REDIS_URL={settings.REDIS_URL}')" && \
    python -c "from src.entrypoints.health import start_health_server; print('✅ Health server')" && \
    python -c "from src.infrastructure.telegram.bot_pool import BotPool; print('✅ BotPool')" && \
    python -c "from src.infrastructure.telegram.redis_circuit_breaker import RedisCircuitBreaker; print('✅ Circuit Breaker')" && \
    echo "=== All critical imports OK ==="

RUN useradd --create-home taduser
USER taduser

EXPOSE 8080

CMD ["taskiq", "worker", "src.entrypoints.broker:broker", "--workers", "2", "--fs-discover"]
