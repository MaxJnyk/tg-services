# Dockerfile
FROM python:3.12-slim

WORKDIR /app

RUN pip install uv

COPY pyproject.toml ./
RUN uv pip install --system -r pyproject.toml

COPY src/ ./src/
COPY migrations/ ./migrations/
COPY scripts/ ./scripts/

RUN useradd --create-home taduser
USER taduser

EXPOSE 8080

CMD ["taskiq", "worker", "src.entrypoints.broker:broker", "--workers", "2", "--fs-discover"]
