FROM python:3.11-slim

LABEL org.opencontainers.image.description="Real-Time Streaming Analytics — producer, consumer, and dashboard services"

RUN apt-get update && apt-get install -y --no-install-recommends \
    netcat-openbsd \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY startup.sh .
RUN chmod +x startup.sh

ENTRYPOINT ["./startup.sh"]
