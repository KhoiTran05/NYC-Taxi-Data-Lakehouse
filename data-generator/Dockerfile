FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY data_generator.py .

RUN mkdir -p data

ENV PYTHONUNBUFFERED=1
ENV DATABASE_URL=postgresql://postgres:postgres@postgres:5432/taxi_db
ENV BATCH_SIZE=50
ENV INTERVAL_SECONDS=60

CMD ["python", "data_generator.py"]