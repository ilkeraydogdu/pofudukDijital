# Multi-stage Dockerfile for Marketing Data Platform

# Stage 1: Base Python image
FROM python:3.11-slim as base

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    postgresql-client \
    curl \
    wget \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Stage 2: Dependencies
FROM base as dependencies

WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Install Playwright browsers
RUN playwright install chromium && \
    playwright install-deps chromium

# Stage 3: Application
FROM dependencies as app

WORKDIR /app

# Copy application code
COPY src/ ./src/
COPY config/ ./config/
COPY airflow/ ./airflow/
COPY dbt/ ./dbt/
COPY scripts/ ./scripts/
COPY .env.example .env

# Create necessary directories
RUN mkdir -p /app/data /app/logs /app/.cache /app/exports

# Set permissions
RUN chmod +x scripts/*.sh 2>/dev/null || true

# Stage 4: API Service
FROM app as api

EXPOSE 8000

CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

# Stage 5: Airflow Scheduler
FROM app as airflow-scheduler

# Install Airflow
RUN pip install apache-airflow==2.8.0

# Set Airflow home
ENV AIRFLOW_HOME=/app/airflow

# Initialize Airflow database
RUN airflow db init && \
    airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

CMD ["airflow", "scheduler"]

# Stage 6: Airflow Webserver
FROM app as airflow-webserver

# Install Airflow
RUN pip install apache-airflow==2.8.0

ENV AIRFLOW_HOME=/app/airflow

EXPOSE 8080

CMD ["airflow", "webserver", "--port", "8080"]

# Stage 7: Worker (for background tasks)
FROM app as worker

CMD ["python", "-m", "celery", "-A", "src.workers.celery_app", "worker", "--loglevel=info"]

# Stage 8: Development environment
FROM app as development

# Install development dependencies
RUN pip install \
    ipython \
    jupyter \
    black \
    flake8 \
    mypy \
    pytest \
    pytest-cov \
    pytest-asyncio

EXPOSE 8000 8080 8888

CMD ["/bin/bash"]