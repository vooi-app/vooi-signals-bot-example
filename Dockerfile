FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency files first for better layer caching
COPY pyproject.toml ./

# Install dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -e .

# Copy application code
COPY bot/ ./bot/
COPY alembic/ ./alembic/
COPY alembic.ini ./
COPY fixtures/ ./fixtures/

# Create logs directory
RUN mkdir -p /app/logs/vooi-errors-archive

# Create non-root user for security
RUN useradd -m -u 1000 botuser && chown -R botuser:botuser /app
USER botuser

# QUALITY-03: Healthcheck verifies DB connectivity, not just import
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD python -m bot healthcheck || exit 1

CMD ["python", "-m", "bot", "run"]
