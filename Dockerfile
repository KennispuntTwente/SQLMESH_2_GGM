# GGM Pipeline Docker Image
# Build: docker build -t ggm-pipeline .
# Run:   docker run --rm --network host ggm-pipeline --dest postgres
#
# This image provides an alternative to running with local Python/uv.
# It packages all dependencies and can run the full pipeline.

FROM python:3.12.10-slim

# Install system dependencies for database drivers
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        # For PostgreSQL
        libpq-dev \
        # For MSSQL (pyodbc)
        unixodbc-dev \
        gcc \
        g++ \
        wget \
        unzip \
        # General utilities
        curl \
    && if apt-cache show libaio1t64 >/dev/null 2>&1; then \
        apt-get install -y --no-install-recommends libaio1t64; \
      else \
        apt-get install -y --no-install-recommends libaio1; \
      fi \
    && rm -rf /var/lib/apt/lists/*

# Install Oracle Instant Client (required for oracledb thick mode)
RUN wget -q https://download.oracle.com/otn_software/linux/instantclient/2350000/instantclient-basic-linux.x64-23.5.0.24.07.zip \
    -O /tmp/instantclient.zip \
    && unzip /tmp/instantclient.zip -d /opt/oracle \
    && rm /tmp/instantclient.zip \
    && echo /opt/oracle/instantclient_23_5 > /etc/ld.so.conf.d/oracle-instantclient.conf \
    && ldconfig

ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_23_5

# Install uv for fast Python package management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Set working directory
WORKDIR /app

# Copy dependency files first (for better layer caching)
COPY pyproject.toml uv.lock ./

# Install locked dependencies into a virtual environment (does not install the project itself)
RUN uv sync --frozen --no-dev --no-install-project

# Copy the rest of the application
COPY . .

# Set Python path to include the project root
ENV PYTHONPATH=/app

# Default environment variables (can be overridden)
ENV GGM_DESTINATION=postgres
ENV GGM_GATEWAY=local

# Healthcheck - verify Python and key modules are available
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD uv run python -c "import dlt; import sqlmesh; print('OK')" || exit 1

# Default entrypoint runs the pipeline script
ENTRYPOINT ["uv", "run", "python", "scripts/pipeline.py"]

# Default command shows help
CMD ["--help"]
