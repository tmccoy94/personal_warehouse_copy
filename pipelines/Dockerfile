FROM python:3.12-slim

# Install uv
RUN pip install uv

WORKDIR /opt/dagster/app

# Copy dependency files first (better caching)
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --frozen

# Copy source code
COPY src ./src

ENV PYTHONPATH=/opt/dagster/app/src
ENV PATH="/opt/dagster/app/.venv/bin:$PATH"