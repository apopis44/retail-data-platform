FROM python:3.12.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home \
    DBT_PROJECT_DIR=/app/dbt \
    DBT_PROFILES_DIR=/app/dbt

WORKDIR /app/dagster

COPY docker/dagster/requirements.txt /tmp/requirements.txt

RUN apt-get update \
    && apt-get install \
        --yes \
        --no-install-recommends \
        git \
    && python -m pip install \
        --no-cache-dir \
        --requirement /tmp/requirements.txt \
    && python -m pip check \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/requirements.txt \
    && mkdir -p "${DAGSTER_HOME}"

COPY dagster/ /app/dagster/
COPY dbt/ /app/dbt/

EXPOSE 3000

CMD ["dagster", "dev", "--host", "0.0.0.0", "--port", "3000", "--module-name", "retail_orchestration.definitions"]
