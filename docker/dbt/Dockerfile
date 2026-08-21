FROM python:3.12.12-slim-bookworm

ENV  PYTHONDONTWRITEBYTECODE=1 \
     PYTHONUNBUFFERED=1 \
     PIP_DISABLE_PIP_VERSION_CHECK=1 \
     DBT_PROFILES_DIR=/app/dbt

WORKDIR /app/dbt

COPY docker/dbt/requirements.txt /tmp/requirements.txt

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
        /tmp/requirements.txt

ENTRYPOINT ["dbt"]

CMD ["--help"]
