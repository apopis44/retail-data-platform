FROM python:3.12.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY docker/kafka/requirements.txt /tmp/requirements.txt

RUN python -m pip install \
        --no-cache-dir \
        --requirement /tmp/requirements.txt \
    && rm /tmp/requirements.txt \
    && groupadd --system cdc \
    && useradd \
        --system \
        --gid cdc \
        --home-dir /app \
        cdc

COPY --chown=cdc:cdc kafka/ /app/kafka/

USER cdc

CMD ["python", "-m", "kafka.bootstrap"]