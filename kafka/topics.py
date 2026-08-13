import time
import logging
from typing import Any

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from kafka.config import Settings

LOGGER = logging.getLogger(__name__)

def topic_already_exists(error: KafkaException) -> bool:
    if not error.args:
        return False

    kafka_error = error.args[0]

    return (
        isinstance(kafka_error, KafkaError)
        and kafka_error.code() == KafkaError.TOPIC_ALREADY_EXISTS
    )

def validate_topic(
    topic: str,
    topic_metadata: Any,
    settings: Settings,
) -> None:
    if topic_metadata.error is not None:
        raise RuntimeError(
            f"Kafka returned an error for topic {topic}: "
            f"{topic_metadata.error}"
        )

    partition_count = len(topic_metadata.partitions)

    if partition_count != settings.topic_partitions:
        raise RuntimeError(
            f"Kafka topic {topic} has {partition_count} partitions; "
            f"expected {settings.topic_partitions}"
        )

    replication_factors = {
        len(partition.replicas)
        for partition in topic_metadata.partitions.values()
    }

    expected_replication_factor = {
        settings.topic_replication_factor
    }

    if replication_factors != expected_replication_factor:
        raise RuntimeError(
            f"Kafka topic {topic} has replication factors "
            f"{sorted(replication_factors)}; expected "
            f"{settings.topic_replication_factor}"
        )

def ensure_topics(settings: Settings) -> None:
    admin = AdminClient(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
        }
    )

    metadata = admin.list_topics(timeout=10)

    missing_topics = [
        topic
        for topic in settings.topic_names
        if topic not in metadata.topics
    ]

    existing_topics = set(settings.topic_names) - set(missing_topics)

    for topic in sorted(existing_topics):
        LOGGER.info(
            "Kafka topic %s is already present",
            topic,
        )

    if missing_topics:
        topic_requests = [
            NewTopic(
                topic,
                num_partitions=settings.topic_partitions,
                replication_factor=(
                    settings.topic_replication_factor
                ),
            )
            for topic in missing_topics
        ]

        futures = admin.create_topics(
            topic_requests,
            operation_timeout=10,
            request_timeout=10,
        )

        for topic, future in futures.items():
            try:
                future.result(timeout=10)

                LOGGER.info(
                    "Created Kafka topic %s",
                    topic,
                )
            except KafkaException as error:
                if topic_already_exists(error):
                    LOGGER.info(
                        "Kafka topic %s is already present",
                        topic,
                    )
                    continue
                raise RuntimeError(
                    f"Could not create Kafka topic: {topic}"
                ) from error

    deadline = time.monotonic() + 30

    while True:
        refreshed_metadata = admin.list_topics(timeout=10)

        missing_after_setup = [
            topic
            for topic in settings.topic_names
            if topic not in refreshed_metadata.topics
        ]

        if not missing_after_setup:
            break

        if time.monotonic() >= deadline:
            raise RuntimeError(
                "Kafka topics are still missing after setup: "
                + ", ".join(missing_after_setup)
            )

        LOGGER.info(
            "Waiting for Kafka topic metadata: %s",
            ", ".join(missing_after_setup),
        )

        time.sleep(1)

    for topic in settings.topic_names:
        topic_metadata = refreshed_metadata.topics.get(topic)

        if topic_metadata is None:
            raise RuntimeError(
                f"Kafka topic is still missing after setup: {topic}"
            )

        validate_topic(
            topic,
            topic_metadata,
            settings,
        )

        LOGGER.info(
            "Kafka topic %s is correctly configured",
            topic,
        )

    LOGGER.info(
        "All CDC Kafka topics are ready: %s",
        ", ".join(settings.topic_names),
    )
