import logging

from kafka.config import Settings
from kafka.debezium import (
    ensure_connector,
    wait_until_connector_is_running,
)
from kafka.postgres import ensure_publication
from kafka.topics import ensure_topics

LOGGER = logging.getLogger(__name__)

def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s "
            "%(levelname)s "
            "%(name)s - "
            "%(message)s"
        ),
    )

def main() -> int:
    configure_logging()

    try:
        LOGGER.info("Starting CDC bootstrap")

        settings = Settings.from_env()

        LOGGER.info(
            "Preparing PostgreSQL publication %s",
            settings.publication_name
        )
        ensure_publication(settings)

        LOGGER.info("Preparing Kafka CDC topics")
        ensure_topics(settings)

        LOGGER.info(
            "Applying Debezium connector %s",
            settings.connector_name
        )
        ensure_connector(settings)

        LOGGER.info(
            "Waiting for Debezium connector to start"
        )
        wait_until_connector_is_running(settings)

    except Exception:
        LOGGER.exception("CDC bootstrap failed")
        return 1

    LOGGER.info(
        "CDC bootstrap completed successfully for %s topics",
        len(settings.topic_names)
    )

    return 0

if __name__ == "__main__":
    raise SystemExit(main())