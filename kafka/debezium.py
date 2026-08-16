import json
import logging
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from kafka.config import Settings

LOGGER = logging.getLogger(__name__)

REQUEST_TIMEOUT_SECONDS = 10
CONNECTOR_START_TIMEOUT_SECONDS = 120
POLL_INTERVAL_SECONDS = 2

class ConnectRequestError(RuntimeError):
    """Raised when Kafka Connect cannot complete a REST request."""

def build_connector_config(
    settings: Settings,
) -> dict[str, str]:
    return {
        "connector.class": (
            "io.debezium.connector.postgresql.PostgresConnector"
        ),
        "tasks.max": "1",

        "database.hostname": settings.postgres_host,
        "database.port": str(settings.postgres_port),
        "database.dbname": settings.postgres_database,
        "database.user": settings.postgres_user,
        "database.password": settings.postgres_password,

        "topic.prefix": settings.topic_prefix,

        "topic.heartbeat.name": settings.heartbeat_topic,
        "heartbeat.interval.ms": str(
            settings.heartbeat_interval_ms
        ),

        "plugin.name": "pgoutput",
        "publication.name": settings.publication_name,
        "publication.autocreate.mode": "disabled",
        "slot.name": settings.replication_slot_name,
        "slot.drop.on.stop": "false",

        "table.include.list": settings.table_include_list,
        "snapshot.mode": "initial",

        "decimal.handling.mode": "string",
        "tombstones.on.delete": "false",

        "key.converter": (
            "org.apache.kafka.connect.json.JsonConverter"
        ),
        "key.converter.schemas.enable": "false",
        "value.converter": (
            "org.apache.kafka.connect.json.JsonConverter"
        ),
        "value.converter.schemas.enable": "false",
    }

def request_json(
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    request_body = None

    if payload is not None:
        request_body = json.dumps(payload).encode("utf-8")

    request = Request(
        url=url,
        data=request_body,
        method=method,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
    )

    try:
        with urlopen(
            request,
            timeout=REQUEST_TIMEOUT_SECONDS,
        ) as response:
            response_body = response.read()

    except HTTPError as error:
        error_body = error.read().decode(
            "utf-8",
            errors="replace",
        )

        raise ConnectRequestError(
            f"Kafka Connect returned HTTP {error.code} "
            f"for {method} {url}: {error_body}"
        ) from error

    except (URLError, TimeoutError) as error:
        raise ConnectRequestError(
            f"Kafka Connect request failed "
            f"for {method} {url}: {error}"
        ) from error

    if not response_body:
        return {}

    try:
        result = json.loads(response_body)
    except json.JSONDecodeError as error:
        raise ConnectRequestError(
            f"Kafka Connect returned invalid JSON "
            f"for {method} {url}"
        ) from error

    if not isinstance(result, dict):
        raise ConnectRequestError(
            f"Kafka Connect returned an unexpected response "
            f"for {method} {url}"
        )

    return result

def ensure_connector(settings: Settings) -> None:
    connector_name = quote(
        settings.connector_name,
        safe="",
    )

    connector_url = (
        f"{settings.debezium_url}/connectors/"
        f"{connector_name}/config"
    )

    request_json(
        method="PUT",
        url=connector_url,
        payload=build_connector_config(settings),
    )

    LOGGER.info(
        "applied Debezium connector %s",
        settings.connector_name,
    )

def wait_until_connector_is_running(
    settings: Settings,
) -> None:
    connector_name = quote(
        settings.connector_name,
        safe="",
    )

    status_url = (
        f"{settings.debezium_url}/connectors/"
        f"{connector_name}/status"
    )

    deadline = (
        time.monotonic()
        + CONNECTOR_START_TIMEOUT_SECONDS
    )

    last_status = "status not available"

    while time.monotonic() < deadline:
        try:
            status = request_json(
                method="GET",
                url=status_url,
            )
        except ConnectRequestError as error:
            last_status = str(error)
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        connector = status.get("connector", {})
        tasks = status.get("tasks", [])

        connector_state = connector.get(
            "state",
            "UNKNOWN",
        )

        task_status = [
            task.get("state", "UNKNOWN")
            for task in tasks
        ]

        last_status = (
            f"connector={connector_state}, "
            f"tasks={task_status or ['not-created']}"
        )

        if connector_state == "FAILED":
            connector_trace = connector.get(
                "trace",
                "connector failed without an error trace",
            )

            raise RuntimeError(
                f"Debezium connector "
                f"{settings.connector_name} failed:\n"
                f"{connector_trace}"
            )

        failed_tasks = [
            task
            for task in tasks
            if task.get('state') == "FAILED"
        ]

        if failed_tasks:
            task_traces = "\n".join(
                task.get(
                    "trace",
                    "Task failed without an error trace",
                )
                for task in failed_tasks
            )

            raise RuntimeError(
                f"Debezium connector "
                f"{settings.connector_name} has failed tasks: \n"
                f"{task_traces}"
            )

        connector_is_running = (
            connector_state == "RUNNING"
        )

        tasks_are_running = (
            bool(tasks)
            and all(
                state == "RUNNING"
                for state in task_status
            )
        )

        if connector_is_running and tasks_are_running:
            LOGGER.info(
                "Debezium connector %s and its task are running",
                settings.connector_name,
            )
            return

        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError(
        f"Debezium connector {settings.connector_name} "
        f"did not start within "
        f"{CONNECTOR_START_TIMEOUT_SECONDS} seconds "
        f"({last_status})"
    )
