import os
from dataclasses import dataclass


CDC_TABLES = (
    "public.customers",
    "public.products",
    "public.orders",
    "public.order_items",
)

def required_env(name: str) -> str :
    value = os.getenv(name)

    if value is None or not value.strip():
        raise ValueError(
            f"Required environment variable is missing: {name}"
        )
    return value.strip()

@dataclass(frozen=True, slots=True)
class Settings:
    postgres_host: str
    postgres_port: int
    postgres_database: str
    postgres_user: str
    postgres_password: str

    kafka_bootstrap_servers: str

    debezium_url: str
    connector_name: str
    heartbeat_topic: str
    heartbeat_interval_ms: int
    publication_name: str
    replication_slot_name: str

    topic_prefix: str
    topic_partitions: int
    topic_replication_factor: int

    tables: tuple[str, ...]

    @property
    def topic_names(self) -> tuple[str, ...]:
        cdc_topics = tuple(
            f"{self.topic_prefix}.{table}"
            for table in self.tables
        )

        return cdc_topics + (self.heartbeat_topic,)

    @property
    def table_include_list(self) -> str:
        return ",".join(self.tables)

    @classmethod
    def from_env(cls) -> "Settings":
        heartbeat_interval_ms = int(
            required_env("DEBEZIUM_HEARTBEAT_INTERVAL_MS")
        )

        if heartbeat_interval_ms <= 0:
            raise ValueError(
                "DEBEZIUM_HEARTBEAT_INTERVAL_MS must be greater than zero"
            )
        return cls(
            postgres_host= required_env("POSTGRES_HOST"),
            postgres_port= int(os.getenv("POSTGRES_PORT", "5432")),
            postgres_database= required_env("POSTGRES_DB"),
            postgres_user= required_env("POSTGRES_USER"),
            postgres_password= required_env("POSTGRES_PASSWORD"),

            kafka_bootstrap_servers= required_env("KAFKA_BOOTSTRAP_SERVERS"),

            debezium_url= required_env("DEBEZIUM_URL").rstrip("/"),
            connector_name= os.getenv(
                "DEBEZIUM_CONNECTOR_NAME",
                "postgres-retail-cdc"
            ),

            heartbeat_topic=required_env(
                "DEBEZIUM_HEARTBEAT_TOPIC"
            ),
            heartbeat_interval_ms=heartbeat_interval_ms,

            publication_name= os.getenv(
                "POSTGRES_PUBLICATION_NAME",
                "retail_cdc"
            ),
            replication_slot_name= os.getenv(
                "POSTGRES_REPLICATION_SLOT_NAME",
                "retail_cdc_slot"
            ),

            topic_prefix= os.getenv(
                "KAFKA_TOPIC_PREFIX",
                "retail"
            ),
            topic_partitions= int(
                os.getenv("KAFKA_TOPIC_PARTITIONS", "3")
            ),
            topic_replication_factor= int(
                os.getenv("KAFKA_TOPIC_REPLICATION_FACTOR", "1")
            ),

            tables= CDC_TABLES,
        )



