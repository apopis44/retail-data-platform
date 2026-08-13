# Retail Data Platform

A production-style learning project that combines streaming ingestion with
scheduled warehouse transformations for retail analytics. PostgreSQL is the
operational source, Debezium and Kafka carry change data capture (CDC) events,
Flink lands those events in BigQuery, and Dagster orchestrates dbt models after
the Bronze layer.

## Project status

The source snapshot pipeline and PostgreSQL-to-Kafka CDC path are complete for
all four source tables. The next ingestion milestone is to use Flink to land the
raw CDC events in BigQuery Bronze.

| Layer | Status |
| --- | --- |
| Synthetic retail data generation | Complete |
| PostgreSQL source schema and snapshot load | Complete |
| Snapshot validation | Complete |
| PostgreSQL -> Debezium -> Kafka CDC for all source tables | Complete |
| BigQuery Bronze, Silver, and Gold datasets | Provisioned |
| Flink SQL Kafka -> BigQuery Bronze sinks | Next milestone |
| Dagster + dbt Bronze -> Silver -> Gold processing | Planned |

## Architecture

![Retail data platform workflow](docs/new_flow.svg)

Component ownership is intentionally separated:

- **PostgreSQL** remains the operational source of truth.
- **Debezium** performs the initial snapshot and then captures committed row
  changes from PostgreSQL WAL.
- **Kafka** stores the raw CDC stream in table-specific topics.
- **Flink SQL** continuously reads the four Kafka topics and routes their raw
  events into table-specific BigQuery Bronze tables.
- **BigQuery Bronze** stores the append-only raw CDC event history, including
  the Debezium envelope and Kafka ingestion metadata.
- **Dagster** schedules and observes the daily dbt transformation jobs.
- **dbt** incrementally resolves Bronze events into current relational state in
  Silver, runs data-quality tests, and builds Gold business aggregations.

## Processing workflow

The ingestion path is streaming from the source through Bronze:

```text
PostgreSQL -> Debezium -> Kafka -> Flink -> BigQuery Bronze
```

When a Debezium connector starts without stored offsets, it takes a consistent
snapshot of every included table and writes one read event per row to Kafka.
After the snapshot completes, the same connector continuously publishes
inserts, updates, and deletes from PostgreSQL WAL. This keeps related entities
such as customers, orders, and order items on the same ingestion path and avoids
waiting for a separate daily source extract.

The initial snapshot has been verified for all four source tables:

| Kafka topic | Snapshot records |
| --- | ---: |
| `retail.public.customers` | 100,000 |
| `retail.public.products` | 10,000 |
| `retail.public.orders` | 1,000,000 |
| `retail.public.order_items` | 2,000,000 |
| **Total** | **3,110,000** |

Debezium committed the snapshot offsets and is now continuously processing live
WAL changes. Snapshot reads use operation `r`; subsequent creates, updates, and
deletes use `c`, `u`, and `d`.

The scheduled batch boundary begins after Bronze:

```text
Dagster -> dbt: Bronze -> Silver -> Gold
```

dbt performs the stateful warehouse work: deduplication, applying the latest CDC
operation, relationship checks, joins, and aggregations. Dagster provides the
schedule, dependency orchestration, retries, observability, and test execution;
it does not extract PostgreSQL tables into Bronze.

## Source data model

The generated dataset represents four related retail entities:

| Table | Default rows | Description |
| --- | ---: | --- |
| `customers` | 100,000 | Customer identity and country |
| `products` | 10,000 | Product catalogue and pricing |
| `orders` | 1,000,000 | Customer orders and status |
| `order_items` | 2,000,000 | Products and quantities within orders |

PostgreSQL enforces primary keys, foreign keys, uniqueness, non-negative prices,
positive quantities, and indexes on the main relationship and date columns.

## Repository structure

```text
retail-data-platform/
├── dagster/                 # Dagster orchestration for dbt jobs
├── dbt/                     # Bronze -> Silver -> Gold models and tests
├── dev/data/                # Generated CSV data (not committed)
├── docker/kafka/            # CDC bootstrap image and pinned dependencies
├── docker/postgres/init/    # PostgreSQL source schema
├── docs/                    # Architecture and workflow diagrams
├── flink/jobs/              # Planned Flink jobs
├── flink-sql/               # Planned streaming SQL
├── kafka/                   # Idempotent PostgreSQL-to-Kafka CDC bootstrap
├── monitoring/              # Planned observability configuration
├── scripts/                 # Synthetic data generator
├── src/ingestion/           # Snapshot loading and validation
├── terraform/               # Planned cloud infrastructure
├── tests/                   # Planned automated tests
├── docker-compose.yml       # Local PostgreSQL, Kafka, Debezium, and Flink
└── requirements.txt         # Current Python dependencies
```

## Local setup

### Prerequisites

- Python 3.12
- Docker with Docker Compose
- Enough local storage for the generated dataset and container volumes

### 1. Create the Python environment

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### 2. Generate the source data

The ingestion pipeline reads from `dev/data` by default.

```bash
python scripts/generate_data.py --output dev/data
```

Generation is deterministic with the default random seed of `42`. Row counts,
the output directory, and the seed can be changed through the script's command
line options.

### 3. Start the local platform

```bash
docker compose up --build -d cdc-bootstrap
docker compose ps
```

The compose stack starts:

- PostgreSQL on `localhost:5432`
- Kafka on `localhost:9092`
- Debezium Connect on `localhost:8083`
- A one-shot `cdc-bootstrap` setup container
- Flink JobManager on `localhost:8081`
- Flink TaskManager

The bootstrap container exits successfully after it has reconciled the
PostgreSQL publication, created and validated the four Kafka topics, applied the
Debezium connector configuration, and confirmed that the connector task is
running. PostgreSQL, Kafka, and Debezium remain running after the bootstrap
exits.

### 4. Load and validate the PostgreSQL snapshot

```bash
python -m src.ingestion.pipeline
```

The snapshot load runs as one transaction:

1. Truncate the four source tables.
2. Bulk-load the CSV files with PostgreSQL `COPY`.
3. Validate the expected row counts.
4. Validate referential integrity between the tables.
5. Commit only after all validations succeed.

## CDC notes

PostgreSQL is configured with logical replication enabled. Kafka exposes an
internal listener for container-to-container communication and an external
listener for local development. Debezium Connect reads PostgreSQL changes and
publishes each captured table to its own Kafka topic.

The registered connector captures `customers`, `products`, `orders`, and
`order_items`, publishing each table to its own `retail.public.*` Kafka topic.
The initial snapshot completed for all four tables, Kafka Connect acknowledged
3,110,000 messages, and Debezium transitioned to continuous WAL streaming.

CDC infrastructure is reproducible through the root `kafka` Python package and
the one-shot `cdc-bootstrap` Compose service. The bootstrap uses PostgreSQL,
Kafka Admin, and Kafka Connect APIs to reconcile resources without deleting
topics, replication slots, or source data. Configuration and credentials are
injected through environment variables rather than committed connector files.

## Next milestone: Flink SQL to BigQuery Bronze

Each Kafka topic will be consumed continuously by Flink SQL and written to its
own append-only Bronze table:

| Kafka source topic | BigQuery Bronze target |
| --- | --- |
| `retail.public.customers` | `retail_bronze.customers_cdc` |
| `retail.public.products` | `retail_bronze.products_cdc` |
| `retail.public.orders` | `retail_bronze.orders_cdc` |
| `retail.public.order_items` | `retail_bronze.order_items_cdc` |

Bronze will preserve each complete Debezium JSON envelope as raw JSON text,
including `before`, `after`, `op`, source metadata, and transaction metadata.
Each row will also carry the Kafka key, topic, partition, offset, timestamp, and
a Flink ingestion timestamp. The Flink SQL pipeline is a stateless pass-through;
it does not window, join, aggregate, deduplicate, or resolve current state.

dbt will later parse and type the raw JSON, apply CDC operations, and build the
current relational state in Silver. Dimensional models and business facts will
be built in BigQuery Gold.

## Configuration

The ingestion and CDC bootstrap support these core environment variables:

| Variable | Default |
| --- | --- |
| `DATA_DIR` | `dev/data` |
| `POSTGRES_HOST` | `postgres` inside Compose |
| `POSTGRES_PORT` | `5432` |
| `POSTGRES_DB` | `retail` |
| `POSTGRES_USER` | `retail_user` |
| `POSTGRES_PASSWORD` | `retail_password` |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` |
| `KAFKA_TOPIC_PREFIX` | `retail` |
| `KAFKA_TOPIC_PARTITIONS` | `3` |
| `KAFKA_TOPIC_REPLICATION_FACTOR` | `1` |
| `DEBEZIUM_URL` | `http://debezium:8083` |
| `DEBEZIUM_CONNECTOR_NAME` | `postgres-retail-cdc` |
| `POSTGRES_PUBLICATION_NAME` | `retail_cdc` |
| `POSTGRES_REPLICATION_SLOT_NAME` | `retail_cdc_slot` |

The defaults are intended only for local development.

## Roadmap

- [x] Generate deterministic retail source data
- [x] Build the PostgreSQL snapshot ingestion pipeline
- [x] Add row-count and referential-integrity validation
- [x] Configure PostgreSQL, Kafka, and Debezium CDC infrastructure
- [x] Build an idempotent Python CDC bootstrap container
- [x] Create and validate four table-specific Kafka topics
- [x] Snapshot 3,110,000 records across all four source tables
- [x] Stream live PostgreSQL WAL changes through Debezium into Kafka
- [ ] Build four stateless Flink SQL Kafka -> BigQuery Bronze sinks
- [ ] Build incremental dbt Bronze -> Silver -> Gold models and tests
- [ ] Orchestrate dbt models, tests, and freshness checks with Dagster
- [ ] Add monitoring, automated tests, and Terraform infrastructure

## License

This project is licensed under the terms in [LICENSE](LICENSE).
