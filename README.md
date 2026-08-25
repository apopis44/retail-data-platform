# Retail Data Platform

A production-style retail data platform built to practise reproducible change
data capture, low-latency stream processing, cloud data warehousing, and
scheduled analytics transformations.

PostgreSQL is the operational source. Debezium captures committed changes,
Kafka stores the CDC streams, Flink SQL parses and types the events, and
BigQuery persists both append-only Bronze history and five-minute real-time
order metrics. dbt now resolves the Bronze CDC history into tested Silver
current-state and order-history models, then builds optimized Gold dimensions,
facts, and a wide reporting table. A MetricFlow semantic layer exposes 19
centrally defined metrics over those Gold models. Dagster orchestrates the
complete dbt build-and-test workflow on a configurable UTC schedule and
records asset lineage, retries, logs, and run history.

Terraform adopts and protects the four existing BigQuery datasets, assigns
consistent metadata and Sandbox-compatible expiration settings, and provisions
separate least-privilege service accounts for Flink and dbt/Dagster. Terraform
runs from a pinned Docker image, so no host installation is required.

## Project status

| Capability | Status |
| --- | --- |
| Deterministic synthetic retail data | Complete |
| PostgreSQL schema, snapshot load, and validation | Complete |
| PostgreSQL publication and Debezium CDC connector | Complete |
| Four table-specific Kafka CDC topics | Complete |
| Debezium heartbeat topic | Complete |
| Flink SQL typed CDC processing | Complete |
| Four BigQuery Bronze CDC tables | Complete |
| Five-minute event-derived order metrics | Complete |
| Containerized dbt runtime and BigQuery sources | Complete |
| dbt staging views and Silver models | Complete |
| dbt data tests and reusable status macro | Complete |
| BigQuery Gold dimensional models | Complete |
| Wide order-item analytics table | Complete |
| dbt Semantic Layer and MetricFlow metrics | Complete |
| Dagster orchestration and scheduling | Complete |
| Terraform-managed BigQuery datasets | Complete |
| Least-privilege GCP service accounts and IAM | Complete |
| Production monitoring | Planned |

## Architecture

![Retail data platform architecture](docs/new_flow.svg)

The diagram predates the completed orchestration milestone: its dashed
Dagster path is now implemented by the asset job documented below.

The completed real-time ingestion path is shown in more detail below.

![Flink real-time processing workflow](docs/flink_realtime_workflow.svg)

The components have intentionally separate responsibilities:

- **PostgreSQL** is the operational source of truth.
- **Debezium** performs the initial snapshot and continuously reads committed
  changes from PostgreSQL WAL. It also publishes heartbeat events.
- **Kafka** durably stores four table-specific CDC streams and one heartbeat
  stream.
- **Flink SQL** parses the Debezium JSON, assigns SQL types, derives useful
  fields, attaches CDC and Kafka metadata, and writes low-latency datasets.
- **BigQuery Bronze** stores typed, append-only CDC event history. It does not
  store the Debezium `before` object.
- **BigQuery Realtime** stores five-minute event-time order metrics produced by
  Flink.
- **dbt** declares the Bronze sources, standardizes CDC events in staging
  views, resolves current relational state, preserves order-status history,
  and builds tested Silver and Gold analytics models.
- **MetricFlow** validates the semantic graph and dynamically compiles 19
  reusable business metrics into BigQuery SQL. Metric results are calculated
  on demand rather than stored in separate tables.
- **Dagster** schedules and observes the ordered dbt staging, Silver, and Gold
  builds, then validates the MetricFlow semantic layer. It streams dbt output
  into the run logs and stops downstream assets when an upstream layer fails.
- **Terraform** manages the existing BigQuery datasets, their lifecycle
  protections and metadata, and additive least-privilege IAM grants for the
  Flink and dbt/Dagster workload identities.

## Data flow

```text
PostgreSQL
    -> Debezium
    -> Kafka
    -> Flink SQL
    -> BigQuery Bronze + BigQuery Realtime

Dagster UTC schedule
    -> dbt_staging: build and test staging views
    -> dbt_silver: build and test Silver tables
    -> dbt_gold: build and test Gold models
    -> semantic_layer_validation: validate MetricFlow
    -> MetricFlow semantic queries on demand

Terraform control plane
    -> adopt and protect four existing BigQuery datasets
    -> provision Flink and dbt/Dagster service accounts
    -> grant additive dataset- and project-scoped IAM roles
```

### PostgreSQL to Kafka

The source contains four related entities:

| PostgreSQL table | Default rows | Kafka topic |
| --- | ---: | --- |
| `customers` | 100,000 | `retail.public.customers` |
| `products` | 10,000 | `retail.public.products` |
| `orders` | 1,000,000 | `retail.public.orders` |
| `order_items` | 2,000,000 | `retail.public.order_items` |
| **Total** | **3,110,000** | |

When no connector offsets exist, Debezium takes a consistent initial snapshot.
Snapshot events use operation `r`. After the snapshot, inserts, updates, and
deletes are published from PostgreSQL WAL with operations `c`, `u`, and `d`.

The idempotent `cdc-bootstrap` service reconciles the PostgreSQL publication,
the five application topics, and the Debezium connector through their
respective APIs. Re-running it does not delete source data, topics, offsets, or
the replication slot.

The fifth application topic, `retail.heartbeat`, is emitted every 30 seconds.
It advances event time for the order-metrics branch even when no new order
arrives.

### Kafka to BigQuery Bronze

The four CDC topics map to four physical BigQuery tables:

| Kafka topic | BigQuery destination |
| --- | --- |
| `retail.public.customers` | `retail_bronze.customers_cdc` |
| `retail.public.products` | `retail_bronze.products_cdc` |
| `retail.public.orders` | `retail_bronze.orders_cdc` |
| `retail.public.order_items` | `retail_bronze.order_items_cdc` |

Flink temporary tables are logical Kafka source and BigQuery sink definitions;
they do not hold intermediate data. Temporary views continuously transform
each incoming event by:

- reading the Debezium `after`, `source`, transaction, and operation fields;
- assigning SQL types to the business fields;
- preserving the record identifier on deletes by falling back to the Kafka
  key;
- deriving `line_amount` for order items;
- adding source, Debezium, Kafka, and Flink timestamps and metadata; and
- creating a deterministic `cdc_event_id` from topic, partition, and offset.

Bronze remains append-only. Updates and deletes are stored as new CDC event
rows; dbt resolves the latest operation for each business key in Silver.

### Event-derived metrics

The orders topic is parsed once. Flink branches the shared `orders_typed` view
into the Bronze sink and the metrics calculation instead of reading and parsing
the Kafka topic twice.

The metrics branch combines new-order events (`operation = 'c'`) with the
heartbeat stream and applies a five-minute event-time tumbling window. Results
are written to:

```text
retail_realtime.order_metrics_5m
```

Each row contains:

- window start and end;
- new-order count;
- gross revenue;
- average order value;
- unique customers;
- the heartbeat that advanced the watermark; and
- the metric generation timestamp.

Heartbeat-only windows are intentionally retained. They produce a zero order
count and zero gross revenue, with a null average order value, proving that the
stream is healthy even during periods without orders.

## dbt transformation layer

dbt runs as an ephemeral Docker Compose tool container. It authenticates to
BigQuery through the same read-only Application Default Credentials mount used
by Flink, uses BigQuery batch query priority, and writes the implemented models
to `retail_silver` and `retail_gold`. The disposable dbt service remains
available for development and targeted commands; scheduled full-pipeline runs
are executed by the containerized Dagster service.

The implemented lineage is:

```text
retail_bronze.*_cdc sources
    -> stg_customers_cdc       -> customers_current
    -> stg_products_cdc        -> products_current
    -> stg_orders_cdc          -> orders_current
                               -> order_status_history
    -> stg_order_items_cdc     -> order_items_current

customers_current             -> dim_customers
products_current              -> dim_products
orders_current                -> fct_orders
orders_current
  + order_items_current       -> fct_order_items
order_status_history          -> fct_order_status_transitions

dim_customers
  + dim_products
  + fct_orders
  + fct_order_items           -> wide_order_items_analytics

dim_customers
  + dim_products
  + fct_orders
  + fct_order_items
  + time_spine_daily          -> MetricFlow semantic queries
```

| dbt resource | Materialization | Purpose |
| --- | --- | --- |
| Four `retail_bronze` sources | External BigQuery tables | Register the Flink-owned Bronze tables and establish lineage |
| Four `stg_*_cdc` models | Views | Select business fields, expand CDC operation codes, identify deletes, and retain operational metadata |
| Four `*_current` models | Tables | Resolve the latest non-deleted record for every business key |
| `order_status_history` | Table | Preserve meaningful order-status changes and time spent in the previous state |
| Two `dim_*` models | Tables | Provide current customer and product descriptive attributes |
| Three `fct_*` models | Tables | Model orders, order items, and order-status transitions at explicit grains |
| `wide_order_items_analytics` | Table | Provide denormalized order-item reporting data with customer, order, and product context |
| `time_spine_daily` | Table | Provide the continuous daily calendar required for time-based MetricFlow queries |
| Four semantic models | Metadata | Declare entities, dimensions, relationships, measures, and default time dimensions |
| Nineteen metrics | Metadata | Centralize reusable order, value, status, customer, product, and rate calculations |

The current-state models rank CDC events deterministically by
`source_event_at`, `source_lsn`, and `kafka_offset`. They retain only the latest
event for each business key and exclude keys whose latest event is a delete.
The history model keeps initial states, actual status changes, and deletions
while removing repeated events that do not change an order's status.

The custom `normalize_order_status` macro trims and uppercases order statuses
and maps the source value `PENDING` to the canonical value `PLACED`. It is used
by both `orders_current` and `order_status_history`, keeping their business
rules consistent.

The project currently defines 94 data tests: 25 for staging, 26 for Silver,
and 43 for Gold. They validate event and business-key uniqueness, required
fields, accepted CDC operations and order statuses, the MetricFlow time spine,
and relationships between customers, orders, products, and order items.

Silver models are intentionally rebuilt as tables rather than implemented as
incremental `MERGE` models. The project uses BigQuery Sandbox without billing,
where DML-dependent incremental strategies and dbt snapshots cannot be
executed. This keeps the demonstrated workflow reproducible without enabling
paid features.

## BigQuery Gold analytics layer

Gold provides three complementary modeling styles for analytics and
reporting:

- `dim_customers` and `dim_products` provide reusable descriptive dimensions.
- `fct_orders`, `fct_order_items`, and `fct_order_status_transitions` preserve
  explicit business grains for trustworthy aggregation and lifecycle analysis.
- `wide_order_items_analytics` provides a denormalized reporting surface for
  analysts who need customer, order, line-item, and product attributes without
  repeatedly rebuilding the same joins.

The principal table optimizations are:

| Gold table | Partitioning | Clustering |
| --- | --- | --- |
| `fct_orders` | Daily `order_date` | `customer_id`, `order_status` |
| `fct_order_items` | Daily `order_date` | `customer_id`, `product_id`, `order_status` |
| `fct_order_status_transitions` | Integer range on `customer_id` | `status_started_at`, `new_order_status`, `transition_type` |
| `wide_order_items_analytics` | Daily `order_date` | `order_id`, `customer_id`, `product_id`, `order_status` |

### Wide-table grain and safe aggregation

`wide_order_items_analytics` has **one row per order item**. An order containing
multiple items therefore appears in multiple rows, with one different
`order_item_id` on each row. The table includes:

- customer identity, email, country, and customer timestamps;
- order date/time, current status, flags, total amount, and timestamps;
- order-item quantity, unit price, line amount, and timestamps; and
- product name, category, identifier, and timestamps.

Because the grain is one row per order item, `total_amount` repeats across all
items belonging to the same order. Do not sum `total_amount` directly from the
wide table for order-level value. Use `fct_orders`, the
`gross_order_value`/`non_cancelled_order_value` semantic metrics, or deduplicate
by `order_id` first. `line_amount` is additive at the wide table's grain and can
be safely summed for product and line-item analysis.

## Semantic and metrics layer

Four Gold models participate in the semantic graph:

- `dim_customers` declares the primary `customer` entity;
- `dim_products` declares the primary `product` entity;
- `fct_orders` declares the primary `retail_order` entity and a foreign
  `customer` entity; and
- `fct_order_items` declares the primary `order_item` entity and foreign
  `retail_order`, `customer`, and `product` entities.

MetricFlow uses these entity relationships to join facts and dimensions when a
metric is grouped by attributes such as customer country or product category.
The semantic layer currently provides 17 simple metrics and two ratio metrics:

| Area | Metrics |
| --- | --- |
| Orders and value | `order_count`, `distinct_customer_count`, `gross_order_value`, `non_cancelled_order_value`, `average_non_cancelled_order_value` |
| Current order status | `processing_order_count`, `processing_order_value`, `shipped_order_count`, `shipped_order_value`, `delivered_order_count`, `cancelled_order_count` |
| Products and line items | `line_item_count`, `non_cancelled_units`, `delivered_units`, `product_order_value`, `product_order_count`, `product_customer_count` |
| Rates | `cancellation_rate`, `delivery_rate` |

Metrics are definitions, not additional BigQuery tables. MetricFlow compiles a
metric request into SQL and calculates the result from the Gold models when the
query is executed. The daily `time_spine_daily` model supplies continuous dates
for time-based metric queries.

Ratio metrics use the conventional decimal representation. For example,
`cancellation_rate = 0.0100` means `1.00%`, and `delivery_rate = 0.0900`
means `9.00%`. Presentation tools should multiply these values by 100 and add
the percent sign.

## Dagster orchestration layer

Dagster models the batch transformation workflow as four ordered assets:

```text
dbt_staging
    -> dbt_silver
        -> dbt_gold
            -> semantic_layer_validation
```

| Dagster asset | Command | Responsibility |
| --- | --- | --- |
| `dbt_staging` | `dbt build --select path:models/staging` | Build the four staging views and execute their tests |
| `dbt_silver` | `dbt build --select path:models/silver` | Rebuild and test current-state and order-history tables |
| `dbt_gold` | `dbt build --select path:models/gold` | Rebuild and test dimensions, facts, the wide table, and time spine |
| `semantic_layer_validation` | `mf validate-configs` | Validate semantic models, entities, dimensions, measures, and metrics against BigQuery |

The assets are selected by the `dbt_batch_pipeline` job and launched by
`dbt_batch_pipeline_schedule`. The schedule reads `DAGSTER_DBT_CRON`, uses UTC,
and is enabled when the code location loads. The tracked configuration uses a
ten-minute test schedule; the intended production-style cadence is every 12
hours:

```ini
# Testing
DAGSTER_DBT_CRON="*/10 * * * *"

# Every 12 hours at 00:00 and 12:00 UTC
DAGSTER_DBT_CRON="0 */12 * * *"
```

Each asset retries once after 60 seconds. The shared command runner avoids
shell interpretation, streams combined dbt output into Dagster logs, and
raises structured Dagster failures with the command, working directory, and
exit code. A failed layer prevents every downstream asset from running.

The single local Dagster container includes the scheduler daemon and web UI.
It mounts the dbt project, mounts Google Application Default Credentials
read-only, and persists run metadata in the `dagster_home` named volume. This
provides scheduled execution, layer-level lineage, run history, logs, retries,
and failure visibility without changing the pinned dbt Core 1.12 runtime.

The workflow was verified with live PostgreSQL status updates. Debezium and
Flink appended the `u` events to Bronze, and subsequent scheduled Dagster runs
updated Silver current state, preserved the status transitions, rebuilt the
Gold current-state and wide models, and validated the semantic layer.

## Delivery and recovery

All five outputs run in one long-lived Flink SQL Statement Set:

- four BigQuery Bronze sinks; and
- one BigQuery real-time metrics sink.

The BigQuery sinks use the connector's `exactly-once` delivery guarantee.
Flink checkpoints every 15 seconds and retains checkpoint state on
cancellation. Kafka offsets and BigQuery writes are committed with successful
checkpoints, so records that were not committed before a failure can be safely
replayed.

The four business sources start from their Kafka consumer-group offsets. The
heartbeat source starts at the latest offset because historical heartbeats are
not business data and do not need to be replayed.

## Infrastructure as Code and IAM

Terraform runs as an ephemeral Docker Compose tool using the pinned
`hashicorp/terraform:1.15.9` image and Google provider `7.45.0`. The tool mounts
only the `terraform` directory and the existing Application Default
Credentials file. The credential is read-only and remains outside Git.

The configuration adopts the four existing BigQuery datasets through
declarative import blocks:

- `retail_bronze`
- `retail_realtime`
- `retail_silver`
- `retail_gold`

One reusable `for_each` resource manages their US location, 60-day Sandbox
table and partition expiration, friendly names, descriptions, and consistent
`data_layer`, `environment`, and `managed_by` labels. Both Terraform lifecycle
protection and provider deletion protection prevent accidental dataset
destruction. Existing dataset access entries are preserved, and all new IAM
permissions use additive member resources rather than authoritative policies.

Terraform provisions two user-managed service accounts without creating
long-lived JSON keys:

| Workload identity | Scope | Role | Purpose |
| --- | --- | --- | --- |
| `retail-flink-writer` | `retail_bronze` | BigQuery Data Editor | Create and write typed CDC tables |
| `retail-flink-writer` | `retail_realtime` | BigQuery Data Editor | Create and write event-derived metric tables |
| `retail-dbt-transformer` | `retail_bronze` | BigQuery Data Viewer | Read immutable Bronze source events |
| `retail-dbt-transformer` | `retail_silver` | BigQuery Data Editor | Build and test Silver models |
| `retail-dbt-transformer` | `retail_gold` | BigQuery Data Editor | Build, test, and query Gold models |
| `retail-dbt-transformer` | GCP project | BigQuery Job User | Create BigQuery query jobs |

Dagster invokes dbt and therefore shares the dbt transformation identity in a
deployed environment. The local Docker environment continues to use the
developer's mounted ADC credential; the service accounts model the
production-style permission boundary without placing private keys in the
repository or Terraform state.

Terraform currently uses local state because the project runs without a
billing account. `terraform.tfstate`, backup state, downloaded providers,
variable files, and saved plans are ignored by Git. Preserve the local state
file and never commit or share it. A shared production deployment should move
state to an encrypted remote backend with locking.

## Repository structure

```text
retail-data-platform/
├── dagster/                     # Dagster assets, batch job, and schedule
├── dbt/                         # Staging, Silver, Gold, tests, macros, metrics
├── dev/data/                    # Generated CSV files; ignored by Git
├── docker/
│   ├── dagster/                 # Reproducible Dagster + dbt image
│   ├── dbt/                     # Reproducible dbt Core + BigQuery image
│   ├── flink/                   # Flink image, runtime config, dependencies
│   ├── kafka/                   # CDC bootstrap image and Python requirements
│   └── postgres/init/           # PostgreSQL source schema
├── docs/                        # Architecture and workflow diagrams
├── flink/jobs/                  # Flink pipeline submission script
├── flink-sql/                   # Sources, views, sinks, and Statement Set
├── kafka/                       # Idempotent CDC bootstrap package
├── monitoring/                  # Planned production monitoring configuration
├── scripts/                     # Synthetic data generator
├── src/ingestion/               # Snapshot load and validation
├── terraform/                   # BigQuery datasets, service accounts, and IAM
├── tests/                       # Automated test area
├── docker-compose.yml           # Local data-platform services
└── requirements.txt             # Local snapshot-ingestion dependency
```

## Technology versions

| Component | Version |
| --- | --- |
| Python | 3.12 |
| PostgreSQL | 16 |
| Apache Kafka | 4.0.0 |
| Debezium Connect | 3.6.0.Final |
| Apache Flink | 1.20.5, Scala 2.12, Java 17 |
| dbt Core | 1.12.2 |
| dbt BigQuery adapter | 1.12.0 |
| dbt MetricFlow | 0.14.0 |
| Dagster and Dagster webserver | 1.13.18 |
| Terraform CLI | 1.15.9 |
| Terraform Google provider | 7.45.0 |

Flink 1.20.5 is used because it includes fixes required by this SQL Statement
Set and connector combination.

## Local setup

### Prerequisites

- Python 3.12
- Docker with Docker Compose
- Google Cloud CLI
- A GCP project with BigQuery enabled
- BigQuery datasets named `retail_bronze`, `retail_realtime`,
  `retail_silver`, and `retail_gold`
- Permission to manage those datasets, create service accounts, and add IAM
  members in the selected GCP project

Terraform itself does not need to be installed on the host. Docker Compose
uses the pinned official image.

### 1. Configure the environment

Copy the safe template and fill in the machine-specific values:

```bash
cp .env.examples .env
```

At minimum, replace the PostgreSQL password, GCP project ID, and absolute path
to the local Application Default Credentials file. The real `.env` file is
ignored by Git.

Create local Application Default Credentials if they do not already exist:

```bash
gcloud auth application-default login
```

### 2. Adopt and protect the GCP infrastructure

Validate Compose, initialize the pinned provider, and check the Terraform
configuration:

```bash
docker compose config --quiet
docker compose run --rm terraform init
docker compose run --rm terraform fmt -check
docker compose run --rm terraform validate
```

The checked-in import blocks adopt the four datasets that already exist in the
project. Save and inspect the exact plan before applying it:

```bash
docker compose run --rm terraform plan -out=plan.tfplan
docker compose run --rm terraform show -no-color plan.tfplan
docker compose run --rm terraform apply plan.tfplan
rm -f -- terraform/plan.tfplan
```

The initial apply imports the datasets, applies safe metadata and deletion
protection, creates the two keyless service accounts, and adds six
least-privilege IAM memberships. It does not recreate datasets or tables.
Never apply a plan that unexpectedly replaces or destroys a dataset.

Confirm that the real infrastructure matches the configuration and display
the managed datasets:

```bash
docker compose run --rm terraform plan -detailed-exitcode
docker compose run --rm terraform output bigquery_datasets
```

An exit code of `0` means Terraform detected no drift. Exit code `2` means the
plan contains changes that must be reviewed; any other nonzero code is an
error.

### 3. Create the Python environment

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### 4. Generate the source data

```bash
python scripts/generate_data.py --output dev/data
```

Generation is deterministic with the default random seed of `42`.

### 5. Start PostgreSQL and load the snapshot

```bash
docker compose up -d postgres
set -a
source .env
set +a
POSTGRES_HOST=localhost python -m src.ingestion.pipeline
```

The snapshot pipeline truncates and loads the four source tables inside one
transaction, validates row counts and referential integrity, and commits only
after every validation succeeds. Docker Compose reads `.env` automatically;
the shell export above makes the same values available to the local Python
process while overriding the container-only hostname.

### 6. Start CDC and Flink

```bash
docker compose up --build -d \
  cdc-bootstrap \
  flink-jobmanager \
  flink-taskmanager
```

Confirm the one-shot bootstrap completed successfully:

```bash
docker compose logs cdc-bootstrap
```

The persistent services remain running after `cdc-bootstrap` exits with code
zero.

### 7. Submit the Flink SQL pipeline

```bash
docker compose exec flink-jobmanager \
  /opt/flink/jobs/submit_realtime_pipeline.sh
```

The script renders the SQL with environment values, submits one detached
Statement Set, and prints the Flink Job ID. The job remains running after the
SQL client exits.

Open the Flink dashboard at [http://localhost:8081](http://localhost:8081) and
confirm that the job and all five sink branches are running.

### 8. Build and validate the dbt Silver layer

Build the reproducible dbt image and verify its configuration and BigQuery
connection:

```bash
docker compose build dbt
docker compose run --rm dbt debug
docker compose run --rm dbt ls --resource-type source
```

Build and test the staging views first, followed by the Silver tables:

```bash
docker compose run --rm dbt run --select path:models/staging
docker compose run --rm dbt test --select path:models/staging

docker compose run --rm dbt run --select path:models/silver
docker compose run --rm dbt test --select path:models/silver
```

These commands create four staging views and five Silver tables in
`retail_silver`, then execute the model-level data-quality and relationship
tests. Each command uses a disposable container and exits when dbt finishes.

### 9. Build Gold and validate the semantic layer

Build and test the Gold dimensions, facts, wide table, and daily time spine:

```bash
docker compose run --rm dbt run --select path:models/gold
docker compose run --rm dbt test --select path:models/gold
```

Parse the semantic definitions and confirm that dbt discovers four semantic
models and 19 metrics:

```bash
docker compose run --rm dbt parse --no-partial-parse
docker compose run --rm dbt ls --resource-type semantic_model
docker compose run --rm dbt ls --resource-type metric
```

Validate the semantic graph and its physical BigQuery entities, dimensions,
measures, and metrics:

```bash
docker compose run --rm --entrypoint mf dbt validate-configs
```

### 10. Query metrics from the CLI

List available metrics or inspect the dimensions available to a metric:

```bash
docker compose run --rm --entrypoint mf dbt list metrics

docker compose run --rm --entrypoint mf dbt list dimensions \
  --metrics order_count
```

Use smaller metric groups and `--quiet` to keep the CLI tables readable.
Display the order overview:

```bash
docker compose run --rm --entrypoint mf dbt query \
  --metrics order_count,gross_order_value,non_cancelled_order_value,average_non_cancelled_order_value \
  --decimals 2 \
  --quiet
```

Display current processing and shipped metrics:

```bash
docker compose run --rm --entrypoint mf dbt query \
  --metrics processing_order_count,processing_order_value,shipped_order_count,shipped_order_value \
  --decimals 2 \
  --quiet
```

Display cancellation and delivery rates:

```bash
docker compose run --rm --entrypoint mf dbt query \
  --metrics cancellation_rate,delivery_rate \
  --decimals 4 \
  --quiet
```

MetricFlow prints ratios as decimals: `0.0100` represents `1.00%`, while
`0.0900` represents `9.00%`.

### 11. Start the scheduled Dagster pipeline

Build the pinned Dagster image and validate the code location:

```bash
docker compose build dagster

docker compose run --rm --no-deps --entrypoint dagster dagster \
  definitions validate \
  --module-name retail_orchestration.definitions
```

Start the scheduler daemon and web UI:

```bash
docker compose up -d dagster
docker compose ps dagster
```

Open [http://localhost:3000](http://localhost:3000). The Assets view shows the
four-layer dependency graph, the Jobs view exposes `dbt_batch_pipeline`, and
the schedule view shows `dbt_batch_pipeline_schedule` and its next UTC tick.
Scheduled runs stream dbt and MetricFlow logs into the corresponding asset
steps.

After changing `DAGSTER_DBT_CRON`, recreate only the Dagster service so the new
container receives the updated environment value:

```bash
docker compose config --quiet
docker compose up -d --force-recreate dagster
```

The `dagster_home` volume preserves run history across recreation.

## Configuration reference

| Variable | Purpose |
| --- | --- |
| `POSTGRES_HOST` | PostgreSQL host used inside Compose |
| `POSTGRES_PORT` | PostgreSQL port |
| `POSTGRES_DB` | Source database name |
| `POSTGRES_USER` | Source database user |
| `POSTGRES_PASSWORD` | Source database password |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka internal listener |
| `KAFKA_TOPIC_PREFIX` | Prefix for table-specific CDC topics |
| `KAFKA_TOPIC_PARTITIONS` | Partition count for application topics |
| `KAFKA_TOPIC_REPLICATION_FACTOR` | Replication factor for application topics |
| `DEBEZIUM_URL` | Kafka Connect REST endpoint |
| `DEBEZIUM_CONNECTOR_NAME` | PostgreSQL connector name |
| `DEBEZIUM_HEARTBEAT_TOPIC` | Heartbeat topic consumed by Flink |
| `DEBEZIUM_HEARTBEAT_INTERVAL_MS` | Debezium heartbeat interval |
| `POSTGRES_PUBLICATION_NAME` | Logical-replication publication |
| `POSTGRES_REPLICATION_SLOT_NAME` | Debezium replication slot |
| `GOOGLE_CLOUD_PROJECT` | BigQuery project ID |
| `GOOGLE_ADC_PATH` | Host path to the ADC JSON file mounted read-only |
| `DAGSTER_DBT_CRON` | UTC cron expression for the scheduled dbt batch pipeline |

The Terraform Compose service maps `GOOGLE_CLOUD_PROJECT` to the Terraform
input `project_id` through `TF_VAR_project_id`. The BigQuery location defaults
to `US` in `terraform/variables.tf`.

## Roadmap

- [x] Generate deterministic retail source data
- [x] Load and validate the PostgreSQL snapshot
- [x] Build reproducible PostgreSQL-to-Kafka CDC infrastructure
- [x] Snapshot 3,110,000 records across four source tables
- [x] Stream live WAL changes into four Kafka topics
- [x] Add Debezium heartbeats for event-time progress
- [x] Parse and type all four CDC streams with Flink SQL
- [x] Load four append-only BigQuery Bronze tables
- [x] Produce heartbeat-driven five-minute order metrics
- [x] Configure checkpoint-backed exactly-once BigQuery delivery
- [x] Containerize dbt and declare the four BigQuery Bronze sources
- [x] Build and test four CDC staging views
- [x] Build and test four current-state Silver tables
- [x] Preserve order-status history and centralize status normalization
- [x] Build and test dimensional fact models in BigQuery Gold
- [x] Build a partitioned and clustered wide order-item reporting table
- [x] Define and validate four semantic models and 19 MetricFlow metrics
- [x] Add a tested daily time spine for time-based metric queries
- [x] Orchestrate dbt builds, tests, and semantic validation with Dagster
- [x] Adopt and protect four BigQuery datasets with Terraform
- [x] Provision keyless workload identities and least-privilege additive IAM
- [ ] Add production monitoring

## License

This project is licensed under the terms in [LICENSE](LICENSE).
