# Debezium PostgreSQL CDC

The existing connector name, publication name, and replication slot are kept so
that the connector can reuse its recorded Kafka offsets and PostgreSQL WAL
position. Despite the legacy `orders` name, the connector captures all four
retail source tables.

The signaling table enables incremental snapshots when a table is added after
the connector's initial snapshot has already completed. This avoids replaying
the existing `orders` snapshot.

## PostgreSQL setup

Apply the publication and signaling-table configuration after the source tables
exist:

```bash
docker exec -i retail-postgres \
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  < docker/debezium/postgres/setup_cdc.sql
```

## Connector registration

The connector template contains environment-variable placeholders so database
credentials are not committed. Render it before sending it to Kafka Connect:

```bash
envsubst < docker/debezium/connectors/postgres-retail-cdc.json \
  | curl --fail --request POST \
      --header 'Content-Type: application/json' \
      --data-binary @- \
      http://localhost:8083/connectors
```

## Kafka topics

Kafka topic auto-creation is disabled. Create the four table topics and the
internal signaling topic before registering the connector or requesting a
snapshot:

```bash
for topic in customers products orders order_items; do
  docker exec retail-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "retail.public.${topic}" \
    --partitions 3 \
    --replication-factor 1
done

docker exec retail-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic retail.public.debezium_signal \
  --partitions 1 \
  --replication-factor 1
```

## Backfill newly added tables

After adding tables to an existing connector, request an incremental snapshot:

```sql
INSERT INTO public.debezium_signal (id, type, data)
VALUES (
    'retail-source-backfill-v1',
    'execute-snapshot',
    '{"data-collections":["public.customers","public.products","public.order_items"],"type":"incremental"}'
);
```
