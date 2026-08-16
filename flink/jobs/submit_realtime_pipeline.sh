#!/usr/bin/env sh

set -eu

: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS must be set}"
: "${GOOGLE_CLOUD_PROJECT:?GOOGLE_CLOUD_PROJECT must be set}"
: "${DEBEZIUM_HEARTBEAT_TOPIC:?DEBEZIUM_HEARTBEAT_TOPIC must be set}"

sql_directory="${FLINK_SQL_DIRECTORY:-/opt/flink/sql}"
rendered_sql="$(mktemp /tmp/retail-realtime-pipeline.XXXXXX.sql)"
submission_log="$(mktemp /tmp/retail-realtime-submission.XXXXXX.log)"

cleanup() {
    rm -f "${rendered_sql}" "${submission_log}"
}

trap cleanup EXIT INT TERM

if ! find /opt/flink/connectors -name '*.jar' -type f \
    -print -quit | grep -q .; then
    echo "No Flink connector JARs were found" >&2
    exit 1
fi

{
    for sql_file in \
        "${sql_directory}/settings.sql" \
        "${sql_directory}/kafka_sources.sql" \
        "${sql_directory}/typed_views.sql" \
        "${sql_directory}/bigquery_sinks.sql" \
        "${sql_directory}/streaming_job.sql"
    do
        cat "${sql_file}"
        printf '\n'
    done
} \
    | envsubst '${KAFKA_BOOTSTRAP_SERVERS} ${GOOGLE_CLOUD_PROJECT} ${DEBEZIUM_HEARTBEAT_TOPIC}' \
    > "${rendered_sql}"

if /opt/flink/bin/sql-client.sh \
    --library /opt/flink/connectors \
    --file "${rendered_sql}" \
    > "${submission_log}" 2>&1; then

    if grep -q '\[ERROR\]' "${submission_log}"; then
        echo "Flink SQL execution failed" >&2
        tail -n 200 "${submission_log}" >&2
        exit 1
    fi

    job_id="$(
        sed -n 's/^Job ID: //p' "${submission_log}" \
            | tail -n 1
    )"

    if [ -z "${job_id}" ]; then
        echo "Flink submission succeeded but returned no Job ID" >&2
        tail -n 100 "${submission_log}" >&2
        exit 1
    fi

    printf '%s\n' "Flink realtime pipeline submitted successfully"
    printf 'Job ID: %s\n' "${job_id}"
else
    exit_code=$?

    printf 'Flink submission failed with exit code %s\n' \
        "${exit_code}" >&2

    tail -n 200 "${submission_log}" >&2
    exit "${exit_code}"
fi