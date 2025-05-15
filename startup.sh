#!/bin/bash
set -e

SCRIPT=${1:-producer}
MAX_WAIT=120

wait_for() {
    local name=$1
    local host=$2
    local port=$3
    local elapsed=0
    echo "Waiting for ${name} at ${host}:${port}..."
    until nc -z "${host}" "${port}"; do
        if [ "${elapsed}" -ge "${MAX_WAIT}" ]; then
            echo "ERROR: ${name} did not become available within ${MAX_WAIT}s — aborting"
            exit 1
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    echo "${name} is ready"
}

wait_for "Kafka" kafka 9092

if [ "${SCRIPT}" = "consumer" ]; then
    wait_for "MongoDB" mongodb 27017

    PG_HOST_VAL=${PG_HOST:-postgresql}
    PG_PORT_VAL=${PG_PORT:-5432}
    PG_DB_VAL=${PG_DB:-transactions}
    PG_USER_VAL=${PG_USER:-postgres}
    elapsed=0
    echo "Waiting for PostgreSQL at ${PG_HOST_VAL}:${PG_PORT_VAL}..."
    until PGPASSWORD="${PG_PASSWORD:-postgres}" psql \
        -h "${PG_HOST_VAL}" -p "${PG_PORT_VAL}" \
        -U "${PG_USER_VAL}" -d "${PG_DB_VAL}" \
        -c "\q" > /dev/null 2>&1; do
        if [ "${elapsed}" -ge "${MAX_WAIT}" ]; then
            echo "ERROR: PostgreSQL did not become available within ${MAX_WAIT}s — aborting"
            exit 1
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    echo "PostgreSQL is ready"
fi

echo "Starting ${SCRIPT}..."
exec python "src/${SCRIPT}.py"
