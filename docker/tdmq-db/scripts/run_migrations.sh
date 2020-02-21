#!/bin/bash

set -o nounset
set -o errexit

#### main ####

if [[ "${POSTGRES_RUN_MIGRATIONS:-}" = "true" ]]; then
    source "$(dirname "${0}")/_functions.sh"
    printf "Starting DB to run migrations\n" >&2

    # start DB listening only on unix socket
    pg_ctl -D "$PGDATA" \
        -o "-c listen_addresses=''" \
        -w start

    file_env 'POSTGRES_DB' "$POSTGRES_USER"

    export PGPASSWORD="${PGPASSWORD:-$POSTGRES_PASSWORD}"

    printf "Running Alembic\n" >&2
    # set POSTGRES_HOST to an empty string forcing connection through Unix socket
    target="${POSTGRES_RUN_MIGRATIONS_TARGET:-head}"
    POSTGRES_HOST='' run_migrations.py "${target}"
    printf "Alembic upgrade to ${target} complete\n" >&2

    pg_ctl -D "$PGDATA" -m fast -w stop
    printf "All done. DB shut down\n" >&2

    unset PGPASSWORD
else
    printf "NOT running Alembic migrations because POSTGRES_RUN_MIGRATIONS is not set\n" >&2
fi
