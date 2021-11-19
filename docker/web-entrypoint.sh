#!/bin/bash

create_db() {
db_created=0
  for i in $(seq 1 10); do
    sleep 5
    echo "trying to init db..."
    flask db init
    if [ "$?" -eq 0 ]; then
      db_created=1
      echo "db initialized."
      break
    fi
  done
  if [ "$db_created" -ne 1 ]; then
    echo "cannot initialize db... exiting."
    exit 1
  fi
}

CREATE_DB=${CREATE_DB:-false}
if [[ "$CREATE_DB" == "true" ]]; then
  create_db
fi

GUNICORN_CONFIG_FILE="${GUNICORN_CONFIG:-gunicorn.conf.py}"
GUNICORN_CMD_ARGS="${GUNICORN_CMD_ARGS:-}"

if [[ "${DEV}" == "true" ]]; then
    echo "DEV is ${DEV}. Starting container in development mode" >&2
    echo "Do not run this configuration in production!" >&2
    set -x
    export FLASK_APP=/tdmq-dist/tdmq/wsgi
    export FLASK_ENV=development
    export FLASK_RUN_PORT=8000 # by default in dev mode it changes its port to 5000
    export FLASK_RUN_HOST=0.0.0.0
    flask run "${@}"
else
    echo "Starting gunicorn." >&2
    echo "Working directory: ${PWD}" >&2
    if [[ -f "${GUNICORN_CONFIG_FILE}" ]]; then
        echo "Using gunicorn configuration file ${GUNICORN_CONFIG_FILE}" >&2
        GUNICORN_CMD_ARGS+=" --config ${GUNICORN_CONFIG_FILE} "
    else
        echo "No gunicorn configuration found (path ${GUNICORN_CONFIG_FILE} does not exist)" >&2
    fi
    echo "Running gunicorn with GUNICORN_CMD_ARGS=${GUNICORN_CMD_ARGS}" >&2
    export GUNICORN_CMD_ARGS
    exec gunicorn -b 0.0.0.0:8000 "wsgi:get_wsgi_app()"
fi
