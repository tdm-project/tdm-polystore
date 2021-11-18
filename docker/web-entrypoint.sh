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

if [[ "${DEV}" == "true" ]]; then
    printf "DEV is ${DEV}. Starting container in development mode\n" >&2
    printf "Do not run this configuration in production!\n" >&2
    set -x
    export FLASK_APP=/tdmq-dist/tdmq/wsgi
    export FLASK_ENV=development
    export FLASK_RUN_PORT=8000 # by default in dev mode it changes its port to 5000
    export FLASK_RUN_HOST=0.0.0.0
    flask run "${@}"
else
    gunicorn -b 0.0.0.0:8000 --config "python:gunicorn.conf.py" "wsgi:get_wsgi_app()"
fi
