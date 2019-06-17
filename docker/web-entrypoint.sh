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

gunicorn -b 0.0.0.0:8000 wsgi:app
