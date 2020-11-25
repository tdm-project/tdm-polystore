#!/bin/bash

set -o nounset

app_id="DMWSuGha6KgsE1d9s2wJ"
app_code="voC1-vkRneenHHQTrpvR7A"
TIME=300

if [[ $# == 1 ]]; then
  Token="${1}"
  printf "Using provided tdmq token %s\n" "${Token}" >&2
elif [[ -n "${TDMQ_AUTH_TOKEN:-}" ]]; then
  Token="${TDMQ_AUTH_TOKEN}"
  printf "Using tdmq token from TDMQ_AUTH_TOKEN environment variable\n" >&2
else
  printf "Usage: $0 <tdmq token>\n" >&2
  exit 2
fi

while true;
do
    python3 ingestor.py --tdmq-token ${Token} --app-id ${app_id} --app-code ${app_code} --source flow
    echo "Sleeping ${TIME}" >&2
    sleep ${TIME}
done
