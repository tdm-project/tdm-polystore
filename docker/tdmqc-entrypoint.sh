#!/bin/bash

export PYTHONPATH="${TDMQ_DIST}"

function run_tests() {
  tdmqc_run_tests

  exit_code=$?

  printf "======================================================================\n" >&2

  if [[ $exit_code != 0 ]]; then
    printf "Tests FAILED!\n" >&2
  else
    printf "Tests ran successfully!\n" >&2
  fi

  printf "======================================================================\n" >&2
  return $exit_code
}

if [[ $# -ge 1 ]]; then
    printf "Executing command: ${@}\n" >&2
    exec "${@}"
else
    run_tests
    printf "Hanging around to keep container alive.  Kill me to exit\n" >&2
    tail -f /dev/null
fi
