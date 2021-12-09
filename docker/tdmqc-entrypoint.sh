#!/bin/bash

function run_tests() {
  export PYTHONPATH="${TDMQ_DIST}"
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

## main ##
if [[ $# -ge 1 ]]; then
    if [[ -z "${DONT_UNSET_HADOOP_VARS:-}" ]]; then
        echo "Unsetting HADOOP_HOME and HADOOP_LOG_DIR env vars" >&2
        unset HADOOP_HOME
        unset HADOOP_LOG_DIR
    fi
    echo "Executing command:" "${@}" >&2
    exec "${@}"
else
    run_tests
    printf "Hanging around to keep container alive.  Kill me to exit\n" >&2
    tail -f /dev/null
fi
