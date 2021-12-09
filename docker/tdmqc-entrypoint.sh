#!/bin/bash

## main ##
if [[ -z "${DONT_UNSET_HADOOP_VARS:-}" ]]; then
    echo 'Unsetting HADOOP_* env vars' >&2
    for varname in ${!HADOOP*}; do
        unset "${varname}"
    done
else
    echo 'The following HADOOP_* variables are set:' "${!HADOOP*}" >&2
fi

if [[ $# -ge 1 ]]; then
    echo "Executing command:" "${@}" >&2
    exec "${@}"
else
    printf "Hanging around to keep container alive.  Kill me to exit\n" >&2
    tail -f /dev/null
fi
