#!/bin/bash

source /usr/local/lib/tdmq_scripts.sh

username="tdmqc"

printf "Creating fake NSS user %s\n" "${username}"

fake_user "${username}"

if [[ $# > 1 ]]; then
    set -x
    exec "${@}"
else
    exec /bin/bash
fi
