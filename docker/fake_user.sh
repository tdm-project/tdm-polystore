#!/bin/bash

source /usr/local/lib/tdmq_scripts.sh

username="tdmqc"

printf "Creating fake NSS user %s\n" "${username}"

fake_user "${username}"

if [[ $# -ge 1 ]]; then
    exec "${@}"
else
    exec /bin/bash
fi
