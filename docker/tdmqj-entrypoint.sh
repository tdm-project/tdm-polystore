#!/bin/bash -l

source /usr/local/lib/tdmq_scripts.sh

# Make the current UID:GID look like the `jupyter` user
fake_user "jupyter"

jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser "${@}"
