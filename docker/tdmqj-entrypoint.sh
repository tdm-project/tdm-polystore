#!/bin/bash -l

# Make the current UID:GID look like the `jupyter` user
if ! getent passwd "$(id -u)" &> /dev/null && [ -e /usr/lib/libnss_wrapper.so ]; then
  export LD_PRELOAD='/usr/lib/libnss_wrapper.so'
  export NSS_WRAPPER_PASSWD="$(mktemp)"
  export NSS_WRAPPER_GROUP="$(mktemp)"
  echo "jupyter:x:$(id -u):$(id -g):Jupyter:$HOME:/bin/false" > "$NSS_WRAPPER_PASSWD"
  echo "jupyter:x:$(id -g):" > "$NSS_WRAPPER_GROUP"
fi

jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser "${@}"
