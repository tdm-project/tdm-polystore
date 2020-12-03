

function fake_user() {
    # Make the current UID:GID look like the specified username
    local username="${1}"
    if [[ -z "${username}" ]]; then
        printf "Missing argument:  username\n" >&2
        exit 2
    fi

    if getent passwd "$(id -u)" &> /dev/null; then
        printf "A user exists. No need to fake it\n" >&2
    elif [[ -e /usr/lib/libnss_wrapper.so ]]; then
        local uid=$(id -u)
        local gid=$(id -g)
        export LD_PRELOAD='/usr/lib/libnss_wrapper.so'
        export NSS_WRAPPER_PASSWD="$(mktemp)"
        export NSS_WRAPPER_GROUP="$(mktemp)"
        echo "${username}:x:${uid}:${gid}:${username}:$HOME:/bin/false" > "$NSS_WRAPPER_PASSWD"
        echo "${username}:x:${gid}:" > "$NSS_WRAPPER_GROUP"

        printf "Installed fake NSS user %s with UID %s and GID %s\n" "${username}" ${uid} ${gid} >&2
        printf "whoami? %s\n" "$(whoami)"
    else
        printf "Can't install fake user.  libnss_wrapper is not installed\n" >&2
    fi
}
