#!/bin/sh
# Nebular OS container entrypoint: create the data directories and, when started as root (the default), hand
# them to the unprivileged `nos` user and run the server as that user. Volumes written by earlier images, which
# ran as root, are taken over on the first start. Started with `--user`, the image changes no ownership.
set -eu

prepare() {
    mkdir -p "$1" || true
    [ "$(id -u)" = "0" ] || return 0
    # A full ownership pass can take minutes on a large volume, so it runs until one has finished (the marker);
    # after that a start only checks the directory and its entries.
    if [ -e "$1/.nos-owned" ] && [ -z "$(find "$1" -maxdepth 1 ! -user nos -print -quit)" ]; then
        return 0
    fi
    if [ -n "$(find "$1" ! -user nos -print -quit)" ]; then
        echo "entrypoint: giving $1 to user nos" >&2
        chown -R nos:nos "$1"
    fi
    touch "$1/.nos-owned" && chown nos:nos "$1/.nos-owned" || true
}

prepare "${NOS_DATA_DIR:-/data/blobs}"
meta_path="${NOS_META_PATH:-/data/meta/metadata.db}"
case "$meta_path" in
    *:*) ;; # a SQLite URI (sqlite:, file:) — the server creates what it names
    *) prepare "$(dirname "$meta_path")" ;;
esac

if [ "$(id -u)" = "0" ]; then
    exec setpriv --reuid=nos --regid=nos --init-groups -- nebular-os "$@"
fi
exec nebular-os "$@"
