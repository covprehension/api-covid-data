#!/bin/bash
set -e
echo "$(whoami)"

if [ "$(whoami)" == "root" ]; then
    chown -R flask:flask /home/flask/
    chown --dereference flask "/proc/$$/fd/1" "/proc/$$/fd/2" || :
    exec gosu flask "$@"
fi