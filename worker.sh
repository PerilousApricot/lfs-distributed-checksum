#!/bin/bash


# Worker rolling script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

KEEP_GOING=1
while [[ $KEEP_GOING ]]; do
    $DIR/worker-real.py $@
    echo
    echo "*******************************************************************"
    echo "  Worker exited with code: $? will restart in 30 seconds           "
    echo "    To exit, hit Ctrl-C now                                        "
    echo "*******************************************************************"
    echo
    sleep 30
done
