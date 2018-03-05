#!/bin/sh

# Exits with an error message
error_exit()
{
    msg="$1"
    code="$2"
    echo "$msg" 1>&2
    exit "$code"
}

if [ "$1" = "configure" ]; then
    addgroup --system core
    adduser --system --no-create-home dhtnode

    # Check that deployment directory exists
    test -d /srv/dhtnode || error_exit "/srv/dhtnode/dhtnode-* directories missing" 1

    # Create directory to which dhtnode will write log files, if it does not
    # exist, and ensure proper permissions.
    for FOLDER in /srv/dhtnode/dhtnode-*
    do
        mkdir -p $FOLDER/data $FOLDER/etc $FOLDER/log

        chown dhtnode:core $FOLDER/data $FOLDER/etc $FOLDER/log

        # only dhtnode (not group!) should be able to write to the log dir,
        # otherwise logrotate will complain...
        chmod u=rwx,g=rx,o=rx $FOLDER/log
    done
fi
