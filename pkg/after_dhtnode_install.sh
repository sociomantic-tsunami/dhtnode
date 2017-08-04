#!/bin/sh

if [ "$1" = "configure" ]; then
    addgroup --system core
    adduser --system --no-create-home dhtnode

    # Check that deployment directory exists
    test -d /srv/dhtnode || exit 1

    # Create directory to which dhtnode will write log files, if it does not
    # exist, and ensure proper permissions.
    for FOLDER in /srv/dhtnode/dhtnode-*
    do
        mkdir -p $FOLDER/data $FOLDER/etc $FOLDER/log

        # TODO: adapt this when the dhtnode runs as its own user
        chown dhtnode:core $FOLDER/data $FOLDER/etc $FOLDER/log

        # only dhtnode (not group!) should be able to write to the log dir,
        # otherwise logrotate will complain...
        chmod u=rwx,g=rx,o=rx $FOLDER/log
    done
fi
