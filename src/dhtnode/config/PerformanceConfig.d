/*******************************************************************************

    Performance config class for use with ocean.util.config.ClassFiller.

    copyright:
        Copyright (c) 2012-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.config.PerformanceConfig;



/*******************************************************************************

    Performance config values

*******************************************************************************/

public class PerformanceConfig
{
    /***************************************************************************

        Period of write buffer flushing (milliseconds)

    ***************************************************************************/

    uint write_flush_ms = 250;

    /***************************************************************************

        Multiplier used to calculate the size of the database at which new data
        sent during redistributions via PutBatch will be rejected. This is to
        prevent the memory consumption of the node growing out of control due to
        uneven rates of data redistribution.

    ***************************************************************************/

    double redist_memory_limit_mulitplier = 1.1;

    /***************************************************************************

        For neo connections: toggles Nagle's algorithm (true = disabled, false =
        enabled) on the underlying socket.

    ***************************************************************************/

    bool no_delay;
}

