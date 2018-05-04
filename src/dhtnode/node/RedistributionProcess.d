/*******************************************************************************

    Global redistribution process manager.

    This module is used to synchronise various pieces of information about an
    in-progress data redistribution. These details are required by several
    request handlers (i.e. not just the Redistribute request handler), hence are
    stored in their own module.

    copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.node.RedistributionProcess;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import ocean.util.log.Logger;

/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("dhtnode.node.RedistributionProcess");
}

/*******************************************************************************

    Public, global instance (only one redistribution is allowed to be in
    progress at any time -- the Redistribute request handler enforces this).
    Instantiated in main.d, before the server is started.

*******************************************************************************/

public RedistributionProcess redistribution_process;

/*******************************************************************************

    Redistribution process class.

*******************************************************************************/

public class RedistributionProcess
{
    import dhtnode.storage.StorageChannels : StorageChannels;

    /***************************************************************************

        Multiplier used to calculate the size of the database at which new data
        sent during redistributions via PutBatch will be rejected. This is to
        prevent the memory consumption of the node growing out of control due to
        uneven rates of data redistribution.

    ***************************************************************************/

    private double redist_memory_limit_mulitplier;

    /***************************************************************************

        Storage channels set

    ***************************************************************************/

    private StorageChannels channels;

    /***************************************************************************

        Is a redistribution in progress?

    ***************************************************************************/

    private bool in_progress;

    /***************************************************************************

        The maximum number of bytes allowed in the storage channels (all
        combined) during a redistribution. Calculated by starting().

    ***************************************************************************/

    private ulong storage_bytes_limit;

    /***************************************************************************

        Constructor.

        Params:
            channels = reference to storage channels set
            redist_memory_limit_mulitplier = multiplier used to calculate the
                size of the database at which new data sent during
                redistributions via PutBatch will be rejected

    ***************************************************************************/

    public this ( StorageChannels channels, double redist_memory_limit_mulitplier )
    {
        assert(channels);
        assert(redist_memory_limit_mulitplier > 0);
        this.channels = channels;
        this.redist_memory_limit_mulitplier = redist_memory_limit_mulitplier;
    }

    /***************************************************************************

        Should be called when a Redistribute request starts being handled.
        Calculates the memory size limit.

    ***************************************************************************/

    public void starting ( )
    {
        assert(!this.in_progress);
        this.in_progress = true;

        auto current_bytes = this.bytesInStorage();
        this.storage_bytes_limit = cast(ulong)
            (current_bytes * this.redist_memory_limit_mulitplier);

        log.info("Starting redistribution. Calculated maximum storage size = {}"
            ~ " (current: {} x multipler: {})", this.storage_bytes_limit,
            current_bytes, this.redist_memory_limit_mulitplier);
    }

    /***************************************************************************

        Decides whether the specified number of bytes should be allowed to be
        added to the storage.

        Params:
            bytes = number of bytes to be added

        Returns:
            true if:
                1. a redistribution is not in progress
                2. a redistribution is in progress and the additional bytes
                   do not take the storage above the calculated maximum

    ***************************************************************************/

    public bool allowed ( ulong bytes )
    {
        if ( this.in_progress )
        {
            return (this.bytesInStorage() + bytes) <= this.storage_bytes_limit;
        }
        else
        {
            return true;
        }
    }

    /***************************************************************************

        Should be called when a Redistribute request is finished.

    ***************************************************************************/

    public void finishing ( )
    {
        assert(this.in_progress);
        this.in_progress = false;

        log.info("Finishing redistribution.");
    }

    /***************************************************************************

        Returns:
            the total number of bytes in all storage channels

    ***************************************************************************/

    private ulong bytesInStorage ( )
    {
        ulong total;
        foreach ( channel; this.channels )
        {
            total += channel.num_bytes;
        }

        return total;
    }
}

version ( UnitTest )
{
    import ocean.core.Test;
    import dhtnode.config.HashRangeConfig;
    import dhtnode.node.DhtHashRange;
    import dhtnode.storage.StorageEngine;
}

/*******************************************************************************

    Test to check that the behaviour of StorageEngine.num_bytes is as this
    module expects it to be. The assumption is that the method (which is called
    by RedistributionProcess.bytesInStorage()) returns the size of the active
    data, *not* the size of the data allocated by the storage engine (and the
    TokyoCabinet database which underlies it). (We know that TokyoCabinet does
    not free allocated memory when data is removed from the database.)

*******************************************************************************/

unittest
{
    // Create a storage engine and get its initial size
    auto hr = new DhtHashRange(hash_t.min, hash_t.max, new HashRangeConfig([]));
    auto storage = new StorageEngine("dummy", hr, 0, (cstring){});
    auto initial_size = storage.num_bytes;

    // Add records until the reported size of the storage engine increases
    do
    {
        storage.put("0000000000000000", "value");
    }
    while ( storage.num_bytes == initial_size );

    // Clear the storage engine and check that the reported size returns to the
    // initial value
    storage.clear();
    test!("==")(storage.num_bytes, initial_size);
}

