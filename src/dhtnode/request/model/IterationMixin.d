/*******************************************************************************

    Mixin for shared iteration code

    copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.model.IterationMixin;

/*******************************************************************************

    Indicates if it is necessary to inject version for key-only iteration
    or both keys + values

*******************************************************************************/

enum IterationKind
{
    Key,
    KeyValue
}

/*******************************************************************************

    Common code shared by all requests that implement protocol based on
    dhtproto.node.request.model.CompressedBatch 

    Template Params:
        resources = host field which stores IRequestResources
        kind = indicates which version of getNext to generate
        predicate = optional predicate function to filter away some records.
            Defaults to predicate that allows everything.

*******************************************************************************/

public template ChannelIteration(alias resources, IterationKind kind, alias predicate = alwaysTrue)
{
    import dhtnode.storage.StorageEngine;
    import dhtnode.storage.StorageEngineStepIterator;
    import ocean.core.Tuple;
    import ocean.core.Verify;
    import ocean.transition;

    /***************************************************************************

        Convenience alias for argument set getNext should expect

    ***************************************************************************/

    static if (kind == IterationKind.Key)
    {
        private alias Tuple!(mstring) ARGS;
    }
    else
    {
        private alias Tuple!(mstring, mstring) ARGS;
    }

    /***************************************************************************

        Set to iterator over requested channel if that channel is present in
        the node. Set to null otherwise (should result in empty OK response)

    ***************************************************************************/

    private StorageEngineStepIterator iterator;

    /***************************************************************************

        Initialize the channel iterator

        Params:
            channel_name = name of channel to be prepared

        Return:
            `true` if it is possible to proceed with request

    ***************************************************************************/

    override protected bool prepareChannel (cstring channel_name)
    {
        auto storage_channel = channel_name in resources.storage_channels;
        if (storage_channel is null)
        {
            this.iterator = null;
        }
        else
        {
            resources.iterator.setStorage(*storage_channel);
            this.iterator = resources.iterator;
            verify(this.iterator !is null);
        }

        // even missing channel is ok, response must return empty record
        // list in that case
        return true;
    }

    /***************************************************************************
        
        Iterates records for the protocol

        Params:
            args = either key or key + value, depending on request type

        Returns:
            `true` if there was data, `false` if request is complete

    ***************************************************************************/

    override protected bool getNext (out ARGS args)
    {
        // missing channel case
        if (this.iterator is null)
            return false;

        // loops either until match is found or last key processed
        while (true)
        {
            this.iterator.next();

            resources.loop_ceder.handleCeding();

            if (this.iterator.lastKey)
                return false;

            static if (kind == IterationKind.Key)
            {
                args[0] = iterator.key();
            }
            else
            {
                args[0] = iterator.key();
                args[1] = iterator.value();
            }

            if (predicate(args))
            {
                this.resources.node_info.record_action_counters.increment(
                        "iterated", iterator.value.length);
                return true;
            }
        }
    }
}

/*******************************************************************************

    Default predicate which allows all records to be sent to the client.

    Params:
        args = any arguments

    Returns:
        true

*******************************************************************************/

public bool alwaysTrue (T...)(T args)
{
    return true;
}
