/*******************************************************************************

    Implementation of DHT 'PutBatch' request

    copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.PutBatchRequest;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import Protocol = dhtproto.node.request.PutBatch;

import ocean.util.log.Logger;

/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("dhtnode.request.PutBatchRequest");
}

/*******************************************************************************

    Request handler

*******************************************************************************/

public class PutBatchRequest : Protocol.PutBatch
{
    import dhtnode.node.RedistributionProcess;
    import dhtnode.storage.StorageEngine;
    import dhtnode.request.model.ConstructorMixin;

    import ocean.core.TypeConvert : downcast;

    /***************************************************************************

        Used to cache storage channel current request operates on

    ***************************************************************************/

    private StorageEngine storage_channel;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();

    /***************************************************************************

        Caches requested channel

    ***************************************************************************/
    
    final override protected bool prepareChannel ( cstring channel_name )
    {
        if (!super.prepareChannel(channel_name))
            return false;
        auto storage_channel = this.resources.storage_channels.getCreate(channel_name);
        this.storage_channel = downcast!(StorageEngine)(storage_channel);
        if (this.storage_channel is null)
            return false;
        return true;
    }

    /***************************************************************************

        Verifies that this node is responsible of handling specified record key

        Params:
            key = key to check

        Returns:
            'true' if key is allowed / accepted

    ***************************************************************************/

    final override protected bool isAllowed ( cstring key )
    {
        return this.resources.storage_channels.responsibleForKey(key);
    }

    /***************************************************************************

        Verifies that this node is allowed to store records of given size

        Params:
            size = size to check

        Returns:
            'true' if size is allowed

    ***************************************************************************/

    final override protected bool isSizeAllowed ( size_t size )
    {
        if ( !this.resources.storage_channels.sizeLimitOk(size) )
        {
            .log.warn("Batch rejected: size limit exceeded");
            return false;
        }

        if ( !redistribution_process.allowed(size) )
        {
            .log.warn("Batch rejected: uneven redistribution");
            return false;
        }

        return true;
    }

    /***************************************************************************

        Tries storing record in DHT and reports success status

        Params:
            channel = channel to write record to
            key = record key
            value = record value

        Returns:
            'true' if storing was successful

    ***************************************************************************/

    final override protected bool putRecord ( cstring channel, cstring key,
        in void[] value )
    {
        this.storage_channel.put(key, cast(cstring) value, false);
        this.resources.node_info.record_action_counters
            .increment("written", value.length);
        return true;
    }
}
