/*******************************************************************************

    Implementation of DHT 'Put' request

    copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.PutRequest;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import Protocol = dhtproto.node.request.Put;

/*******************************************************************************

    Request handler

*******************************************************************************/

public scope class PutRequest : Protocol.Put
{
    import dhtnode.request.model.ConstructorMixin;
    import dhtnode.storage.StorageEngine;

    import ocean.core.TypeConvert : downcast;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();

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
       return this.resources.storage_channels.sizeLimitOk(size);
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
        this.resources.node_info.record_action_counters
            .increment("written", value.length);

        auto storage_channel = this.resources.storage_channels.getCreate(channel);
        if (storage_channel is null)
            return false;

        auto dht_channel = downcast!(StorageEngine)(storage_channel);
        assert(dht_channel);
        dht_channel.put(key, cast(cstring) value);

        return true;
    }
}
