/*******************************************************************************

    Implementation of DHT 'Remove' request

    copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.RemoveRequest;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import Protocol = dhtproto.node.request.Remove;

/*******************************************************************************

    Request handler

*******************************************************************************/

public scope class RemoveRequest : Protocol.Remove
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

        Removes the record from the channel

        Params:
            channel_name = name of channel to remove from
            key = key of record to remove

    ***************************************************************************/

    final override protected void remove ( cstring channel_name, cstring key )
    {
        auto storage_channel =
            *this.resources.channel_buffer in this.resources.storage_channels;
        if ( storage_channel !is null )
        {
            auto dht_channel = downcast!(StorageEngine)(*storage_channel);
            assert(dht_channel);
            auto bytes = dht_channel.getSize(key);
            dht_channel.remove(key);
            this.resources.node_info.record_action_counters
                .increment("deleted", bytes);
        }
    }
}
