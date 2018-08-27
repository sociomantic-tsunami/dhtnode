/*******************************************************************************

    Implementation of DHT 'RemoveChannel' request

    copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.RemoveChannelRequest;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import Protocol = dhtproto.node.request.RemoveChannel;

/*******************************************************************************

    Request handler

*******************************************************************************/

public scope class RemoveChannelRequest : Protocol.RemoveChannel
{
    import dhtnode.request.model.ConstructorMixin;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();

    /***************************************************************************

        Must remove the specified channel from the storage engine.
        Any failure is considered critical.

        Params:
            channel_name = name of channel to be removed

    ***************************************************************************/

    final override protected void removeChannel ( cstring channel_name )
    {
        auto storage_channel = channel_name in this.resources.storage_channels;

        if ( storage_channel !is null )
        {
            auto records = storage_channel.num_records;
            auto bytes = storage_channel.num_bytes;
            this.resources.storage_channels.remove(channel_name);

            // Note that the number of bytes reported as having been handled by
            // this action is not strictly correct: it includes not only the
            // size of the actual records, but also the size of the TokyoCabinet
            // map structures required to store those records. This is such a
            // rarely performed request that I don't think anyone will mind ;)
            this.resources.node_info.record_action_counters
                .increment("deleted", bytes, records);
        }
    }
}
