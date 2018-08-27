/*******************************************************************************

    Implementation of DHT 'Get' request

    copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.GetRequest;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import Protocol = dhtproto.node.request.Get;

/*******************************************************************************

    Request handler

*******************************************************************************/

public scope class GetRequest : Protocol.Get
{
    import dhtnode.request.model.ConstructorMixin;
    import dhtnode.storage.StorageEngine;

    import ocean.core.Verify;
    import ocean.core.TypeConvert : downcast;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();

    /***************************************************************************

        Must check if there is any record in specified channel with specified
        key and return it if possible

        Params:
            channel_name = name of channel to query
            key = key of record to find

        Returns:
            value of queried record, empty array if not found

    ***************************************************************************/

    final override protected void[] getValue ( cstring channel_name, cstring key )
    {
        auto storage_channel = channel_name in this.resources.storage_channels;

        if (storage_channel !is null)
        {
            auto dht_channel = downcast!(StorageEngine)(*storage_channel);
            verify(dht_channel !is null);
            mstring value_slice;
            dht_channel.get(key, *this.resources.value_buffer, value_slice);
            this.resources.node_info.record_action_counters
                .increment("read", value_slice.length);
            return value_slice;
        }

        return null;
    }
}
