/*******************************************************************************

    Implementation of DHT 'Exists' request

    copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.ExistsRequest;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import Protocol = dhtproto.node.request.Exists;

/*******************************************************************************

    Request handler

*******************************************************************************/

public scope class ExistsRequest : Protocol.Exists
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

        Check if there is any record in specified channel with specified
        key

        Params:
            channel_name = name of channel to check
            key          = key of record to check

        Returns:
            'true' if such record exists

    ***************************************************************************/

    final override protected bool recordExists ( cstring channel_name,
        cstring key )
    {
        auto storage_channel = channel_name in this.resources.storage_channels;
        if (storage_channel is null)
            return false;
        auto dht_channel = downcast!(StorageEngine)(*storage_channel);
        verify(dht_channel !is null);
        return dht_channel.exists(key);
    }
}
