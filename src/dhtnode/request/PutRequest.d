/*******************************************************************************

    Implementation of DHT 'Put' request

    copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved

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

    import ocean.core.Verify;
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

        Returns:
            the maximum size (in bytes) allowed for a record to be added to the
            storage engine. (Uses the value configured for the maximum size of
            a GetAll record batch, ensuring that all records added to the
            storage engine can be returned to the client via GetAll.)

    ***************************************************************************/

    final override protected size_t recordSizeLimit ( )
    {
        // Packing the record in the batch brings overhead of:
        // 16 bytes for the key (as string) and a size_t for the key's
        // length and value's length
        static immutable batch_overhead_size = 16 + 2 * size_t.sizeof;
        return this.resources.storage_channels.batch_size - batch_overhead_size;
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
        verify(dht_channel !is null);
        dht_channel.put(key, cast(cstring) value);

        return true;
    }
}
