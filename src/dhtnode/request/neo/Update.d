/*******************************************************************************

    Update request implementation.

    copyright:
        Copyright (c) 2018 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.neo.Update;

import dhtproto.node.neo.request.Update;

import dhtnode.connection.neo.SharedResources;
import dhtnode.node.DhtHashRange;

import swarm.neo.node.RequestOnConn;

import ocean.transition;
import ocean.core.TypeConvert : downcast;
import ocean.core.Verify;

/*******************************************************************************

    DHT node implementation of the v0 Update request protocol.

*******************************************************************************/

public scope class UpdateImpl_v0 : UpdateProtocol_v0
{
    import swarm.util.Hash : isWithinNodeResponsibility;

    /***************************************************************************

        Checks whether the node is responsible for the specified key.

        Params:
            key = key of record to write

        Returns:
            true if the node is responsible for the key

    ***************************************************************************/

    override protected bool responsibleForKey ( hash_t key )
    {
        auto resources_ =
            downcast!(SharedResources.RequestResources)(this.resources);
        verify(resources_ !is null);

        auto node_info = resources_.node_info;
        return isWithinNodeResponsibility(key,
            node_info.min_hash, node_info.max_hash);
    }

    /***************************************************************************

        Updates a single record from the storage engine.

        Params:
            channel = channel to read from
            key = key of record to read
            dg = called with the value of the record, if it exists

        Returns:
            true if the operation succeeded (the record was fetched or did not
            exist); false if an error occurred

    ***************************************************************************/

    override protected bool get ( cstring channel, hash_t key,
        void delegate ( Const!(void)[] value ) dg )
    {
        auto resources_ =
            downcast!(SharedResources.RequestResources)(this.resources);
        verify(resources_ !is null);

        auto storage_channel = resources_.storage_channels.getCreate(channel);
        if (storage_channel is null)
            return false;

        storage_channel.get(key,
            ( cstring value )
            {
                resources_.node_info.record_action_counters
                    .increment("read", value.length);
                dg(value);
            }
        );

        return true;
    }

    /***************************************************************************

        Writes a single record to the storage engine.

        Params:
            channel = channel to write to
            key = key of record to write
            value = record value to write

        Returns:
            true if the record was written; false if an error occurred

    ***************************************************************************/

    override protected bool put ( cstring channel, hash_t key, in void[] value )
    {
        auto resources_ =
            downcast!(SharedResources.RequestResources)(this.resources);
        verify(resources_ !is null);

        auto storage_channel = resources_.storage_channels.getCreate(channel);
        if (storage_channel is null)
            return false;

        storage_channel.put(key, cast(cstring) value);

        resources_.node_info.record_action_counters
            .increment("written", value.length);

        return true;
    }

    /***************************************************************************

        Removes a single record from the storage engine.

        Params:
            channel = channel to remove to
            key = key of record to remove

        Returns:
            true if the record was removed; false if an error occurred

    ***************************************************************************/

    override protected bool remove ( cstring channel, hash_t key )
    {
        auto resources_ =
            downcast!(SharedResources.RequestResources)(this.resources);
        verify(resources_ !is null);

        auto storage_channel = resources_.storage_channels.getCreate(channel);
        if (storage_channel is null)
            return false;

        auto bytes = storage_channel.getSize(key);
        if ( bytes > 0 )
        {
            storage_channel.remove(key);
            resources_.node_info.record_action_counters.increment("deleted", bytes);
        }

        return true;
    }
}
