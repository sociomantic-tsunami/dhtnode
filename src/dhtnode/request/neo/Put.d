/*******************************************************************************

    Put request implementation.

    copyright:
        Copyright (c) 2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.neo.Put;

import dhtproto.node.neo.request.Put;

import dhtnode.connection.neo.SharedResources;

import swarm.neo.node.RequestOnConn;
import swarm.neo.request.Command;

import ocean.transition;
import ocean.core.TypeConvert : castFrom, downcast;
import ocean.core.Verify;

import dhtnode.node.DhtHashRange;

/*******************************************************************************

    DHT node implementation of the v0 Put request protocol.

*******************************************************************************/

public scope class PutImpl_v0 : PutProtocol_v0
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
}
