/*******************************************************************************

    Remove request implementation.

    copyright:
        Copyright (c) 2018 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.neo.Remove;

import dhtproto.node.neo.request.Remove;

import dhtnode.connection.neo.SharedResources;

import swarm.neo.node.RequestOnConn;
import swarm.neo.request.Command;

import ocean.transition;
import ocean.core.TypeConvert : castFrom, downcast;
import ocean.core.Verify;

import dhtnode.node.DhtHashRange;

/*******************************************************************************

    DHT node implementation of the v0 Remove request protocol.

*******************************************************************************/

public scope class RemoveImpl_v0 : RemoveProtocol_v0
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

        Removes a single record from the storage engine.

        Params:
            channel = channel to remove from
            key = key of record to remove
            existed = out value, set to true if the record was present and
                removed or false if the record was not present

        Returns:
            true if the operation succeeded (the record was removed or did not
            exist); false if an error occurred

    ***************************************************************************/

    override protected bool remove ( cstring channel, hash_t key,
        out bool existed )
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
            existed = true;
            storage_channel.remove(key);
            resources_.node_info.record_action_counters.increment("deleted", bytes);
        }

        return true;
    }
}
