/*******************************************************************************

    Exists request implementation.

    copyright:
        Copyright (c) 2018 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.neo.Exists;

import dhtproto.node.neo.request.Exists;

import dhtnode.connection.neo.SharedResources;

import swarm.neo.node.RequestOnConn;
import swarm.neo.request.Command;

import ocean.transition;
import ocean.core.TypeConvert : castFrom, downcast;
import ocean.core.Verify;

import dhtnode.node.DhtHashRange;

/*******************************************************************************

    DHT node implementation of the v0 Exists request protocol.

*******************************************************************************/

public scope class ExistsImpl_v0 : ExistsProtocol_v0
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

        Checks whether a single record exists in the storage engine.

        Params:
            channel = channel to check in
            key = key of record to check
            found = out value, set to true if the record exists

        Returns:
            true if the operation succeeded; false if an error occurred

    ***************************************************************************/

    override protected bool exists ( cstring channel, hash_t key, out bool found )
    {
        auto resources_ =
            downcast!(SharedResources.RequestResources)(this.resources);
        verify(resources_ !is null);

        auto storage_channel = resources_.storage_channels.getCreate(channel);
        if (storage_channel is null)
            return false;

        found = storage_channel.exists(key);
        return true;
    }
}
