/*******************************************************************************

    RemoveChannel request implementation.

    copyright:
        Copyright (c) 2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.neo.RemoveChannel;

import dhtproto.node.neo.request.RemoveChannel;

import dhtnode.connection.neo.SharedResources;
import dhtnode.storage.StorageEngine;

import swarm.neo.node.RequestOnConn;
import swarm.neo.request.Command;

import ocean.transition;
import ocean.core.TypeConvert : castFrom, downcast;
import ocean.core.Verify;

/*******************************************************************************

    DHT node implementation of the v0 RemoveChannel request protocol.

*******************************************************************************/

public scope class RemoveChannelImpl_v0 : RemoveChannelProtocol_v0
{
    /***************************************************************************

        Checks whether the specified client is permitted to remove channels.

        Params:
            client_name = name of client requesting channel removal

        Returns:
            true if the client is permitted to remove channels

    ***************************************************************************/

    override protected bool clientPermitted ( cstring client_name )
    {
        return client_name == "admin";
    }

    /***************************************************************************

        Removes the specified channel.

        Params:
            channel_name = channel to remove

        Returns:
            true if the operation succeeded (the channel was removed or did not
            exist); false if an error occurred

    ***************************************************************************/

    override protected bool removeChannel ( cstring channel_name )
    {
        auto resources_ =
            downcast!(SharedResources.RequestResources)(this.resources);
        verify(resources_ !is null);

        auto storage_channel = channel_name in resources_.storage_channels;

        if ( storage_channel !is null )
        {
            auto records = storage_channel.num_records;
            auto bytes = storage_channel.num_bytes;
            resources_.storage_channels.remove(channel_name);

            // Note that the number of bytes reported as having been handled by
            // this action is not strictly correct: it includes not only the
            // size of the actual records, but also the size of the TokyoCabinet
            // map structures required to store those records. This is such a
            // rarely performed request that I don't think anyone will mind ;)
            resources_.node_info.record_action_counters
                .increment("deleted", bytes, records);
        }

        return true;
    }
}
