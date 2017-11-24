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

/*******************************************************************************

    The request handler for the table of handlers. When called, runs in a fiber
    that can be controlled via `connection`.

    Params:
        shared_resources = an opaque object containing resources owned by the
            node which are required by the request
        connection = performs connection socket I/O and manages the fiber
        cmdver = the version number of the RemoveChannel request as specified by
            the client
        msg_payload = the payload of the first message of this request

*******************************************************************************/

public void handle ( Object shared_resources, RequestOnConn connection,
    Command.Version cmdver, Const!(void)[] msg_payload )
{
    auto dht_shared_resources = downcast!(SharedResources)(shared_resources);
    assert(dht_shared_resources);

    switch ( cmdver )
    {
        case 0:
            scope rq_resources = dht_shared_resources.new RequestResources;
            scope rq = new RemoveChannelImpl_v0(rq_resources);
            rq.handle(connection, msg_payload);
            break;

        default:
            auto ed = connection.event_dispatcher;
            ed.send(
                ( ed.Payload payload )
                {
                    payload.addConstant(
                        GlobalStatusCode.RequestVersionNotSupported);
                }
            );
            break;
    }
}

/*******************************************************************************

    DHT node implementation of the v0 RemoveChannel request protocol.

*******************************************************************************/

public scope class RemoveChannelImpl_v0 : RemoveChannelProtocol_v0
{
    /// Request resources
    private SharedResources.RequestResources resources;

    /***************************************************************************

        Constructor.

        Params:
            resources = shared resource acquirer

    ***************************************************************************/

    public this ( SharedResources.RequestResources resources )
    {
        super(resources);

        this.resources = resources;
    }

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

        return true;
    }
}
