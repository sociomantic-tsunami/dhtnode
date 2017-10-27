/*******************************************************************************

    GetChannels request implementation.

    copyright:
        Copyright (c) 2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.neo.GetChannels;

import dhtproto.node.neo.request.GetChannels;

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
        cmdver = the version number of the GetChannels request as specified by
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
            scope rq = new GetChannelsImpl_v0(rq_resources);
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

    DHT node implementation of the v0 GetChannels request protocol.

*******************************************************************************/

public scope class GetChannelsImpl_v0 : GetChannelsProtocol_v0
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

        opApply iteration over the names of the channels in storage.

    ***************************************************************************/

    protected override int opApply ( int delegate ( ref cstring ) dg )
    {
        foreach ( channel; this.resources.storage_channels )
        {
            cstring const_channel = channel.id;
            if ( auto ret = dg(const_channel) )
                break;
        }
        return 0;
    }
}
