/*******************************************************************************

    GetHashRange request implementation.

    copyright:
        Copyright (c) 2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.neo.GetHashRange;

import dhtproto.node.neo.request.GetHashRange;

import dhtnode.connection.neo.SharedResources;

import swarm.neo.node.RequestOnConn;
import swarm.neo.request.Command;

import ocean.transition;
import ocean.core.TypeConvert : castFrom, downcast;

import dhtnode.node.DhtHashRange;

/*******************************************************************************

    The request handler for the table of handlers. When called, runs in a fiber
    that can be controlled via `connection`.

    Params:
        shared_resources = an opaque object containing resources owned by the
            node which are required by the request
        connection  = performs connection socket I/O and manages the fiber
        cmdver      = the version number of the Consume command as specified by
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
            scope rq = new GetHashRangeImpl_v0(rq_resources);
            rq.handle(connection, msg_payload);
            break;

        default:
            auto ed = connection.event_dispatcher;
            ed.send(
                ( ed.Payload payload )
                {
                    payload.addConstant(GlobalStatusCode.RequestVersionNotSupported);
                }
            );
            break;
    }
}

/*******************************************************************************

    DHT node implementation of the v0 GetHashRange request protocol.

*******************************************************************************/

public scope class GetHashRangeImpl_v0 : GetHashRangeProtocol_v0, IHashRangeListener
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

        Gets the current hash range of this node.

        Params:
            min = out value where the current minimum hash of this node is stored
            max = out value where the current maximum hash of this node is stored

    ***************************************************************************/

    override protected void getCurrentHashRange ( out hash_t min, out hash_t max )
    {
        auto range = this.resources.storage_channels.hash_range.range;
        min = range.min;
        max = range.max;
    }

    /***************************************************************************

        Informs the node that this request is now waiting for hash range
        updates. hashRangeUpdate() will be called, when updates are pending.

    ***************************************************************************/

    override protected void registerForHashRangeUpdates ( )
    {
        this.resources.storage_channels.hash_range.updates.register(this);
    }

    /***************************************************************************

        Informs the node that this request is no longer waiting for hash range
        updates.

    ***************************************************************************/

    override protected void unregisterForHashRangeUpdates ( )
    {
        this.resources.storage_channels.hash_range.updates.unregister(this);
    }

    /***************************************************************************

        Gets the next pending hash range update (or returns false, if no updates
        are pending). The implementing node should store a queue of updates per
        GetHashRange request and feed them to the request, in order, when this
        method is called.

        Params:
            update = out value to receive the next pending update, if one is
                available

        Returns:
            false if no update is pending

    ***************************************************************************/

    override protected bool getNextHashRangeUpdate ( out HashRangeUpdate update )
    {
        return this.resources.storage_channels.hash_range.updates.
            getNextUpdate(this, update);
    }

    /***************************************************************************

        IHashRangeListener method. Notifies a request when either the hash range
        of this node has changed or information about another node is available.

    ***************************************************************************/

    public void hashRangeUpdateAvailable ( )
    {
        this.hashRangeUpdate();
    }

    /***************************************************************************

        IHashRangeListener method. Required in order for a map of interface
        instances to be possible.

    ***************************************************************************/

    override public hash_t toHash ( )
    {
        return super.toHash();
    }
}
