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
import ocean.core.Verify;

import dhtnode.node.DhtHashRange;

/*******************************************************************************

    DHT node implementation of the v0 GetHashRange request protocol.

*******************************************************************************/

public scope class GetHashRangeImpl_v0 : GetHashRangeProtocol_v0, IHashRangeListener
{
    /***************************************************************************

        Gets the current hash range of this node.

        Params:
            min = out value where the current minimum hash of this node is stored
            max = out value where the current maximum hash of this node is stored

    ***************************************************************************/

    override protected void getCurrentHashRange ( out hash_t min, out hash_t max )
    {
        auto resources_ =
            downcast!(SharedResources.RequestResources)(this.resources);
        verify(resources_ !is null);

        auto range = resources_.storage_channels.hash_range.range;
        min = range.min;
        max = range.max;
    }

    /***************************************************************************

        Informs the node that this request is now waiting for hash range
        updates. hashRangeUpdate() will be called, when updates are pending.

    ***************************************************************************/

    override protected void registerForHashRangeUpdates ( )
    {
        auto resources_ =
            downcast!(SharedResources.RequestResources)(this.resources);
        verify(resources_ !is null);

        resources_.storage_channels.hash_range.updates.register(this);
    }

    /***************************************************************************

        Informs the node that this request is no longer waiting for hash range
        updates.

    ***************************************************************************/

    override protected void unregisterForHashRangeUpdates ( )
    {
        auto resources_ =
            downcast!(SharedResources.RequestResources)(this.resources);
        verify(resources_ !is null);

        resources_.storage_channels.hash_range.updates.unregister(this);
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
        auto resources_ =
            downcast!(SharedResources.RequestResources)(this.resources);
        verify(resources_ !is null);

        return resources_.storage_channels.hash_range.updates.
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
