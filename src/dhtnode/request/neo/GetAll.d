/*******************************************************************************

    GetAll request implementation.

    copyright:
        Copyright (c) 2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.neo.GetAll;

import dhtproto.node.neo.request.GetAll;

import dhtnode.connection.neo.SharedResources;
import dhtnode.storage.StorageEngine;

import swarm.neo.node.RequestOnConn;
import swarm.neo.request.Command;

import ocean.transition;
import ocean.core.TypeConvert : castFrom, downcast;
import ocean.core.Verify;

/*******************************************************************************

    DHT node implementation of the v0 GetAll request protocol.

*******************************************************************************/

public scope class GetAllImpl_v0 : GetAllProtocol_v0
{
    import ocean.core.array.Mutation : copy;
    import ocean.text.convert.Hash : toHashT;
    import ocean.core.array.Mutation : pop;
    import dhtnode.storage.StorageEngineStepIterator;

    /// Storage channel being iterated.
    private StorageEngine channel;

    /// Storage channel iterator.
    private StorageEngineStepIterator iterator;

    /***************************************************************************

        Called to begin the iteration over the channel being fetched.

        Params:
            channel_name = name of channel to iterate over

        Returns:
            true if the iteration has been initialised, false to abort the
            request

    ***************************************************************************/

    override protected bool startIteration ( cstring channel_name )
    {
        auto resources_ =
            downcast!(SharedResources.RequestResources)(this.resources);
        verify(resources_ !is null);

        this.iterator = resources_.getIterator();
        this.channel = resources_.storage_channels.getCreate(channel_name);
        if (this.channel is null)
            return false;

        this.iterator.setStorage(this.channel);

        return true;
    }

    /***************************************************************************

        Called to continue the iteration over the channel being fetched,
        continuing from the specified hash (the last record received by the
        client).

        Params:
            channel_name = name of channel to iterate over
            continue_from = hash of last record received by the client. The
                iteration will continue from the next hash in the channel

        Returns:
            true if the iteration has been initialised, false to abort the
            request

    ***************************************************************************/

    override protected bool continueIteration ( cstring channel_name,
        hash_t continue_from )
    {
        auto resources_ =
            downcast!(SharedResources.RequestResources)(this.resources);
        verify(resources_ !is null);

        this.iterator = resources_.getIterator();
        this.channel = resources_.storage_channels.getCreate(channel_name);
        if (this.channel is null)
            return false;

        this.iterator.setStorage(this.channel);
        this.iterator.startFrom(continue_from);

        return true;
    }

    /***************************************************************************

        Gets the next record in the iteration, if one exists.

        Params:
            dg = called with the key and value of the next record, if available

        Returns:
            true if a record was passed to `dg` or false if the iteration is
            finished

    ***************************************************************************/

    override protected bool getNext (
        scope void delegate ( hash_t key, Const!(void)[] value ) dg )
    {
        this.iterator.next();
        if ( this.iterator.lastKey() )
            return false;

        this.iterator.value(
            ( cstring value )
            {
                dg(this.iterator.key, value);
            }
        );

        return true;
    }
}
