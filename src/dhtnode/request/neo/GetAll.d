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

/*******************************************************************************

    The request handler for the table of handlers. When called, runs in a fiber
    that can be controlled via `connection`.

    Params:
        shared_resources = an opaque object containing resources owned by the
            node which are required by the request
        connection  = performs connection socket I/O and manages the fiber
        cmdver      = the version number of the GetAll request as specified by
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
            scope rq = new GetAllImpl_v0(rq_resources);
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

    DHT node implementation of the v0 GetAll request protocol.

*******************************************************************************/

public scope class GetAllImpl_v0 : GetAllProtocol_v0
{
    import ocean.core.array.Mutation : copy;
    import ocean.text.convert.Hash : toHashT;
    import ocean.core.array.Mutation : pop;
    import dhtnode.storage.StorageEngineStepIterator;

    /// Request resources
    private SharedResources.RequestResources resources;

    /// Storage channel being iterated.
    private StorageEngine channel;

    /// Storage channel iterator.
    private StorageEngineStepIterator iterator;

    /***************************************************************************

        Constructor.

        Params:
            resources = shared resource acquirer

    ***************************************************************************/

    public this ( SharedResources.RequestResources resources )
    {
        super(resources);

        this.resources = resources;
        this.iterator = this.resources.getIterator();
    }

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
        this.channel = this.resources.storage_channels.getCreate(channel_name);
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
        this.channel = this.resources.storage_channels.getCreate(channel_name);
        if (this.channel is null)
            return false;

        this.iterator.setStorage(this.channel);
        this.iterator.startFrom(continue_from);

        return true;
    }

    /***************************************************************************

        Gets the next record in the iteration, if one exists.

        Params:
            key = receives the key of the next record, if available
            value = receives the value of the next record, if available

        Returns:
            true if a record was returned via the out arguments or false if the
            iteration is finished

    ***************************************************************************/

    override protected bool getNext ( out hash_t key, ref void[] value )
    {
        this.iterator.next();
        if ( this.iterator.lastKey() )
            return false;

        auto ok = toHashT(this.iterator.key, key);
        assert(ok);
        value.copy(this.iterator.value);

        return true;
    }
}
