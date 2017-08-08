/*******************************************************************************

    Mirror request implementation.

    copyright:
        Copyright (c) 2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.neo.Mirror;

import dhtproto.node.neo.request.Mirror;

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
        cmdver      = the version number of the Mirror request as specified by
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
            scope rq = new MirrorImpl_v0(rq_resources);
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

    DHT node implementation of the v0 Mirror request protocol.

*******************************************************************************/

public scope class MirrorImpl_v0 : MirrorProtocol_v0, StorageEngine.IListener
{
    import ocean.text.convert.Hash : toHashT;
    import ocean.core.array.Mutation : pop;
    import dhtnode.storage.StorageEngineStepIterator;

    /// Request resources
    private SharedResources.RequestResources resources;

    /// Storage channel being mirrored.
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
    }

    /***************************************************************************

        Performs any logic needed to subscribe to and start mirroring the
        channel of the given name.

        Params:
            channel_name = channel to mirror

        Returns:
            true if the channel may be used, false to abort the request

    ***************************************************************************/

    override protected bool prepareChannel ( cstring channel_name )
    {
        this.channel = this.resources.storage_channels.getCreate(channel_name);
        if (this.channel is null)
            return false;

        return true;
    }

    /***************************************************************************

        Returns:
            the name of the channel being mirrored (for logging)

    ***************************************************************************/

    override protected cstring channelName ( )
    {
        return this.channel.id;
    }

    /***************************************************************************

        Registers this request to receive updates on the channel.

    ***************************************************************************/

    override protected void registerForUpdates ( )
    {
        assert(this.channel !is null);
        this.channel.registerListener(this);
    }

    /***************************************************************************

        Unregisters this request from receiving updates on the channel.

    ***************************************************************************/

    override protected void unregisterForUpdates ( )
    {
        if (this.channel !is null)
            this.channel.unregisterListener(this);
    }

    /***************************************************************************

        Gets the value of the record with the specified key, if it exists.

        Params:
            key = key of record to get from storage
            buf = buffer to write the value into

        Returns:
            record value or null, if the record does not exist

    ***************************************************************************/

    override protected void[] getRecordValue ( hash_t key, ref void[] buf )
    {
        auto str_value = cast(mstring)buf;
        mstring value_slice;
        this.channel.get(key, str_value, value_slice);

        // It's possible that the record could have been removed in the
        // meantime, so only return it if it still exists.
        if ( value_slice is null )
            return null;

        buf = value_slice;
        return buf;
    }

    /***************************************************************************

        Called to begin iterating over the channel being mirrored.

    ***************************************************************************/

    override protected void startIteration ( )
    {
        this.iterator = this.resources.getIterator();
        this.iterator.setStorage(this.channel);
    }

    /***************************************************************************

        Gets the key of the next record in the iteration.

        Params:
            hash_key = output value to receive the next key

        Returns:
            true if hash_key was set or false if the iteration is finished

    ***************************************************************************/

    override protected bool iterateNext ( out hash_t hash_key )
    {
        this.iterator.next();
        if ( this.iterator.lastKey() )
            return false;

        auto ok = toHashT(this.iterator.key, hash_key);
        assert(ok);
        return true;
    }

    /***************************************************************************

        DhtListener interface method. Called by Storage when records are
        modified or the channel is deleted.

        Params:
            code = trigger event code
            key  = new dht key

    ***************************************************************************/

    public void trigger ( Code code, cstring key )
    {
        void update ( UpdateType type, cstring key )
        {
            hash_t hash_key;
            auto ok = toHashT(key, hash_key);
            assert(ok);

            this.updated(Update(type, hash_key));
        }

        with ( Code ) switch ( code )
        {
            case DataReady:
                update(UpdateType.Change, key);
                break;

            case Deletion:
                update(UpdateType.Deletion, key);
                break;

            case Finish:
                this.channelRemoved();
                break;

            default:
               break;
        }
    }
}
