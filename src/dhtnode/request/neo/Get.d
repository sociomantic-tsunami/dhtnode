/*******************************************************************************

    Get request implementation.

    copyright:
        Copyright (c) 2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.neo.Get;

import dhtproto.node.neo.request.Get;

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
            scope rq = new GetImpl_v0(rq_resources);
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

    DHT node implementation of the v0 Get request protocol.

*******************************************************************************/

public scope class GetImpl_v0 : GetProtocol_v0
{
    import swarm.util.Hash : isWithinNodeResponsibility;

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

        Checks whether the node is responsible for the specified key.

        Params:
            key = key of record to write

        Returns:
            true if the node is responsible for the key

    ***************************************************************************/

    override protected bool responsibleForKey ( hash_t key )
    {
        auto node_info = this.resources.node_info;
        return isWithinNodeResponsibility(key,
            node_info.min_hash, node_info.max_hash);
    }

    /***************************************************************************

        Gets a single record from the storage engine.

        Params:
            channel = channel to write to
            key = key of record to write
            value = buffer to receive record value. If the record does not exist
                in the storage engine, value.length must be set to 0

        Returns:
            true if the operation succeeded (the record was fetched or did not
            exist); false if an error occurred

    ***************************************************************************/

    override protected bool get ( cstring channel, hash_t key, ref void[] value )
    {
        auto storage_channel = this.resources.storage_channels.getCreate(channel);
        if (storage_channel is null)
            return false;

        auto m_value = cast(mstring)value;
        mstring value_slice;
        storage_channel.get(key, m_value, value_slice);
        value = value_slice;

        this.resources.node_info.record_action_counters
            .increment("read", value.length);

        return true;
    }
}
