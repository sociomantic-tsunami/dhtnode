/*******************************************************************************

    DHT node implementation

    copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.node.DhtNode;



/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import swarm.node.model.NeoChannelsNode : ChannelsNodeBase;

import dhtnode.node.IDhtNodeInfo;

import dhtnode.storage.StorageEngine;

import dhtnode.connection.DhtConnectionHandler;



/*******************************************************************************

    DhtNode

*******************************************************************************/

public class DhtNode :
    ChannelsNodeBase!(StorageEngine, DhtConnectionHandler), IDhtNodeInfo
{
    import swarm.Const : NodeItem;
    import dhtnode.node.DhtHashRange;
    import dhtnode.connection.SharedResources;
    import Neo = dhtnode.connection.neo.SharedResources;
    import dhtnode.node.RequestHandlers;

    import dhtnode.config.ServerConfig;

    import dhtnode.connection.DhtConnectionHandler : DhtConnectionSetupParams;
    import dhtnode.storage.StorageChannels;

    import ocean.io.select.EpollSelectDispatcher;

    import ocean.io.compress.lzo.LzoChunkCompressor;


    /***************************************************************************

        DHT node state

    ***************************************************************************/

    private State state_;


    /**************************************************************************

        Node minimum & maximum hash

    ***************************************************************************/

    private DhtHashRange hash_range;


    /// Shared resources.
    private Neo.SharedResources shared_resources;


    /***************************************************************************

        Constructor.

        Params:
            server_config = config settings for the server
            node_item = node address/port
            channels = storage channels instance to use
            hash_range = min/max hash range tracker
            epoll = epoll select dispatcher to be used internally
            per_request_stats = names of requests to be stats tracked
            no_delay = toggle Nagle's algorithm (true = disabled, false =
                enabled) on the connection sockets

    ***************************************************************************/

    public this ( ServerConfig server_config, NodeItem node_item,
        StorageChannels channels, DhtHashRange hash_range,
        EpollSelectDispatcher epoll, istring[] per_request_stats,
        bool no_delay )
    {
        this.hash_range = hash_range;

        this.shared_resources = new Neo.SharedResources(channels, this, epoll);

        // Classic connection handler settings
        auto conn_setup_params = new DhtConnectionSetupParams;
        conn_setup_params.node_info = this;
        conn_setup_params.epoll = epoll;
        conn_setup_params.storage_channels = channels;
        conn_setup_params.shared_resources = new SharedResources;
        conn_setup_params.lzo = new LzoChunkCompressor;

        // Neo node / connection handler settings
        Options options;
        options.epoll = epoll;
        options.requests = requests;
        options.shared_resources = this.shared_resources;
        options.no_delay = no_delay;
        options.unix_socket_path = idup(server_config.unix_socket_path());
        options.credentials_filename = "etc/credentials";

        // The neo port must currently always be +100 from the legacy port. See
        // DhtHashRange.newNodeAdded().
        super(NodeItem(server_config.address(), server_config.port()),
            cast(ushort)(server_config.port() + 100), channels, conn_setup_params, options,
            server_config.backlog);

        // Initialise requests to be stats tracked.
        foreach ( cmd; per_request_stats )
        {
            this.request_stats.init(cmd);
        }
    }


    /***************************************************************************

        Returns:
            Minimum hash supported by DHT node.

    ***************************************************************************/

    override public hash_t min_hash ( )
    {
        return this.hash_range.range.min;
    }


    /***************************************************************************

        Returns:
            Maximum hash supported by DHT node.

    ***************************************************************************/

    override public hash_t max_hash ( )
    {
        return this.hash_range.range.max;
    }


    /***************************************************************************

        DHT node state setter.

        Params:
            new state of node

    ***************************************************************************/

    public void state ( State s )
    {
        this.state_ = s;
    }


    /***************************************************************************

        Returns:
            state of node

    ***************************************************************************/

    override public State state ( )
    {
        return this.state_;
    }


    /***************************************************************************

        Returns:
            identifier string for this node

    ***************************************************************************/

    override protected cstring id ( )
    {
        return typeof(this).stringof;
    }


    /***************************************************************************

        Returns:
            list of identifiers for action types being tracked for the node

    ***************************************************************************/

    override protected istring[] record_action_counter_ids ( )
    {
        return ["written", "read", "forwarded", "iterated", "deleted"];
    }

    /***************************************************************************

        Calls `callback` with a `RequestResources` object whose scope is limited
        to the run-time of `callback`.

        Params:
            callback = a callback to call with a `RequestResources` object

    ***************************************************************************/

    override protected void getResourceAcquirer (
        scope void delegate ( Object request_resources ) callback )
    {
        scope request_resources = this.shared_resources.new RequestResources;
        callback(request_resources);
    }
}
