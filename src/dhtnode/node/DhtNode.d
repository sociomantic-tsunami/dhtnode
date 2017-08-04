/*******************************************************************************

    DHT node implementation

    copyright:
        Copyright (c) 2011-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.node.DhtNode;



/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import swarm.node.model.ChannelsNode : ChannelsNodeBase;

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


    /***************************************************************************

        Constructor.

        Params:
            node_item = node address/port
            channels = storage channels instance to use
            hash_range = min/max hash range tracker
            epoll = epoll select dispatcher to be used internally
            backlog = (see ISelectListener ctor)
            per_request_stats = names of requests to be stats tracked

    ***************************************************************************/

    public this ( NodeItem node_item, StorageChannels channels,
        DhtHashRange hash_range, EpollSelectDispatcher epoll,
        int backlog, istring[] per_request_stats )
    {
        this.hash_range = hash_range;

        auto conn_setup_params = new DhtConnectionSetupParams;
        conn_setup_params.node_info = this;
        conn_setup_params.epoll = epoll;
        conn_setup_params.storage_channels = channels;
        conn_setup_params.shared_resources = new SharedResources;
        conn_setup_params.lzo = new LzoChunkCompressor;

        super(node_item, channels, conn_setup_params, backlog);

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
}

