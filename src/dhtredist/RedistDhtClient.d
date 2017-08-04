/*******************************************************************************

    Specialised DHT client for DHT reidtribution tool.

    copyright:
        Copyright (c) 2014-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtredist.RedistDhtClient;


/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import dhtproto.client.DhtClient;

/*******************************************************************************

    Dht client sub-class with the following modifications:
        * Modifies the behaviour upon setting the hash range of a node in the
          node registry, such that the usual consistency checks are not applied.
          This is because a DHT which is to be redistributed typically is *not*
          consistent, by the standard definition -- newly added nodes will have
          essentially random hash ranges in their configuration files (to be
          overwritten by the redistribution when it begins).
        * Adds the facility to assign a Redistribute request.

*******************************************************************************/

public class RedistDhtClient : SchedulingDhtClient
{
    import swarm.Const;
    import swarm.client.model.IClient;
    import swarm.client.model.ClientSettings;
    import swarm.client.connection.RequestOverflow;

    import dhtproto.client.legacy.DhtConst : DhtConst, HashRange, NodeHashRange;
    import dhtproto.client.legacy.internal.connection.DhtNodeConnectionPool;
    import Swarm = dhtproto.client.legacy.internal.registry.DhtNodeRegistry;
    import dhtproto.client.legacy.internal.RequestSetup;
    import dhtproto.client.legacy.internal.request.params.RedistributeInfo;


    /***************************************************************************

        Specialised dht node registry which does not perform the usual
        consistency checks on nodes' hash ranges (see explanation above).

    ***************************************************************************/

    private static class DhtNodeRegistry : Swarm.DhtNodeRegistry
    {
        /***********************************************************************

            Constructor

            Params:
                epoll = selector dispatcher instance to register the socket and
                    I/O events
                settings = client settings instance
                request_overflow = overflow handler for requests which don't fit
                    in the request queue
                error_reporter = error reporter instance to notify on error or
                    timeout

        ***********************************************************************/

        public this ( EpollSelectDispatcher epoll, ClientSettings settings,
            IRequestOverflow request_overflow,
            INodeConnectionPoolErrorReporter error_reporter )
        {
            super(epoll, settings, request_overflow, error_reporter);
        }


        /***********************************************************************

            Sets the hash range for which a node is responsible. The standard
            consistency checks with other nodes in the registry are not
            performed.

            Params:
                address = address of node to set hash range for
                port = port of node to set hash range for
                min = minimum hash the specified node should handle
                max = maximum hash the specified node should handle

        ***********************************************************************/

        override public void setNodeResponsibleRange ( mstring address, ushort port,
            hash_t min, hash_t max )
        {
            auto conn_pool = super.inRegistry(address, port);
            assert(conn_pool, "node not in registry");

            auto dht_conn_pool = (cast(DhtNodeConnectionPool*)conn_pool);
            dht_conn_pool.setNodeRange(min, max);
        }
    }


    /***************************************************************************

        Constructor

        Params:
            epoll = EpollSelectorDispatcher instance to use
            conn_limit = maximum number of connections to each DHT node
            queue_size = maximum size of the per-node request queue
            fiber_stack_size = size of connection fibers' stack (in bytes)
            max_events = limit on the number of events which can be managed
                by the scheduler at one time. (0 = no limit)

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll,
        size_t conn_limit = IClient.Config.default_connection_limit,
        size_t queue_size = IClient.Config.default_queue_size,
        size_t fiber_stack_size = IClient.default_fiber_stack_size,
        uint max_events = 0 )
    {
        super(epoll, conn_limit, queue_size, fiber_stack_size, max_events);
    }


    /***************************************************************************

        Constructs the client's dht node registry, returning an instance of the
        specialised registry defined above.

        Params:
            epoll = epoll instance
            settings = client settings instance
            request_overflow = overflow handler for requests which don't fit in
                the request queue
            error_reporter = error reporter instance to notify on error or
                timeout

        Returns:
            new specialised DhtNodeRegistry instance

    ***************************************************************************/

    override protected DhtNodeRegistry newDhtNodeRegistry (
        EpollSelectDispatcher epoll, ClientSettings settings,
        IRequestOverflow request_overflow,
        INodeConnectionPoolErrorReporter error_reporter )
    {
        return new DhtNodeRegistry(epoll, settings, request_overflow,
            error_reporter);
    }


    /***************************************************************************

        Creates a Redistribute request, sent to the specified node, which will
        cause it to change its hash responsibility range and to redistribute any
        records for which it is no longer responsible to one of a list of other
        nodes. All this information is contained in an instance of the
        RedistributeInfo struct, returned by the user-provided input delegate,
        of type:

            RedistributeInfo delegate ( RequestContext context )

        Params:
            addr = ip address of dht node to send request to
            port = port of dht node to send request to
            input = input delegate which should return redistribution info
            notifier = notification callback

        Returns:
            instance allowing optional settings to be set and then to be passed
            to assign()

    ***************************************************************************/

    private struct Redistribute
    {
        mixin RequestBase;
        mixin IODelegate;       // io(T) method
        mixin Node;             // node(NodeItem) method

        mixin RequestParamsSetup; // private setup() method, used by assign()
    }

    public Redistribute redistribute ( mstring addr, ushort port,
        RequestParams.RedistributeDg input, RequestNotification.Callback notifier )
    {
        return *Redistribute(DhtConst.Command.E.Redistribute,
            notifier).node(NodeItem(addr, port)).io(input);
    }
}


