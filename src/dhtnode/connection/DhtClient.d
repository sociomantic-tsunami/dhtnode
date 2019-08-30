/*******************************************************************************

    Stripped-down / customised dht client which can be used to perform fiber-
    suspending Put requests to nodes in a partially-known dht, without requiring
    a full handshake.

    copyright:
        Copyright (c) 2014-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.connection.DhtClient;



/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import swarm.Const;
import swarm.util.ExtensibleClass;
import swarm.util.Hash : HashRange;

import swarm.client.model.IClient;
import swarm.client.model.ClientSettings;

import swarm.client.request.notifier.IRequestNotification;

import swarm.client.ClientExceptions;

import swarm.client.plugins.ScopeRequests;

import dhtproto.client.legacy.internal.request.params.RequestParams;

import dhtproto.client.legacy.internal.RequestSetup;

import swarm.client.RequestSetup;

import Swarm = dhtproto.client.legacy.internal.registry.DhtNodeRegistry;

import dhtproto.client.legacy.DhtConst;

import dhtproto.client.legacy.internal.connection.DhtNodeConnectionPool;

import ocean.core.Enforce;



/*******************************************************************************

    Custom DhtNodeRegistry with the following modifications from the base dht
    node registry in swarm:
        1. Allows the set of nodes in the registry to be cleared. (The client
           may be reused for the handling of requests to completely different
           sets of nodes, so the registry needs to be resettable.)
        2. Overrides the getResponsiblePool() method to work with a partially
           specified dht. (The base class enforces that the nodes in the
           registry cover the complete hash range.)

*******************************************************************************/

public class DhtNodeRegistry : Swarm.DhtNodeRegistry
{
    /***************************************************************************

        Constructor

        Params:
            epoll = selector dispatcher instance to register the socket and I/O
                events
            settings = client settings instance
            request_overflow = overflow handler for requests which don't fit in
                the request queue
            error_reporter = error reporter instance to notify on error or
                timeout

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, ClientSettings settings,
        IRequestOverflow request_overflow,
        INodeConnectionPoolErrorReporter error_reporter )
    {
        super(epoll, settings, request_overflow, error_reporter);
    }


    /***************************************************************************

        Removes all nodes from the registry.

    ***************************************************************************/

    public void clear ( )
    {
        this.nodes.list.length = 0;
        enableStomping(this.nodes.list);
        this.nodes.map.clear();
    }


    /***************************************************************************

        Gets the connection pool which is responsible for the given request.

        Params:
            params = request parameters

        Returns:
            connection pool responsible for request (null if none found)

    ***************************************************************************/

    override protected NodeConnectionPool getResponsiblePool ( IRequestParams params )
    {
        if ( params.node.set() )
        {
            auto pool = super.inRegistry(params.node.Address, params.node.Port);
            return pool is null ? null : *pool;
        }

        auto dht_params = cast(RequestParams)params;
        auto hash = dht_params.hash;

        foreach ( connpool; this.nodes.list )
        {
            auto dht_pool = cast(DhtNodeConnectionPool)connpool;

            if ( dht_pool.isResponsibleFor(hash) )
            {
                return dht_pool;
            }
        }

        return null;
    }
}



/*******************************************************************************

    Custom DhtClient, derived from IClient. Implements the functionality
    required by the super class plus the following features:
        1. Fiber-suspending requests via the perform() method mixed-in by the
           built-in scope requests plugin. This is the only means of assigning
           requests via this client.
        2. addNode() method which sets the new node's hash range responsibility.
        3. clearNodes() method to clear registry.
        4. Supports only two commands: Put and PutBatch.

*******************************************************************************/

public class DhtClient : IClient
{
    /***************************************************************************

        Built-in scope requests plugin, allowing fiber-suspending requests via
        the mixed-in perform() method.

    ***************************************************************************/

    mixin ExtensibleClass!(ScopeRequestsPlugin);


    /***************************************************************************

        Local alias definitions

    ***************************************************************************/

    public alias .IRequestNotification RequestNotification;

    public alias .RequestParams RequestParams;


    /***************************************************************************

        Exceptions thrown in error cases.

    ***************************************************************************/

    private BadChannelNameException bad_channel_exception;


    /***************************************************************************

        Constructor

        Params:
            epoll = EpollSelectorDispatcher instance to use
            conn_limit = maximum number of connections to each DHT node
            queue_size = maximum size of the per-node request queue
            fiber_stack_size = size (in bytes) of stack of individual connection
                fibers

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll,
        size_t conn_limit = IClient.Config.default_connection_limit,
        size_t queue_size = IClient.Config.default_queue_size,
        size_t fiber_stack_size = IClient.default_fiber_stack_size )
    {
        ClientSettings settings;
        settings.conn_limit = conn_limit;
        settings.queue_size = queue_size;
        settings.fiber_stack_size = fiber_stack_size;

        auto node_registry = new DhtNodeRegistry(epoll, settings,
            this.requestOverflow, this.errorReporter);
        super(epoll, node_registry);

        this.bad_channel_exception = new BadChannelNameException;

        this.setPlugins(new ScopeRequestsPlugin);
    }


    /***************************************************************************

        Removes all nodes from the registry.

    ***************************************************************************/

    public void clearNodes ( )
    {
        auto dht_registry = cast(DhtNodeRegistry)this.registry;
        dht_registry.clear();
    }


    /***************************************************************************

        Adds a new node to the registry. The node's hash range responsibility is
        also set.

        Params:
            node = node address/port
            range = node hash range

    ***************************************************************************/

    public void addNode ( NodeItem node, HashRange range )
    {
        auto dht_registry = cast(DhtNodeRegistry)this.registry;
        dht_registry.add(node.Address, node.Port);
        dht_registry.setNodeResponsibleRange(node.Address, node.Port,
            range.min, range.max);
        dht_registry.setNodeAPIVersion(node.Address, node.Port,
            DhtConst.ApiVersion);
    }


    /***************************************************************************

        Creates a Put request, which will send a single value with the specified
        key to the dht. The database record value is read from the specified
        input delegate, which should be of the form:

            char[] delegate ( RequestContext context )

        It is illegal to put empty values to the node.

        Params:
            channel = database channel
            key = database record key
            input = input delegate which provides record value to send
            notifier = notification callback

        Returns:
            instance allowing optional settings to be set and then to be passed
            to perform()

    ***************************************************************************/

    private struct Put
    {
        mixin RequestBase;
        mixin IODelegate;       // io(T) method
        mixin Channel;          // channel(char[]) method
        mixin Key;              // key ( K ) (K) method

        mixin RequestParamsSetup; // private setup() method, used by assign()
    }

    public Put put ( Key ) ( cstring channel, Key key, scope RequestParams.PutValueDg input,
                             scope RequestNotification.Callback notifier )
    {
        return *Put(DhtConst.Command.E.Put, notifier).channel(channel).key(key)
            .io(input).contextFromKey();
    }


    /***************************************************************************

        Creates a PutBatch request, which will send a batch of values to the
        specified node in the dht. The batch is read from the specified input
        delegate, which should be of the form:

            NodeRecordBatcher delegate ( RequestContext context )

        Params:
            addr = address of node to send batch to
            port = port of node to send batch to
            channel = database channel
            input = input delegate which provides record batch to send
            notifier = notification callback

        Returns:
            instance allowing optional settings to be set and then to be passed
            to perform()

    ***************************************************************************/

    private struct PutBatch
    {
        mixin RequestBase;
        mixin IODelegate;       // io(T) method
        mixin Channel;          // channel(char[]) method
        mixin Node;             // node(NodeItem) method

        mixin RequestParamsSetup; // private setup() method, used by assign()
    }

    public PutBatch putBatch ( mstring addr, ushort port, cstring channel,
        scope RequestParams.PutBatchDg input, scope RequestNotification.Callback notifier )
    {
        return *PutBatch(DhtConst.Command.E.PutBatch, notifier)
            .node(NodeItem(addr, port)).channel(channel).io(input);
    }


    /***************************************************************************

        Creates a new request params instance (derived from IRequestParams), and
        passes it to the provided delegate.

        This method is used by the request scheduler plugin, which needs to be
        able to construct and use a request params instance without knowing
        which derived type is used by the client.

        Params:
            dg = delegate to receive and use created scope IRequestParams
                instance

    ***************************************************************************/

    override protected void scopeRequestParams (
        scope void delegate ( IRequestParams params ) dg )
    {
        scope params = new RequestParams;
        dg(params);
    }


    /***************************************************************************

        Checks whether the given channel name is valid. Channel names can only
        contain alphanumeric characters, underscores or dashes.

        If the channel name is not valid then the user specified error callback
        is invoked with the BadChannelName status code.

        Params:
            params = request params to check

        Throws:
            * if the channel name is invalid
            * if a filtering request is being assigned but the filter string is
              empty

            (exceptions will be caught in super.assignParams)

    ***************************************************************************/

    override protected void validateRequestParams_ ( IRequestParams params )
    {
        auto dht_params = cast(RequestParams)params;

        // Validate channel name, for commands which use it
        with ( DhtConst.Command.E ) switch ( params.command )
        {
            case PutBatch:
                enforce(this.bad_channel_exception,
                    .validateChannelName(dht_params.channel));
                break;
            default:
        }
    }


    /***************************************************************************

        Assigns a new request to the client. The request is validated, and the
        notification callback may be invoked immediately if any errors are
        detected. Otherwise the request is sent to the node registry, where it
        will be either executed immediately (if a free connection is available)
        or queued for later execution.

        Note: this method is private as it is only called from perform(), which
        is the intended public interface of this class.

        Template params:
            T = request type (should be one of the structs defined in this
                module)

        Params:
            request = request to assign

    ***************************************************************************/

    private void assign ( T ) ( T request )
    {
        this.scopeRequestParams(
            ( IRequestParams params )
            {
                request.setup(params);

                this.assignParams(params);
            });
    }
}

