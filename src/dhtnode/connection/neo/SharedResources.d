/*******************************************************************************

    DHT shared resource manager. Handles acquiring / relinquishing of global
    resources by active request handlers.

    copyright:
        Copyright (c) 2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.connection.neo.SharedResources;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

/*******************************************************************************

    Resources owned by the node which are needed by the request handlers.

*******************************************************************************/

public final class SharedResources
{
    import ocean.util.container.pool.FreeList;
    import swarm.neo.request.RequestEventDispatcher;
    import swarm.neo.util.MessageFiber;
    import ocean.io.select.EpollSelectDispatcher;
    import dhtnode.storage.StorageChannels;
    import dhtnode.storage.StorageEngineStepIterator;
    import dhtnode.node.IDhtNodeInfo;
    import dhtproto.node.neo.request.core.IRequestResources;
    import swarm.neo.util.AcquiredResources;
    import swarm.util.RecordBatcher;
    import ocean.io.compress.Lzo;

    /***************************************************************************

        Pool of generic buffers. (We store ubyte[] buffers internally, as a
        workaround for ambiguities in ocean.core.Buffer because void[][] can be
        implicitly cast to void[].)

    ***************************************************************************/

    private FreeList!(ubyte[]) buffers;

    /// Pool of RequestEventDispatcher instances.
    private FreeList!(RequestEventDispatcher) request_event_dispatchers;

    /// Pool of MessageFiber instances.
    private FreeList!(MessageFiber) fibers;

    /// Pool of Timer instances.
    private FreeList!(Timer) timers;

    /// Pool of channel iterators.
    private FreeList!(StorageEngineStepIterator) iterators;

    /// Pool of record batchers.
    private FreeList!(RecordBatcher) record_batchers;

    /***************************************************************************

        Reference to the storage channels which the requests are operating on.

    ***************************************************************************/

    public StorageChannels storage_channels;

    /***************************************************************************

        Reference to the information interface for the node.

    ***************************************************************************/

    public IDhtNodeInfo node_info;

    /// Epoll instance used by the node.
    public EpollSelectDispatcher epoll;

    /// Shared LZO instance, used by all record batchers.
    private Lzo lzo;

    /***************************************************************************

        Constructor.

        Params:
            storage_channels = storage channels which the requests are operating
                on
            node_info = information interface to the node
            epoll = epoll instance used by the node

    ***************************************************************************/

    public this ( StorageChannels storage_channels, IDhtNodeInfo node_info,
        EpollSelectDispatcher epoll )
    {
        this.storage_channels = storage_channels;
        this.node_info = node_info;
        this.epoll = epoll;

        this.buffers = new FreeList!(ubyte[]);
        this.request_event_dispatchers = new FreeList!(RequestEventDispatcher);
        this.fibers = new FreeList!(MessageFiber);
        this.timers = new FreeList!(Timer);
        this.iterators = new FreeList!(StorageEngineStepIterator);
        this.record_batchers = new FreeList!(RecordBatcher);

        this.lzo = new Lzo;
    }

    /***************************************************************************

        Scope class which may be newed inside request handlers to get access to
        the shared pools of resources. Any acquired resources are relinquished
        in the destructor.

        The class should always be newed as scope, but cannot be declared as
        such because the request handler classes need to store a reference to it
        as a member, which is disallowed for scope instances.

    ***************************************************************************/

    public /*scope*/ class RequestResources : IRequestResources
    {
        /// Set of acquired buffers
        private AcquiredArraysOf!(void) acquired_buffers;

        /// Set of acquired fibers.
        private Acquired!(MessageFiber) acquired_fibers;

        /// Set of acquired timers.
        private Acquired!(Timer) acquired_timers;

        /// Set of acquired storage channel iterators.
        private Acquired!(StorageEngineStepIterator) acquired_iterators;

        /// Set of acquired record batchers.
        private Acquired!(RecordBatcher) acquired_record_batchers;

        /// Singleton RequestEventDispatcher used by this request.
        private AcquiredSingleton!(RequestEventDispatcher)
            acquired_request_event_dispatcher;

        /***********************************************************************

            Constructor.

        ***********************************************************************/

        this ( )
        {
            this.acquired_buffers.initialise(this.outer.buffers);
            this.acquired_fibers.initialise(this.outer.buffers,
                this.outer.fibers);
            this.acquired_timers.initialise(this.outer.buffers,
                this.outer.timers);
            this.acquired_iterators.initialise(this.outer.buffers,
                this.outer.iterators);
            this.acquired_record_batchers.initialise(this.outer.buffers,
                this.outer.record_batchers);
            this.acquired_request_event_dispatcher.initialise(
                this.outer.request_event_dispatchers);
        }

        /***********************************************************************

            Destructor. Relinquishes any acquired resources.

        ***********************************************************************/

        ~this ( )
        {
            this.acquired_buffers.relinquishAll();
            this.acquired_fibers.relinquishAll();
            this.acquired_timers.relinquishAll();
            this.acquired_iterators.relinquishAll();
            this.acquired_record_batchers.relinquishAll();
            this.acquired_request_event_dispatcher.relinquish();
        }

        /***********************************************************************

            Returns:
                the node's storage channels

        ***********************************************************************/

        public StorageChannels storage_channels ( )
        {
            return this.outer.storage_channels;
        }

        /***********************************************************************

            Returns:
                the information interface to the node

        ***********************************************************************/

        public IDhtNodeInfo node_info ( )
        {
            return this.outer.node_info;
        }

        /***********************************************************************

            Returns:
                a shared LZO instance

        ***********************************************************************/

        public Lzo lzo ( )
        {
            return this.outer.lzo;
        }

        /***********************************************************************

            Returns:
                a new iterator storage channel instance. The user must call its
                setStorage() method before use

        ***********************************************************************/

        public StorageEngineStepIterator getIterator ( )
        {
            return this.acquired_iterators.acquire(
                new StorageEngineStepIterator);
        }

        /***********************************************************************

            Returns:
                a pointer to a new chunk of memory (a void[]) to use during the
                request's lifetime

        ***********************************************************************/

        override public void[]* getVoidBuffer ( )
        {
            return this.acquired_buffers.acquire();
        }

        /***********************************************************************

            Gets a fiber to use during the request's lifetime and assigns the
            provided delegate as its entry point.

            Params:
                fiber_method = entry point to assign to acquired fiber

            Returns:
                a new MessageFiber acquired to use during the request's lifetime

        ***********************************************************************/

        override public MessageFiber getFiber ( scope void delegate ( ) fiber_method )
        {
            bool new_fiber = false;

            MessageFiber newFiber ( )
            {
                new_fiber = true;
                return new MessageFiber(fiber_method, 64 * 1024);
            }

            auto fiber = this.acquired_fibers.acquire(newFiber());
            if ( !new_fiber )
                fiber.reset(fiber_method);

            return fiber;
        }

        /***********************************************************************

            Gets a record batcher to use during the request's lifetime.

            Returns:
                a new record batcher acquired to use during the request's
                lifetime

        ***********************************************************************/

        override public RecordBatcher getRecordBatcher ( )
        {
            auto batcher = this.acquired_record_batchers.acquire(
                new RecordBatcher(this.outer.lzo));
            batcher.clear();
            return batcher;
        }

        /***********************************************************************

            Gets a periodically firing timer.

            Params:
                period_s = seconds part of timer period
                period_ms = milliseconds part of timer period
                timer_dg = delegate to call when timer fires

            Returns:
                ITimer interface to a timer to use during the request's lifetime

        ***********************************************************************/

        override public ITimer getTimer ( uint period_s, uint period_ms,
            scope void delegate ( ) timer_dg )
        {
            auto timer = this.acquired_timers.acquire(new Timer);
            timer.initialise(period_s, period_ms, timer_dg);
            return timer;
        }
    }

    /***************************************************************************

        Timer class which implements the ITimer interface expected by the
        request resources in dhtproto.

    ***************************************************************************/

    private class Timer : IRequestResources.ITimer
    {
        import ocean.io.select.client.TimerEvent;

        /// Flag set to true when the timer is running.
        private bool running;

        /// Timer event registered with epoll.
        private TimerEvent timer;

        // User's timer delegate.
        private void delegate ( ) timer_dg;

        /***********************************************************************

            Constructor.

        ***********************************************************************/

        private this ( )
        {
            this.timer = new TimerEvent(&this.timerDg);
        }

        /***********************************************************************

            Sets up the timer period and user delegate.

            Params:
                period_s = seconds part of timer period
                period_ms = milliseconds part of timer period
                timer_dg = delegate to call when timer fires

        ***********************************************************************/

        private void initialise ( uint period_s, uint period_ms,
            scope void delegate ( ) timer_dg )
        {
            this.timer_dg = timer_dg;
            this.timer.set(period_s, period_ms, period_s, period_ms);
        }

        /***********************************************************************

            Starts the timer, registering it with epoll.

        ***********************************************************************/

        public void start ( )
        {
            this.running = true;
            this.outer.epoll.register(this.timer);
        }

        /***********************************************************************

            Stops the timer, unregistering it from epoll.

        ***********************************************************************/

        public void stop ( )
        {
            this.running = false;
            this.outer.epoll.unregister(this.timer);
        }

        /***********************************************************************

            Internal delegate called when timer fires. Calls the user's delegate
            and handles unregistering when stopped.

            Returns:
                true to re-register, false to unregister

        ***********************************************************************/

        private bool timerDg ( )
        {
            this.timer_dg();

            // Just in case the timer has fired in epoll, then stop() is called,
            // then this delegate is called, we unregister if the timer should
            // no longer be running.
            return this.running;
        }
    }
}
