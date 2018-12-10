/*******************************************************************************

    DHT Node Server Daemon

    copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.main;



/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import ocean.util.app.DaemonApp;

import ocean.util.log.Logger;

import core.memory;



/*******************************************************************************

    D2-only stomping prevention counter

*******************************************************************************/

version ( D_Version2 )
{
    mixin(`
        extern(C) extern shared long stomping_prevention_counter;
    `);
}



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger logger;
static this ( )
{
    logger = Log.lookup("dhtnode.main");
}



/*******************************************************************************

    Main function. Parses command line arguments and either displays help or
    starts dht node.

    Params:
        cl_args = array with raw command line arguments

*******************************************************************************/

version (UnitTest) {} else
private int main ( istring[] cl_args )
{
    auto app = new DhtNodeServer;
    return app.main(cl_args);
}



/*******************************************************************************

    DHT node application base class

*******************************************************************************/

public class DhtNodeServer : DaemonApp
{
    import Version;

    import dhtnode.config.ServerConfig;
    import dhtnode.config.PerformanceConfig;
    import dhtnode.config.HashRangeConfig;

    import dhtnode.node.DhtHashRange;
    import dhtnode.node.RedistributionProcess;

    import dhtnode.node.DhtNode;
    import dhtnode.connection.DhtConnectionHandler;
    import dhtnode.storage.StorageChannels;

    import ocean.core.MessageFiber;
    import ocean.core.Enforce;
    import ocean.core.Verify;

    import ocean.io.select.protocol.generic.ErrnoIOException : IOWarning;
    import ocean.io.select.selector.EpollException;

    import ocean.util.config.ConfigParser;
    import ConfigReader = ocean.util.config.ConfigFiller;

    import ocean.io.select.EpollSelectDispatcher;

    import ocean.io.select.client.model.ISelectClient;

    import Hash = ocean.text.convert.Hash;

    import dhtproto.client.legacy.DhtConst;
    import swarm.util.node.log.Stats;
    import swarm.util.RecordBatcher;

    import ocean.core.ExceptionDefinitions : IOException, OutOfMemoryException;

    import core.sys.posix.signal: SIGINT, SIGTERM, SIGQUIT;
    import core.sys.posix.sys.mman : mlockall, MCL_CURRENT, MCL_FUTURE;
    import core.stdc.errno : errno, EPERM, ENOMEM;
    import core.stdc.string : strerror;
    import ocean.text.util.StringC;

    /***************************************************************************

        Memory node config values

    ***************************************************************************/

    private static class MemoryConfig
    {
        /// Maximum number of bytes allowed in the node. 0 = no size limit.
        ulong size_limit = 0;

        /// Determines if the node will mlockall() the memory so it doesn't get
        /// swapped out by the kernel under heavy memory pressure (default is
        /// true). This option is mostly intended for testing environments, in
        /// live systems it should normally always be true.
        bool lock_memory = true;

        /// Behaviour upon encountering an out-of-range record.
        ConfigReader.LimitInit!(istring, "load", "load", "fatal", "ignore")
            allow_out_of_range;

        /// If this many records have been loaded from a channel file and all
        /// were out-of-range, then abort. If 0, this behaviour is disabled.
        /// (This is to safeguard against the case where all records are
        /// out-of-range.)
        uint abort_after_all_out_of_range = 10_000;

        /// Determines if regular buffered I/O (true) or direct I/O is used
        /// (false, the default). This should be only set to true for testing
        /// purposes; using direct I/O imposes some restrictions over the type
        /// of filesystem that complicates testing quite a bit, making it
        /// impossible to load/dump files to overlayfs, tmpfs or encrypted
        /// filesystems. This option SHOULD NEVER be set to true in live systems.
        bool disable_direct_io = false;

        /// Number of buckets to allocate in each channel; passed into
        /// tokyocabinet. 0 = use tokyocabinet's default number of buckets.
        uint bnum = 0;

        /// Batch size used by legacy compressed batch requests (e.g. GetAll).
        /// This is a de facto record size limit, as any records that exceed the
        /// configured batch size cannot be returned to clients via batch
        /// requests.
        ConfigReader.Min!(size_t, 1024, RecordBatcher.DefaultMaxBatchSize) batch_size;

        /***********************************************************************

            Returns:
                OutOfRangeHandling settings struct corresponding to the values
                of allow_out_of_range and abort_after_all_out_of_range, read
                from the config file

        ***********************************************************************/

        public StorageChannels.OutOfRangeHandling out_of_range_handling ( )
        {
            StorageChannels.OutOfRangeHandling out_of_range_handling;
            out_of_range_handling.abort_after_all_out_of_range =
                this.abort_after_all_out_of_range;

            with ( StorageChannels.OutOfRangeHandling.Mode )
            switch ( this.allow_out_of_range() )
            {
                case "load":
                    out_of_range_handling.mode = Load;
                    break;
                case "fatal":
                    out_of_range_handling.mode =  Fatal;
                    break;
                case "ignore":
                    out_of_range_handling.mode =  Ignore;
                    break;
                default:
                    verify(false);
            }

            return out_of_range_handling;
        }
    }

    private MemoryConfig memory_config;


    /***************************************************************************

        Config classes for server and performance

    ***************************************************************************/

    private ServerConfig server_config;

    private PerformanceConfig performance_config;


    /***************************************************************************

        Epoll selector instance

    ***************************************************************************/

    private EpollSelectDispatcher epoll;


    /***************************************************************************

        DHT node instance. Constructed after the config file has been
        parsed -- currently the type of node is setin the config file.

    ***************************************************************************/

    private DhtNode node;


    /***************************************************************************

        Storage channels owned by the node (created by sub-class).

    ***************************************************************************/

    private StorageChannels storage_channels;


    /***************************************************************************

        Hash range for which node is responsible. Set from config file at
        startup.

    ***************************************************************************/

    private DhtHashRange hash_range;


    /***************************************************************************

        Node stats logger

    ***************************************************************************/

    private ChannelsNodeStats dht_stats;


    /***************************************************************************

        Reusable buffer for formatting error messages.

    ***************************************************************************/

    private mstring conn_error_buf;


    /***************************************************************************

        Constructor

    ***************************************************************************/

    public this ( )
    {
        version ( D_Version2 ) { }
        else
        {
            // GC collect hooks only available in D1
            GC.monitor(&this.gcCollectStart, &this.gcCollectEnd);
        }

        const app_name = "dhtnode";
        const app_desc = "dhtnode: DHT server node.";

        DaemonApp.OptionalSettings optional;
        optional.signals = [SIGINT, SIGTERM, SIGQUIT];

        this.epoll = new EpollSelectDispatcher;

        super(app_name, app_desc, version_info, optional);
    }


    /***************************************************************************

        Override default DaemonApp arguments parsing, specifying that --config
        is required.

        Params:
            app = application instance
            args = arguments parser instance

    ***************************************************************************/

    override public void setupArgs ( IApplication app, Arguments args )
    {
        super.setupArgs(app, args);

        args("config").deefalts = null;
        args("config").required;
    }


    /***************************************************************************

        Signal handler, called from epoll (via the SignalExt) when one of the
        registered signals is received by the process.

        Params:
            signum = code of signal which fired

    ***************************************************************************/

    public override void onSignal ( int signum )
    {
        switch ( signum )
        {
            case SIGINT:
            case SIGTERM:
            case SIGQUIT:
                this.sigintHandler();
                break;

            default:
                break;
        }
    }


    /***************************************************************************

        Get values from the configuration file.

        Params:
            app = application instance
            config = config parser instance

    ***************************************************************************/

    public override void processConfig ( IApplication app, ConfigParser config )
    {
        ConfigReader.fill("Server", this.server_config, config);
        ConfigReader.fill("Performance", this.performance_config, config);
        ConfigReader.fill("Options_Memory", this.memory_config, config);

        hash_t min, max;

        enforce(Hash.hashDigestToHashT(this.server_config.minval(), min, true),
            "Minimum hash specified in config file is invalid -- "
            "a full-length hash is expected");

        enforce(Hash.hashDigestToHashT(this.server_config.maxval(), max, true),
            "Maximum hash specified in config file is invalid -- "
            "a full-length hash is expected");

        this.hash_range = new DhtHashRange(min, max,
            new HashRangeConfig(this.config_ext.default_configs));
    }


    /***************************************************************************

        Do the actual application work. Called by the super class.

        Params:
            args = command line arguments
            config = parser instance with the parsed configuration

        Returns:
            status code to return to the OS

    ***************************************************************************/

    override protected int run ( Arguments args, ConfigParser config )
    {
        if ( this.memory_config.lock_memory )
        {
            if ( !this.lockMemory() )
            {
                return 1;
            }
        }

        auto storage = this.newStorageChannels();
        redistribution_process = new RedistributionProcess(
            storage, this.performance_config.redist_memory_limit_mulitplier);

        this.node = new DhtNode(this.server_config, this.node_item,
            storage, this.hash_range, this.epoll, this.per_request_stats,
            this.performance_config.no_delay);
        this.dht_stats =
            new ChannelsNodeStats(this.node, this.stats_ext.stats_log);

        this.node.error_callback = &this.nodeError;
        this.node.connection_limit = server_config.connection_limit;

        logger.info("Starting DHT node --------------------------------");

        this.startEventHandling(this.epoll);

        this.timer_ext.register(&this.onWriterFlush,
            cast(double)this.performance_config.write_flush_ms / 1000.0);

        this.node.register(this.epoll);

        logger.info("Starting event loop");
        this.epoll.eventLoop();
        logger.info("Event loop exited");

        return 0;
    }


    /***************************************************************************

        Periodic stats update callback.

    ***************************************************************************/

    override protected void onStatsTimer ( )
    {
        this.reportSystemStats();
        this.dht_stats.log();
        this.stats_ext.stats_log.add(.Log.stats());

        struct StompingPreventionStats
        {
            long stomping_prevention_counter;
        }

        StompingPreventionStats stomping_stats;

        version ( D_Version2 )
        {
            mixin(`
                import core.atomic : atomicLoad, atomicStore;

                stomping_stats.stomping_prevention_counter =
                    atomicLoad(.stomping_prevention_counter);
                atomicStore(.stomping_prevention_counter, 0UL);
            `);
        }

        this.stats_ext.stats_log.add(stomping_stats);
        this.stats_ext.stats_log.flush();
    }


    /***************************************************************************

        Periodic writer flush callback.

        Returns:
            true to re-register timer

    ***************************************************************************/

    private bool onWriterFlush ( )
    {
        try
        {
            this.node.flush();
        }
        catch ( Exception exception )
        {
            logger.error("Exception caught in writer flush timer handler: {} @ {}:{}",
                exception.message, exception.file, exception.line);
        }

        return true;
    }


    /***************************************************************************

        Creates a new instance of the storage channels. Calls the abstract
        newStorageChannels_() method, which does the actual construction of the
        storage channels instance.

        Returns:
            StorageChannels instance

    ***************************************************************************/

    private StorageChannels newStorageChannels ( )
    {
        this.storage_channels = new StorageChannels(this.server_config.data_dir,
            this.memory_config.size_limit, this.hash_range,
            this.memory_config.bnum, this.memory_config.out_of_range_handling,
            this.memory_config.disable_direct_io, this.memory_config.batch_size());

        return this.storage_channels;
    }


    /***************************************************************************

        Locks the process memory (including all future allocations) into RAM,
        preventing swapping to disk.

        Returns:
            true if the memory lock succeeded

    ***************************************************************************/

    private bool lockMemory ( )
    {
        if ( mlockall(MCL_CURRENT | MCL_FUTURE) )
        {
            const default_error = "Unknown";
            istring msg = default_error;

            // Provide custom error messages for expected errors
            switch ( errno )
            {
                case EPERM:
                    msg = "Executable does not have permissions to lock "
                        "memory";
                    break;
                case ENOMEM:
                    msg = "Attempted to lock more memory than allowed by "
                        "soft resource limit (RLIMIT_MEMLOCK)";
                    break;
                default:
                    // Leave default error message
            }

            auto error = strerror(errno);
            auto errno_desc = StringC.toDString(error);

            logger.fatal("Error when attempting to lock memory: {} "
                "(errno={}, '{}')", msg, errno, errno_desc);

            return false;
        }

        return true;
    }


    /***************************************************************************

        Returns:
            list of names of requests to be stats tracked

    ***************************************************************************/

    private istring[] per_request_stats ( )
    out ( rqs )
    {
        foreach ( rq; rqs )
        {
            assert(rq in DhtConst.Command(),
                "Cannot track stats for unknown request " ~ rq);
        }
    }
    body
    {
        return ["Put", "Get", "Exists", "Remove", "Listen", "GetAll", "GetAllKeys",
                "GetAllFilter", "PutBatch", "Redistribute"];
    }


    /***************************************************************************

        Returns:
            node item (address/port) for this node

    ***************************************************************************/

    private DhtConst.NodeItem node_item ( )
    {
        return DhtConst.NodeItem(this.server_config.address(), this.server_config.port());
    }


    /***************************************************************************

        Callback for exceptions inside the node's event loop. Writes errors to
        the log file.

        Params:
            exception = exception which occurred
            event_info = info about epoll event during which exception occurred
            conn = info about the connection handler where the exception
                occurred

    ***************************************************************************/

    private void nodeError ( Exception exception,
        IAdvancedSelectClient.Event event_info,
        DhtConnectionHandler.IConnectionHandlerInfo conn )
    {
        if ( cast(MessageFiber.KilledException)exception ||
             cast(IOWarning)exception ||
             cast(IOException)exception ||
             cast(EpollException)exception )
        {
            // Don't log these exception types, which only occur on the normal
            // disconnection of a client.
        }
        else
        {
            this.conn_error_buf.length = 0;
            enableStomping(this.conn_error_buf);
            conn.formatInfo(this.conn_error_buf);
            logger.error("Exception caught in eventLoop: '{}' @ {}:{} on {}",
                exception.message, exception.file, exception.line,
                this.conn_error_buf);
        }
    }


    /***************************************************************************

        SIGINT, TERM and QUIT handler. (Called from onSignal().)

        Firstly unregisters all periodics.

        Secondly stops the node's select listener (stopping any more requests
        from being processed) and cancels any active requests.

        Thirdly calls the node's shutdown() method, shutting down the storage
        channels.

        Finally shuts down epoll. This will result in the run() method, above,
        returning.

    ***************************************************************************/

    private void sigintHandler ( )
    {
        logger.info("SIGINT handler. Shutting down.");

        logger.trace("SIGINT handler: shutting down periodics");
        this.timer_ext.clear();
        logger.trace("SIGINT handler: shutting down periodics finished");

        logger.trace("SIGINT handler: stopping node listener");
        this.node.stopListener(this.epoll);
        logger.trace("SIGINT handler: stopping node listener finished");

        logger.trace("SIGINT handler: shutting down node");
        this.node.shutdown();
        logger.trace("SIGINT handler: shutting down node finished");

        logger.trace("SIGINT handler: shutting down epoll");
        this.epoll.shutdown();
        logger.trace("SIGINT handler: shutting down epoll finished");

        logger.trace("Finished SIGINT handler");

        this.node.state = DhtNode.State.ShutDown;
    }

    version ( D_Version2 ) { }
    else
    {
        /***********************************************************************

            Called (in D1 builds) when a GC collection starts.

        ***********************************************************************/

        private void gcCollectStart ( )
        {
            logger.trace("Starting GC collection");
        }

        /***********************************************************************

            Called (in D1 builds) when a GC collection finishes.

            Params:
                freed = the number of bytes freed overall
                pagebytes = the number of bytes freed within full pages

        ***********************************************************************/

        private void gcCollectEnd ( int freed, int pagebytes )
        {
            logger.trace("Finished GC collection: {} bytes freed, {} bytes freed within full pages",
                freed, pagebytes);
        }
    }
}

