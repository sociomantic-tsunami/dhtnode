/*******************************************************************************

    Distributed Hashtable Node Connection Handler

    copyright:
        Copyright (c) 2010-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.connection.DhtConnectionHandler;



/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import swarm.node.connection.ConnectionHandler;

import swarm.node.model.INodeInfo;

import dhtproto.client.legacy.DhtConst;

import dhtnode.connection.SharedResources;

import dhtnode.request.GetVersionRequest;
import dhtnode.request.GetResponsibleRangeRequest;
import dhtnode.request.GetChannelsRequest;
import dhtnode.request.GetSizeRequest;
import dhtnode.request.GetChannelSizeRequest;
import dhtnode.request.GetAllRequest;
import dhtnode.request.GetAllFilterRequest;
import dhtnode.request.RemoveChannelRequest;
import dhtnode.request.GetNumConnectionsRequest;
import dhtnode.request.ExistsRequest;
import dhtnode.request.GetRequest;
import dhtnode.request.PutRequest;
import dhtnode.request.RemoveRequest;
import dhtnode.request.ListenRequest;
import dhtnode.request.GetAllKeysRequest;
import dhtnode.request.RedistributeRequest;
import dhtnode.request.PutBatchRequest;

import dhtproto.node.request.model.DhtCommand;



/*******************************************************************************

    DHT node connection handler setup class.

    TODO: enable HMAC authentication by deriving from HmacAuthConnectionSetupParams

*******************************************************************************/

public class DhtConnectionSetupParams : ConnectionSetupParams
{
    import dhtnode.storage.StorageChannels;

    import ocean.io.compress.lzo.LzoChunkCompressor;


    /***************************************************************************

        Reference to the storage channels which the requests are operating on.

    ***************************************************************************/

    public StorageChannels storage_channels;


    /***************************************************************************

        Reference to the request resources pool shared between all connection
        handlers.

    ***************************************************************************/

    public SharedResources shared_resources;


    /***************************************************************************

        Lzo de/compressor.

    ***************************************************************************/

    public LzoChunkCompressor lzo;
}


/*******************************************************************************

    Dht node connection handler class.

    An object pool of these connection handlers is contained in the
    SelectListener which is instantiated inside DhtNode.

    TODO: enable HMAC authentication by deriving from HmacAuthConnectionHandler

*******************************************************************************/

public class DhtConnectionHandler
    : ConnectionHandlerTemplate!(DhtConst.Command)
{
    import dhtnode.request.model.RequestResources;


    /***************************************************************************

        Helper class to acquire and relinquish resources required by a request
        while it is handled. The resources are acquired from the shared
        resources instance which is passed to the constructor (in the
        DhtConnectionSetupParams instance). Acquired resources are automatically
        relinquished in the destructor.

        Note that it is assumed that each request will own at most one of each
        resource type (it is not possible, for example, to acquire two value
        buffers).

    ***************************************************************************/

    private scope class DhtRequestResources
        : RequestResources, IDhtRequestResources
    {
        import ocean.io.compress.lzo.LzoChunkCompressor;
        import ocean.io.select.fiber.SelectFiber;

        import dhtproto.node.request.params.RedistributeNode;

        import swarm.util.RecordBatcher;


        /**********************************************************************

            Forward methods of DhtCommand.Resources to own implementations

        **********************************************************************/

        override public mstring* getChannelBuffer ( )
        {
            return this.channel_buffer();
        }

        /// ditto
        override public mstring* getKeyBuffer ( )
        {
            return this.key_buffer();
        }

        /// ditto
        override public mstring* getFilterBuffer ( )
        {
            return this.filter_buffer();
        }

        /// ditto
        override public mstring* getValueBuffer ( )
        {
            return this.value_buffer();
        }

        /// ditto
        override public mstring* getDecompressBuffer ( )
        {
            return this.batch_buffer();
        }

        /// ditto
        override public ubyte[]* getCompressBuffer ( )
        {
            return cast(ubyte[]*) this.batch_buffer();
        }

        /// ditto
        override public RecordBatcher getRecordBatcher ( )
        {
            return this.batcher();
        }

        /// ditto
        override public RecordBatch getRecordBatch ( )
        {
            return this.record_batch();
        }

        /// ditto
        override public RedistributeNode[]* getRedistributeNodeBuffer ( )
        {
            return this.redistribute_node_buffer();
        }


        /***********************************************************************

            Constructor.

        ***********************************************************************/

        public this ( )
        {
            super(this.setup.shared_resources);
        }


        /***********************************************************************

            Internal connection setup params getter.

        ***********************************************************************/

        override public StorageChannels storage_channels ( )
        {
            return this.setup.storage_channels;
        }


        /***********************************************************************

            Node info getter.

        ***********************************************************************/

        override public IDhtNodeInfo node_info ( )
        {
            return cast(IDhtNodeInfo)this.setup.node_info;
        }


        /***********************************************************************

            Channel buffer newer.

        ***********************************************************************/

        override protected mstring new_channel_buffer ( )
        {
            return new char[32];
        }


        /***********************************************************************

            Key buffer newer.

        ***********************************************************************/

        override protected mstring new_key_buffer ( )
        {
            return new char[16]; // 16 hex digits in a 64-bit hash
        }


        /***********************************************************************

            Value buffer newer.

        ***********************************************************************/

        override protected mstring new_value_buffer ( )
        {
            return new char[512];
        }

        /***********************************************************************

            Channel list buffer newer

        ***********************************************************************/

        override protected cstring[] new_channel_list_buffer ( )
        {
            return new cstring[this.setup.storage_channels.length];
        }

        /***********************************************************************

            Filter buffer newer.

        ***********************************************************************/

        override protected mstring new_filter_buffer ( )
        {
            return new char[10];
        }


        /***********************************************************************

            Batch buffer newer.

        ***********************************************************************/

        override protected mstring new_batch_buffer ( )
        {
            return new char[RecordBatcher.DefaultMaxBatchSize];
        }


        /***********************************************************************

            Hash buffer newer.

        ***********************************************************************/

        override protected hash_t[] new_hash_buffer ( )
        {
            return new hash_t[10];
        }


        /***********************************************************************

            Step iterator newer.

        ***********************************************************************/

        override protected StorageEngineStepIterator new_iterator ( )
        {
            return this.setup.storage_channels.newIterator();
        }


        /***********************************************************************

            RedistributeNode buffer newer.

        ***********************************************************************/

        override protected RedistributeNode[] new_redistribute_node_buffer ( )
        {
            return new RedistributeNode[2];
        }


        /***********************************************************************

            Select event newer.

        ***********************************************************************/

        override protected FiberSelectEvent new_event ( )
        {
            return new FiberSelectEvent(this.outer.fiber);
        }


        /***********************************************************************

            Select timer newer.

        ***********************************************************************/

        override protected FiberTimerEvent new_timer ( )
        {
            return new FiberTimerEvent(this.outer.fiber);
        }


        /***********************************************************************

            Loop ceder newer.

        ***********************************************************************/

        override protected LoopCeder new_loop_ceder ( )
        {
            return new LoopCeder(this.event);
        }


        /***********************************************************************

            Record batcher newer.

        ***********************************************************************/

        override protected RecordBatcher new_batcher ( )
        {
            return new RecordBatcher(this.setup.lzo.lzo,
                this.setup.storage_channels.batch_size);
        }


        /***********************************************************************

            Record batch newer.

        ***********************************************************************/

        override protected RecordBatch new_record_batch ( )
        {
            return new RecordBatch(this.setup.lzo.lzo);
        }


        /***********************************************************************

            Node record batch newer.

        ***********************************************************************/

        override protected NodeRecordBatcherMap new_node_record_batch ( )
        {
            static immutable estimated_num_nodes = 5;
            return new NodeRecordBatcherMap(this.setup.lzo.lzo,
                estimated_num_nodes);
        }


        /***********************************************************************

            Dht client newer.

        ***********************************************************************/

        override protected DhtClient new_dht_client ( )
        {
            return new DhtClient(this.outer.fiber.epoll);
        }


        /***********************************************************************

            Select event initialiser.

        ***********************************************************************/

        override protected void init_event ( FiberSelectEvent event )
        {
            event.fiber = this.outer.fiber;
        }


        /***********************************************************************

            Select timer initialiser.

        ***********************************************************************/

        override protected void init_timer ( FiberTimerEvent timer )
        {
            timer.fiber = this.outer.fiber;
        }


        /***********************************************************************

            Loop ceder initialiser.

        ***********************************************************************/

        override protected void init_loop_ceder ( LoopCeder loop_ceder )
        {
            loop_ceder.event = this.event;
        }


        /***********************************************************************

            Dht client initialiser.

        ***********************************************************************/

        override protected void init_dht_client ( DhtClient dht_client )
        {
            dht_client.clearNodes();
        }


        /***********************************************************************

            Internal connection setup params getter.

        ***********************************************************************/

        private DhtConnectionSetupParams setup ( )
        {
            return cast(DhtConnectionSetupParams)this.outer.setup;
        }
    }


    /***************************************************************************

        Reuseable exception thrown when the command code read from the client
        is not supported (i.e. does not have a corresponding entry in
        this.requests).

    ***************************************************************************/

    private Exception invalid_command_exception;


    /***************************************************************************

        Constructor.

        Params:
            finalize_dg = user-specified finalizer, called when the connection
                is shut down
            setup = struct containing setup data for this connection

    ***************************************************************************/

    public this ( scope FinalizeDg finalize_dg, ConnectionSetupParams setup )
    {
        super(finalize_dg, setup);

        this.invalid_command_exception = new Exception("Invalid command",
            __FILE__, __LINE__);
    }


    /***************************************************************************

        Returns:
            the maximum buffer size that is considered sane for a DHT node

    ***************************************************************************/

    override protected ulong bufferSizeWarnLimit ( )
    {
        return (cast(DhtConnectionSetupParams)this.setup)
            .storage_channels.batch_size;
    }


    /***************************************************************************

        Command code 'None' handler. Treated the same as an invalid command
        code.

    ***************************************************************************/

    override protected void handleNone ( )
    {
        this.handleInvalidCommand();
    }


    /***************************************************************************

        Command code 'GetVersion' handler.

    ***************************************************************************/

    override protected void handleGetVersion ( )
    {
        this.handleCommand!(GetVersionRequest);
    }


    /***************************************************************************

        Command code 'GetResponsibleRange' handler.

    ***************************************************************************/

    override protected void handleGetResponsibleRange ( )
    {
        this.handleCommand!(GetResponsibleRangeRequest);
    }


    /***************************************************************************

        Command code 'GetNumConnections' handler.

    ***************************************************************************/

    override protected void handleGetNumConnections ( )
    {
        this.handleCommand!(GetNumConnectionsRequest);
    }


    /***************************************************************************

        Command code 'GetChannels' handler.

    ***************************************************************************/

    override protected void handleGetChannels ( )
    {
        this.handleCommand!(GetChannelsRequest);
    }


    /***************************************************************************

        Command code 'GetSize' handler.

    ***************************************************************************/

    override protected void handleGetSize ( )
    {
        this.handleCommand!(GetSizeRequest);
    }


    /***************************************************************************

        Command code 'GetChannelSize' handler.

    ***************************************************************************/

    override protected void handleGetChannelSize ( )
    {
        this.handleCommand!(GetChannelSizeRequest);
    }


    /***************************************************************************

        Command code 'Put' handler.

    ***************************************************************************/

    override protected void handlePut ( )
    {
        this.handleCommand!(PutRequest, RequestStatsTracking.TimeAndCount);
    }


    /***************************************************************************

        Command code 'PutBatch' handler.

    ***************************************************************************/

    override protected void handlePutBatch ( )
    {
        this.handleCommand!(PutBatchRequest, RequestStatsTracking.TimeAndCount);
    }


    /***************************************************************************

        Command code 'Get' handler.

    ***************************************************************************/

    override protected void handleGet ( )
    {
        this.handleCommand!(GetRequest, RequestStatsTracking.TimeAndCount);
    }


    /***************************************************************************

        Command code 'GetAll' handler.

    ***************************************************************************/

    override protected void handleGetAll ( )
    {
        this.handleCommand!(GetAllRequest, RequestStatsTracking.Count);
    }


    /***************************************************************************

        Command code 'GetAllFilter' handler.

    ***************************************************************************/

    override protected void handleGetAllFilter ( )
    {
        this.handleCommand!(GetAllFilterRequest, RequestStatsTracking.Count);
    }


    /***************************************************************************

        Command code 'GetAllKeys' handler.

    ***************************************************************************/

    override protected void handleGetAllKeys ( )
    {
        this.handleCommand!(GetAllKeysRequest, RequestStatsTracking.Count);
    }


    /***************************************************************************

        Command code 'Listen' handler.

    ***************************************************************************/

    override protected void handleListen ( )
    {
        this.handleCommand!(ListenRequest, RequestStatsTracking.Count);
    }


    /***************************************************************************

        Command code 'Exists' handler.

    ***************************************************************************/

    override protected void handleExists ( )
    {
        this.handleCommand!(ExistsRequest, RequestStatsTracking.TimeAndCount);
    }


    /***************************************************************************

        Command code 'Remove' handler.

    ***************************************************************************/

    override protected void handleRemove ( )
    {
        this.handleCommand!(RemoveRequest, RequestStatsTracking.TimeAndCount);
    }


    /***************************************************************************

        Command code 'RemoveChannel' handler.

    ***************************************************************************/

    override protected void handleRemoveChannel ( )
    {
        this.handleCommand!(RemoveChannelRequest);
    }


    /***************************************************************************

        Command code 'Redistribute' handler.

    ***************************************************************************/

    override protected void handleRedistribute ( )
    {
        this.handleCommand!(RedistributeRequest, RequestStatsTracking.Count);
    }


    /***************************************************************************

        Command handler method template.

        Template params:
            Handler = type of request handler
            stats_tracking = request stats tracking mode (see enum in
                swarm.node.connection.ConnectionHandler)

    ***************************************************************************/

    private void handleCommand ( Handler : DhtCommand,
        RequestStatsTracking stats_tracking = RequestStatsTracking.None ) ( )
    {
        scope resources = new DhtRequestResources;
        scope handler = new Handler(this.reader, this.writer, resources);

        // calls handler.handle() and checks memory and buffer allocation after
        // request finishes
        this.handleRequest!(ConnectionResources, DhtRequestResources,
            stats_tracking)(handler, resources, handler.command_name);
    }
}

